# frozen_string_literal: true
require 'socket'
require 'logger'
require 'time'
require 'thread'

module MongodbGateway
  class Server
    def initialize(opts, log, upstream)
      @opts = opts
      @log = log
      @upstream = upstream
      @req_id = 0
    end

    def start
      session_map    = SessionMap.new(@upstream)

      server  = TCPServer.new(@opts.listen_host, @opts.listen_port)
      server.close_on_exec = true if server.respond_to?(:close_on_exec=)

      clients = {}
      mu = Mutex.new

      shutdown_requested = false
      force_shutdown = false

      sig_count = 0
      handler = proc do
        sig_count += 1
        shutdown_requested = true
        force_shutdown = true if sig_count >= 2
      end
      Signal.trap('INT', &handler)
      Signal.trap('TERM', &handler)

      sweeper_run = true
      sweeper = Thread.new do
        Thread.current.report_on_exception = false
        while sweeper_run
          sleep @opts.sweep_interval
          mu.synchronize do
            clients.dup.each do |sock, thr|
              alive_sock = begin
                !sock.closed? && sock.fileno
                true
              rescue
                false
              end
              clients.delete(sock) unless alive_sock && thr&.alive?
              begin sock.close rescue nil end unless alive_sock
              begin thr.kill  rescue nil end if thr && !thr.alive?
            end
          end
        end
      end

      @log.info "Listening on #{@opts.listen_host}:#{@opts.listen_port}; upstream=#{@opts.upstream_uri}; redactions=#{@opts.redact_fields.to_a.join(',')}; redact_cmds=#{@opts.redact_cmds.to_a.join(',')}; raw_requests=#{@opts.raw_requests}; no_monitor_logs=#{@opts.no_monitor_logs}; no_responses=#{@opts.no_responses}; sweep_interval=#{@opts.sweep_interval}s"

      begin
        until shutdown_requested
          readable, = IO.select([server], nil, nil, 0.25)
          next unless readable

          begin
            sock = server.accept_nonblock
          rescue IO::WaitReadable, Errno::EINTR
            next
          end

          th = Thread.new(sock) do |s|
            begin
              handle_connection(s, session_map)
            rescue => e
              @log.error "connection error: #{e.class}: #{e.message}"
            ensure
              mu.synchronize { clients.delete(s) }
              s.close rescue nil
            end
          end
          th.report_on_exception = false

          mu.synchronize { clients[sock] = th }
        end
      ensure
        sweeper_run = false
        sweeper.join rescue nil

        server.close rescue nil

        socks, thrs = mu.synchronize { [clients.keys, clients.values] }
        socks.each { |s| s.close rescue nil }

        @log.info "Waiting for #{thrs.count} worker(s) to finish…"
        deadline = Time.now + 1.5
        thrs.each do |t|
          remaining = deadline - Time.now
          next if remaining <= 0
          t.join(remaining) rescue nil
        end
        if force_shutdown
          thrs.each { |t| t.kill if t.alive? rescue nil }
        else
          thrs.each { |t| t.join(0.1) rescue nil }
        end

        @upstream.close rescue nil
        @log.info "Shutdown complete."
      end
    end

    # handle_connection is large; it uses Codec and Redactor modules
    def handle_connection(sock, session_map)
      codec = Codec
      redactor = Redactor

      is_monitor = false

      peer_info = sock.peeraddr(false) rescue nil
      peer_host = peer_info ? peer_info[2] : "unknown"
      peer_port = peer_info ? peer_info[1] : "?"

      first_frame = codec.read_frame(sock)
      return unless first_frame

      hdr = codec.parse_header(first_frame)
      unless hdr[:opcode] == codec::OP_MSG || hdr[:opcode] == codec::OP_QUERY
        @log.warn "Unsupported opcode from client: #{opcode_name(hdr[:opcode])} (#{hdr[:opcode]}); only OP_MSG or OP_QUERY accepted"
        reply = build_op_msg_reply(response_to: hdr[:req_id], doc: error_doc("Only OP_MSG or OP_QUERY supported in gateway PoC", 2, 'BadValue'))
        sock.write(reply) rescue nil
        return
      end

      payload = first_frame.byteslice(16, first_frame.bytesize - 16)

      first_doc = nil
      if hdr[:opcode] == codec::OP_MSG
        _flags, first_doc = codec.parse_op_msg_first_doc(payload)
      else
        parsed_q = codec.parse_op_query(payload)
        first_doc = parsed_q ? parsed_q[:query] : nil
      end

      if first_doc
        is_monitor = likely_monitoring_connection?(first_doc)
        meta = describe_client_from_handshake(first_doc)
        tag  = is_monitor ? " [monitoring]" : ""
        extra = meta && !meta.empty? ? " (#{meta})" : ""
        @log.info "Client connected: #{peer_host}:#{peer_port}#{tag}#{extra}" unless (@opts.no_monitor_logs && is_monitor)

        first_cmd_name = (first_doc.keys.first || '').to_s.downcase
        unless redacted_command?(first_cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor)
          if @opts.raw_requests
            struct = codec.op_msg_structure_hash(hdr, payload, @opts.redact_fields)
            line = @log.prettify_json(struct)
            @log.debug "RAW OP_MSG #{first_cmd_name}: #{line}"
          else
            @log.debug "REQ first #{first_cmd_name}: #{@log.format_for_log(first_doc, @opts.redact_fields)}"
          end
        end
      else
        @log.info "Client connected: #{peer_host}:#{peer_port}"
      end

      handle_req = lambda do |hdr_local, payload_local|
        if hdr_local[:opcode] == codec::OP_QUERY
          parsed = codec.parse_op_query(payload_local)
          unless parsed
            @log.warn "Malformed OP_QUERY from client (unable to parse)"
            sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: error_doc("Invalid OP_QUERY", 2, 'BadValue')))
            return
          end

          db_name  = parsed[:db]
          querydoc = parsed[:query]
          cmd_name = (querydoc.keys.first || '').to_s.downcase
          is_monitor_local = likely_monitoring_connection?(querydoc)

          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local)
            @log.debug "REQ OP_QUERY #{cmd_name}: #{@log.format_for_log(querydoc, @opts.redact_fields)}"
          end

          begin
            if %w[hello ismaster].include?(cmd_name)
              upstream_doc = @upstream.use('admin').database.command(BSON::Document.new({'hello' => 1})).first
              reply_doc = redactor.deep_dup_bson(upstream_doc)
              rewrite_hello_reply!(reply_doc, @opts.listen_host, @opts.listen_port)
              reply_doc = ensure_cursor_reply_shape!(cmd_name, querydoc, db_name, reply_doc)
              unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
                @log.debug "RES #{cmd_name} (rewritten): #{@log.format_for_log(reply_doc, @opts.redact_fields)}"
              end
              sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: reply_doc))
            else
              normalize_command_numeric_types!(querydoc)
              res = @upstream.use(db_name).database.command(querydoc)
              first = res.first || BSON::Document.new({'ok'=>1.0})
              first = ensure_cursor_reply_shape!(cmd_name, querydoc, db_name, first)
              unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
                @log.debug "RES #{cmd_name}: #{@log.format_for_log(first, @opts.redact_fields)}"
              end
              sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: first))
            end
          rescue Mongo::Error::OperationFailure => e
            @log.warn "Upstream OP_QUERY operation failure: code=#{e.code} message=#{e.message}"
            doc = BSON::Document.new({'ok'=>0.0,'errmsg'=>e.message,'code'=>e.code,'codeName'=>e.class.name.split('::').last})
            unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
              @log.debug "RES error #{cmd_name}: #{@log.format_for_log(doc, @opts.redact_fields)}"
            end
            sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
          rescue => e
            @log.error "Gateway error during OP_QUERY handling: #{e.class}: #{e.message}"
            doc = error_doc("Gateway error: #{e.class}: #{e.message}", 1, 'InternalError')
            unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
              @log.debug "RES error #{cmd_name}: #{@log.format_for_log(doc, @opts.redact_fields)}"
            end
            sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
          end

          next
        end

        _flags, cmd = codec.parse_op_msg_first_doc(payload_local)
        unless cmd
          @log.warn "Malformed OP_MSG from client (unable to parse first BSON document)"
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: error_doc("Invalid OP_MSG", 2, 'BadValue')))
          next
        end

        cmd_name = (cmd.keys.first || '').to_s.downcase
        db_name  = (cmd['$db'] || cmd[:$db] || 'admin').to_s
        is_monitor_local = likely_monitoring_connection?(cmd)

        unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local)
          if @opts.raw_requests
            struct = codec.op_msg_structure_hash(hdr_local, payload_local, @opts.redact_fields)
            line = @log.prettify_json(struct)
            @log.debug "RAW OP_MSG #{cmd_name}: #{line}"
          else
            @log.debug "REQ OP_MSG #{cmd_name}: #{@log.format_for_log(cmd, @opts.redact_fields)}"
          end
        end

        if %w[saslstart saslcontinue authenticate].include?(cmd_name)
          @log.warn "Client attempted authentication command (#{cmd_name}); gateway uses its own upstream credentials"
          doc = error_doc("Client authentication not supported by gateway; connect without credentials.", 18, 'AuthenticationFailed')
          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
            @log.debug "RES #{cmd_name} error: #{format_for_log(doc, @opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
          next
        end

        if %w[hello ismaster].include?(cmd_name)
          begin
            upstream_doc = @upstream.use('admin').database.command(BSON::Document.new({'hello' => 1})).first
          rescue => e
            upstream_doc = BSON::Document.new({'ok'=>1.0, 'isWritablePrimary'=>true, 'maxWireVersion'=>17, 'note'=>"hello synthesized due to upstream error: #{e.class}"})
          end
          reply_doc = redactor.deep_dup_bson(upstream_doc)
          rewrite_hello_reply!(reply_doc, @opts.listen_host, @opts.listen_port)
          reply_doc = ensure_cursor_reply_shape!(cmd_name, cmd, db_name, reply_doc)
          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
            @log.debug "RES #{cmd_name} (rewritten): #{@log.format_for_log(reply_doc, @opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: reply_doc))
          next
        end

        begin
          doc_for_upstream = cmd.dup
          doc_for_upstream.delete('$db')
          normalize_command_numeric_types!(doc_for_upstream)
          sess = session_map.fetch(cmd['lsid'] || cmd[:lsid])
          res = @upstream.use(db_name).database.command(doc_for_upstream, session: sess)
          first = res.first || BSON::Document.new({'ok'=>1.0})
          first = ensure_cursor_reply_shape!(cmd_name, cmd, db_name, first)
          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
            @log.debug "RES #{cmd_name}: #{@log.format_for_log(first, @opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: first))
        rescue Mongo::Error::OperationFailure => e
          @log.warn "Upstream operation failure: code=#{e.code} message=#{e.message}"
          doc = BSON::Document.new({'ok'=>0.0,'errmsg'=>e.message,'code'=>e.code,'codeName'=>e.class.name.split('::').last})
          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
            @log.debug "RES #{cmd_name} error: #{@log.format_for_log(doc, @opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
        rescue => e
          @log.error "Gateway error during command handling: #{e.class}: #{e.message}"
          doc = error_doc("Gateway error: #{e.class}: #{e.message}", 1, 'InternalError')
          unless redacted_command?(cmd_name, @opts) || (@opts.no_monitor_logs && is_monitor_local) || @opts.no_responses
            @log.debug "RES #{cmd_name} error: #{@log.format_for_log(doc, @opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
        end
      end

      # Handle first and subsequent frames
      handle_req.call(hdr, payload)

      while (frame = codec.read_frame(sock))
        hdr2 = codec.parse_header(frame)
        unless hdr2[:opcode] == codec::OP_MSG || hdr2[:opcode] == codec::OP_QUERY
          reply = build_op_msg_reply(response_to: hdr2[:req_id], doc: error_doc("Only OP_MSG or OP_QUERY supported in gateway PoC", 2, 'BadValue'))
          @log.warn "Unsupported opcode from client: #{opcode_name(hdr2[:opcode])} (#{hdr2[:opcode]}); only OP_MSG or OP_QUERY accepted"
          sock.write(reply)
          next
        end
        payload2 = frame.byteslice(16, frame.bytesize - 16)
        handle_req.call(hdr2, payload2)
      end
    rescue EOFError, IOError
      @log.info "Client disconnected: #{peer_host}:#{peer_port}" unless (@opts.no_monitor_logs && is_monitor)
    end

    def normalize_command_numeric_types!(doc)
      if doc.key?('txnNumber') || doc.key?(:txnNumber)
        k = doc.key?('txnNumber') ? 'txnNumber' : :txnNumber
        v = doc[k]; doc[k] = (v.is_a?(BSON::Int64) ? v : BSON::Int64.new(Integer(v))) if v
      end
      if doc.key?('getMore') || doc.key?(:getMore)
        k = doc.key?('getMore') ? 'getMore' : :getMore
        v = doc[k]; doc[k] = (v.is_a?(BSON::Int64) ? v : BSON::Int64.new(Integer(v))) if v
      end
      doc
    end

    def ensure_cursor_reply_shape!(cmd_name, cmd_doc, db_name, reply_doc)
      return reply_doc unless reply_doc.is_a?(BSON::Document)
      cursor_val = reply_doc['cursor'] || reply_doc[:cursor]
      return reply_doc unless cursor_val.is_a?(Hash) || cursor_val.is_a?(BSON::Document)

      cursor = cursor_val.is_a?(BSON::Document) ? cursor_val.dup : cursor_val.dup
      get = ->(h, k) { h[k] || h[k.to_sym] }

      begin
        id_val = get.call(cursor, 'id')
        id_i64 = id_val.nil? ? BSON::Int64.new(0) : (id_val.is_a?(BSON::Int64) ? id_val : BSON::Int64.new(Integer(id_val)))
      rescue
        id_i64 = BSON::Int64.new(0)
      end
      cursor['id'] = id_i64

      ns_val = get.call(cursor, 'ns')
      unless ns_val
        coll =
          get.call(cmd_doc, 'collection') ||
          get.call(cmd_doc, 'find') ||
          get.call(cmd_doc, 'aggregate') ||
          get.call(cmd_doc, 'listIndexes') ||
          (cmd_name == 'listcollections' ? '$cmd.listCollections' : '$cmd')
        ns_val = "#{db_name}.#{coll}"
      end
      cursor['ns'] = ns_val

      case cmd_name
      when 'find'     then cursor['firstBatch'] ||= []
      when 'getmore'  then cursor['nextBatch']  ||= []
      end

      normalized = BSON::Document.new
      cursor.each { |k, v| normalized[k.to_s] = v }
      reply_doc['cursor'] = normalized
      reply_doc
    end

    def describe_client_from_handshake(doc)
      return nil unless doc.is_a?(BSON::Document)
      c = doc['client']
      return nil unless c.is_a?(Hash) || c.is_a?(BSON::Document)
      parts = []
      if (drv = c['driver']).is_a?(Hash) || drv.is_a?(BSON::Document)
        parts << "#{drv['name']} #{drv['version']}'.strip"
      end
      if (app = c['application']).is_a?(Hash) || app.is_a?(BSON::Document)
        parts << "app=#{app['name']}" if app['name']
      end
      parts << "platform=#{c['platform']}" if c['platform']
      parts.reject(&:nil?).reject(&:empty?).join(', ')
    end

    def likely_monitoring_connection?(doc)
      return false unless doc.is_a?(BSON::Document)
      cmd = (doc.keys.first || '').to_s.downcase
      return false unless %w[hello ismaster].include?(cmd)
      return true if !doc.key?('lsid') && !doc.key?(:lsid)
      (doc.key?('topologyVersion') || doc.key?(:topologyVersion)) && !doc.key?(:saslStart) && !doc.key?('saslStart')
    end

    def rewrite_hello_reply!(doc, proxy_host, proxy_port)
      return doc unless doc.is_a?(BSON::Document)
      doc['ok'] = 1.0
      doc['helloOk'] = true
      proxy = "#{proxy_host}:#{proxy_port}"
      doc['hosts'] = [proxy]
      doc['me'] = proxy
      doc['logicalSessionTimeoutMinutes'] ||= 30
      doc['maxWireVersion'] ||= 17
      doc['minWireVersion'] ||= 0
      doc
    end

    def error_doc(msg, code = 1, code_name = 'InternalError')
      BSON::Document.new({'ok'=>0.0,'errmsg'=>msg,'code'=>code,'codeName'=>code_name})
    end

    def build_op_msg_reply(response_to:, doc:)
      doc_bin = Codec.bson_encode(doc)
      body = +""
      body << Codec.pack_i32_le(0)
      body << [0].pack('C')
      body << doc_bin
      len = 16 + body.bytesize
      header = Codec.pack_header(len, next_request_id, response_to, Codec::OP_MSG)
      header + body
    end

    def next_request_id
      @req_id ||= 0
      @req_id += 1
    end

    def redacted_command?(name, opts = @opts)
      opts.redact_cmds.include?(name.to_s.downcase)
    end
  end
end
