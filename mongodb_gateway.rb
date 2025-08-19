#!/usr/bin/env ruby
# frozen_string_literal: true
#
require 'socket'
require 'optparse'
require 'logger'
require 'time'
require 'set'
require 'json'
require 'thread'

require 'bson'
require 'mongo'

Mongo::Logger.logger.level = Logger::WARN

OP_MSG   = 2013
OP_QUERY = 2004

def opcode_name(code)
  {
    1=> 'OP_REPLY', 2001=>'OP_UPDATE', 2002=>'OP_INSERT', 2003=>'RESERVED',
    2004=>'OP_QUERY', 2005=>'OP_GET_MORE', 2006=>'OP_DELETE', 2007=>'OP_KILL_CURSORS',
    2010=>'OP_COMMAND', 2011=>'OP_COMMANDREPLY', 2012=>'OP_COMPRESSED', 2013=>'OP_MSG'
  }[code] || "OP_#{code}"
end

def pack_i32_le(i) = [i].pack('l<')
def pack_header(len, req_id, res_to, opcode) = [len, req_id, res_to, opcode].pack('l<l<l<l<')

def read_exact(io, n)
  data = +""
  while data.bytesize < n
    chunk = io.read(n - data.bytesize)
    return nil unless chunk
    data << chunk
  end
  data
end

def read_frame(io)
  hdr = read_exact(io, 4)
  return nil unless hdr
  total_len = hdr.unpack1('l<')
  rest = read_exact(io, total_len - 4) or return nil
  hdr + rest
end

def parse_header(frame)
  len, req, res, op = frame[0,16].unpack('l<l<l<l<')
  {len:, req_id: req, res_to: res, opcode: op}
end

# ===== BSON helpers ===========================================================

def bson_decode(bytes)
  buf = BSON::ByteBuffer.new(bytes)
  BSON::Document.from_bson(buf)
end

def bson_encode(document)
  buf = BSON::ByteBuffer.new
  document.to_bson(buf)
  buf.to_s
end

# ===== Redaction & formatting =================================================

def deep_dup_bson(obj)
  case obj
  when BSON::Document
    dup = BSON::Document.new
    obj.each_pair { |k, v| dup[k] = deep_dup_bson(v) }
    dup
  when Hash
    obj.each_with_object({}) { |(k, v), h| h[k] = deep_dup_bson(v) }
  when Array
    obj.map { |v| deep_dup_bson(v) }
  else
    obj
  end
end

def redact_in!(obj, redact_set)
  return obj if redact_set.nil? || redact_set.empty?
  case obj
  when BSON::Document, Hash
    obj.keys.each do |k|
      ks = k.to_s
      if redact_set.include?(ks)
        obj[k] = "[REDACTED]"
      else
        redact_in!(obj[k], redact_set)
      end
    end
  when Array
    obj.each { |v| redact_in!(v, redact_set) }
  end
  obj
end

def expand_redaction_keys!(set)
  extras = []
  set.each do |k|
    if k.start_with?('$')
      extras << k.sub(/^\$/,'')
    else
      extras << "$#{k}"
    end
  end
  set.merge(extras)
end

# --- Colorized, single-line JSON ---------------------------------------------

RESET  = "\e[0m"
KCOL   = "\e[36m"  # keys
SCOL   = "\e[32m"  # strings
NCOL   = "\e[35m"  # numbers
BCOL   = "\e[33m"  # booleans/null
PCOL   = "\e[90m"  # punctuation

def json_string_literal(s) = JSON.generate(s)

def prettify_json_value(v, buf)
  case v
  when Hash, BSON::Document
    buf << "#{PCOL}{#{RESET}"
    first = true
    v.each_pair do |k, val|
      buf << "#{PCOL}, #{RESET}" unless first
      first = false
      buf << "#{KCOL}#{json_string_literal(k.to_s)}#{RESET}"
      buf << "#{PCOL}: #{RESET}"
      prettify_json_value(val, buf)
    end
    buf << "#{PCOL}}#{RESET}"
  when Array
    buf << "#{PCOL}[#{RESET}"
    v.each_with_index do |el, i|
      buf << "#{PCOL}, #{RESET}" if i > 0
      prettify_json_value(el, buf)
    end
    buf << "#{PCOL}]#{RESET}"
  when String
    buf << "#{SCOL}#{json_string_literal(v)}#{RESET}"
  when Numeric
    buf << "#{NCOL}#{v}#{RESET}"
  when TrueClass, FalseClass
    buf << "#{BCOL}#{v ? 'true' : 'false'}#{RESET}"
  when NilClass
    buf << "#{BCOL}null#{RESET}"
  else
    buf << "#{SCOL}#{JSON.generate(v)}#{RESET}"
  end
end

def prettify_json(obj)
  buf = +""
  prettify_json_value(obj, buf)
  buf
end

def format_for_log(doc, redact_set, as_json: true)
  return doc.inspect if doc.nil?
  redacted = if redact_set && !redact_set.empty?
               d = deep_dup_bson(doc)
               redact_in!(d, redact_set)
             else
               doc
             end
  json_obj = redacted.respond_to?(:as_json) ? redacted.as_json : redacted
  prettify_json(json_obj)
rescue => e
  "[log-format-error: #{e.class}: #{e.message}] #{(redacted || doc).inspect rescue doc.inspect}"
end

# ===== OP_MSG parsing / building =============================================

def parse_op_msg_first_doc(payload)
  return [nil, nil] if payload.bytesize < 5
  flags = payload[0,4].unpack1('l<')
  kind  = payload.getbyte(4)
  return [flags, nil] unless kind == 0
  return [flags, nil] if payload.bytesize < 9
  doc_len = payload[5,4].unpack1('l<')
  return [flags, nil] if payload.bytesize < 5 + doc_len
  raw = payload[5, doc_len]
  [flags, bson_decode(raw)]
rescue => e
  warn "[warn] failed to parse OP_MSG doc: #{e.class}: #{e.message}"
  [nil, nil]
end

def build_op_msg_reply(response_to:, doc:)
  doc_bin = bson_encode(doc)
  body = +""
  body << pack_i32_le(0)  # flags
  body << [0].pack('C')   # section kind 0
  body << doc_bin         # single BSON doc
  len = 16 + body.bytesize
  header = pack_header(len, next_request_id, response_to, OP_MSG)
  header + body
end

def next_request_id
  @req_id ||= 0
  @req_id += 1
end

# ----- OP_MSG raw structure (for --raw-requests) -------------------------------

# Decoded flags
MSG_FLAG_CHECKSUM_PRESENT = 1 << 0
MSG_FLAG_MORE_TO_COME     = 1 << 1
MSG_FLAG_EXHAUST_ALLOWED  = 1 << 16

def redact_doc_for_log(doc, redact_set)
  return doc unless redact_set && !redact_set.empty?
  d = deep_dup_bson(doc)
  redact_in!(d, redact_set)
end

def op_msg_structure_hash(hdr, payload, redact_set)
  # header is from parse_header; payload starts at flags
  flags_u32 = payload[0,4].unpack1('L<')
  checksum_present = (flags_u32 & MSG_FLAG_CHECKSUM_PRESENT) != 0
  more_to_come     = (flags_u32 & MSG_FLAG_MORE_TO_COME) != 0
  exhaust_allowed  = (flags_u32 & MSG_FLAG_EXHAUST_ALLOWED) != 0

  end_idx = payload.bytesize - (checksum_present ? 4 : 0)
  pos = 4
  sections = []

  while pos < end_idx
    kind = payload.getbyte(pos)
    pos += 1
    case kind
    when 0
      raise "Bad OP_MSG body: missing doc length" if end_idx - pos < 4
      dlen = payload[pos,4].unpack1('l<')
      raise "Bad OP_MSG body: truncated doc" if pos + dlen > end_idx
      raw = payload[pos, dlen]; pos += dlen
      doc = bson_decode(raw)
      sections << {
        "kind" => 0,
        "body" => redact_doc_for_log(doc, redact_set)
      }
    when 1
      raise "Bad OP_MSG seq: missing section size" if end_idx - pos < 4
      sec_size = payload[pos,4].unpack1('l<'); pos += 4
      sec_end = pos - 4 + sec_size
      raise "Bad OP_MSG seq: section overruns message" if sec_end > end_idx

      nul = payload.index("\x00", pos)
      raise "Bad OP_MSG seq: missing identifier" unless nul && nul < sec_end
      ident = payload[pos...nul]
      pos = nul + 1

      docs = []
      while pos < sec_end
        raise "Bad OP_MSG seq: truncated document" if sec_end - pos < 4
        dlen = payload[pos,4].unpack1('l<')
        raise "Bad OP_MSG seq: doc overruns section" if pos + dlen > sec_end
        raw = payload[pos, dlen]; pos += dlen
        doc = bson_decode(raw)
        docs << redact_doc_for_log(doc, redact_set)
      end

      sections << {
        "kind" => 1,
        "identifier" => ident,
        "documents" => docs
      }
    else
      # Unknown kind; capture remaining bytes for debugging
      rest = payload[pos, end_idx - pos]
      sections << {
        "kind" => kind,
        "unknownPayloadHex" => rest.unpack1('H*')
      }
      break
    end
  end

  checksum = checksum_present ? payload[end_idx, 4].unpack1('L<') : nil

  {
    "header" => {
      "messageLength" => hdr[:len],
      "requestID"     => hdr[:req_id],
      "responseTo"    => hdr[:res_to],
      "opCode"        => hdr[:opcode],
      "opCodeName"    => opcode_name(hdr[:opcode])
    },
    "flagBits" => flags_u32,
    "flags" => {
      "checksumPresent" => checksum_present,
      "moreToCome"      => more_to_come,
      "exhaustAllowed"  => exhaust_allowed
    },
    "sections" => sections
  }.tap { |h| h["checksum"] = ("0x%08x" % checksum) if checksum }
rescue => e
  { "parseError" => "#{e.class}: #{e.message}" }
end

# ===== OP_QUERY parsing (legacy handshake) ===================================

def parse_op_query(payload)
  idx = 0
  return nil unless payload.bytesize >= 4
  _flags = payload[idx,4].unpack1('l<'); idx += 4
  nul = payload.index("\x00", idx) or return nil
  full_coll = payload[idx...nul]; idx = nul + 1
  return nil unless payload.bytesize >= idx + 8
  _skip  = payload[idx,4].unpack1('l<'); idx += 4
  _limit = payload[idx,4].unpack1('l<'); idx += 4
  return nil unless payload.bytesize >= idx + 4
  q_len = payload[idx,4].unpack1('l<')
  return nil unless payload.bytesize >= idx + q_len
  query_raw = payload[idx, q_len]; idx += q_len
  db_name = full_coll.split('.', 2).first || 'admin'
  query_doc = bson_decode(query_raw)
  { db: db_name, query: query_doc }
rescue => e
  warn "[warn] failed to parse OP_QUERY: #{e.class}: #{e.message}"
  nil
end

# ===== Numeric normalization ==================================================

def to_int64(v)
  return v if v.is_a?(BSON::Int64)
  BSON::Int64.new(Integer(v))
end

def normalize_command_numeric_types!(doc)
  return doc unless doc.is_a?(BSON::Document) || doc.is_a?(Hash)
  if doc.key?('txnNumber') || doc.key?(:txnNumber)
    k = doc.key?('txnNumber') ? 'txnNumber' : :txnNumber
    v = doc[k]; doc[k] = to_int64(v) if v
  end
  if doc.key?('getMore') || doc.key?(:getMore)
    k = doc.key?('getMore') ? 'getMore' : :getMore
    v = doc[k]; doc[k] = to_int64(v) if v
  end
  doc
end

# ===== Ensure cursor reply shape =============================================

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

# ===== Session & Monitoring Cache ============================================

class SessionMap
  def initialize(client) = (@client = client; @map = {}; @mutex = Mutex.new)
  def fetch(lsid)
    return nil unless lsid.is_a?(Hash) || lsid.is_a?(BSON::Document)
    key = lsid['id'] || lsid[:id]
    return nil unless key
    hex = key.respond_to?(:data) ? key.data.unpack1('H*') : key.to_s
    @mutex.synchronize { @map[hex] ||= @client.start_session }
  end
end



# ===== Fingerprinting / hello / errors =======================================

def describe_client_from_handshake(doc)
  return nil unless doc.is_a?(BSON::Document)
  c = doc['client']
  return nil unless c.is_a?(Hash) || c.is_a?(BSON::Document)
  parts = []
  if (drv = c['driver']).is_a?(Hash) || drv.is_a?(BSON::Document)
    parts << "#{drv['name']} #{drv['version']}".strip
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

def upstream_minimal_hello(upstream)
  upstream.use('admin').database.command(BSON::Document.new({'hello'=>1})).first
end

# ===== Options / logger / main ===============================================

Options = Struct.new(
  :listen_host, :listen_port, :upstream_uri,
  :redact_fields, :redact_cmds, :raw_requests, :no_monitor_logs,
  :no_responses,
  :sweep_interval,
  keyword_init: true
)

def parse_list(str)
  str.split(',').map { |s| s.strip }.reject(&:empty?)
end

def parse_opts
  o = Options.new(
    listen_host: '127.0.0.1',
    listen_port: 27018,
    upstream_uri: nil,
    redact_fields: Set.new,
    redact_cmds: Set.new,
    raw_requests: false,
    no_monitor_logs: false,
    no_responses: false,
    sweep_interval: 5.0
  )
  OptionParser.new do |opts|
    opts.banner = <<~B
      Usage: ruby mongodb_gateway.rb [options]

    B
    opts.on("--listen HOST:PORT", "Local listen address (default: #{o.listen_host}:#{o.listen_port})") do |v|
      h,p = v.split(':',2); o.listen_host=h; o.listen_port=Integer(p)
    end
    opts.on("--upstream-uri URI", "MongoDB URI for upstream (required)") { |v| o.upstream_uri = v }
    opts.on("--redact-fields LIST", "Comma-separated field names to redact (e.g. lsid,$clusterTime)") do |v|
      parse_list(v).each { |k| o.redact_fields << k }
    end
    opts.on("--redact-commands LIST", "Comma-separated command names to suppress in REQ/RES logs (e.g. hello,ismaster,ping)") do |v|
      parse_list(v).each { |c| o.redact_cmds << c.downcase }
    end
    opts.on("--raw-requests", "Print structured OP_MSG (header/flags/sections/checksum) for each request; suppress standard REQ log") { o.raw_requests = true }
    opts.on("--no-monitoring-logs", "Suppress logging for monitoring connections") { o.no_monitor_logs = true }
    opts.on("--no-responses", "Suppress logging of responses (RES logs)") { o.no_responses = true }
    opts.on("--sweep-interval SECONDS", Float, "Background sweep interval (default: #{o.sweep_interval}s)") { |v| o.sweep_interval = v }
    opts.on("-h","--help","Show help"){ puts opts; exit }
  end.parse!
  unless o.upstream_uri
    warn "Error: --upstream-uri is required"; exit 1
  end
  expand_redaction_keys!(o.redact_fields)
  o
end

def build_logger
  Logger.new($stdout, level: Logger::DEBUG).tap do |log|
    log.formatter = proc { |sev,_t,_p,msg| "[#{Time.now.utc.iso8601}] #{sev.downcase}: #{msg}\n" }
  end
end

def start_gateway!(opts, log)
  upstream       = Mongo::Client.new(opts.upstream_uri, server_api: { version: '1' })
  session_map    = SessionMap.new(upstream)


  server  = TCPServer.new(opts.listen_host, opts.listen_port)
  server.close_on_exec = true if server.respond_to?(:close_on_exec=)

  # Track sockets -> threads so the sweeper can correlate and cleanly prune.
  clients = {}   # { TCPSocket => Thread }
  mu = Mutex.new

  shutdown_requested = false
  force_shutdown     = false

  # Traps: flip flags only. Second Ctrl+C forces hard shutdown.
  sig_count = 0
  handler = proc do
    sig_count += 1
    shutdown_requested = true
    force_shutdown = true if sig_count >= 2
  end
  Signal.trap('INT',  &handler)
  Signal.trap('TERM', &handler)

  # Background sweeper: prune closed sockets and dead worker threads.
  sweeper_run = true
  sweeper = Thread.new do
    Thread.current.report_on_exception = false
    while sweeper_run
      sleep opts.sweep_interval
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

  log.info "Listening on #{opts.listen_host}:#{opts.listen_port}; upstream=#{opts.upstream_uri}; redactions=#{opts.redact_fields.to_a.join(',')}; redact_cmds=#{opts.redact_cmds.to_a.join(',')}; raw_requests=#{opts.raw_requests}; no_monitor_logs=#{opts.no_monitor_logs}; no_responses=#{opts.no_responses}; sweep_interval=#{opts.sweep_interval}s"

  begin
    # Non-blocking accept loop driven by IO.select so shutdown is responsive without exceptions.
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
          handle_connection(s, upstream, session_map, opts, log)
        rescue => e
          log.error "connection error: #{e.class}: #{e.message}"
        ensure
          mu.synchronize { clients.delete(s) }
          s.close rescue nil
        end
      end
      th.report_on_exception = false

      mu.synchronize { clients[sock] = th }
    end
  ensure
    # Stop sweeper first to avoid racing with shutdown cleanup
    sweeper_run = false
    sweeper.join rescue nil

    # Close listener & clients
    server.close rescue nil

    socks, thrs = mu.synchronize { [clients.keys, clients.values] }
    socks.each { |s| s.close rescue nil }

    # Give workers a brief grace period, then hard-kill any stragglers if forced
    log.info "Waiting for #{thrs.count} worker(s) to finish…"
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

    upstream.close rescue nil
    log.info "Shutdown complete."
  end
end

def redacted_command?(name, opts)
  opts.redact_cmds.include?(name.to_s.downcase)
end

def handle_connection(sock, upstream, session_map, opts, log)
  is_monitor = false

  peer_info = sock.peeraddr(false) rescue nil
  peer_host = peer_info ? peer_info[2] : "unknown"
  peer_port = peer_info ? peer_info[1] : "?"

  first_frame = read_frame(sock)
  return unless first_frame

  hdr = parse_header(first_frame)
  unless hdr[:opcode] == OP_MSG || hdr[:opcode] == OP_QUERY
    log.warn "Unsupported opcode from client: #{opcode_name(hdr[:opcode])} (#{hdr[:opcode]}); only OP_MSG or OP_QUERY accepted"
    reply = build_op_msg_reply(response_to: hdr[:req_id], doc: error_doc("Only OP_MSG or OP_QUERY supported in gateway PoC", 2, 'BadValue'))
    sock.write(reply) rescue nil
    return
  end

  payload = first_frame.byteslice(16, first_frame.bytesize - 16)

  first_doc = nil
  if hdr[:opcode] == OP_MSG
    _flags, first_doc = parse_op_msg_first_doc(payload)
  else
    parsed_q = parse_op_query(payload)
    first_doc = parsed_q ? parsed_q[:query] : nil
  end

  if first_doc
    is_monitor = likely_monitoring_connection?(first_doc)
    meta = describe_client_from_handshake(first_doc)
    tag  = is_monitor ? " [monitoring]" : ""
    extra = meta && !meta.empty? ? " (#{meta})" : ""
    log.info "Client connected: #{peer_host}:#{peer_port}#{tag}#{extra}" unless (opts.no_monitor_logs && is_monitor)

    first_cmd_name = (first_doc.keys.first || '').to_s.downcase
    unless redacted_command?(first_cmd_name, opts) || (opts.no_monitor_logs && is_monitor)
      if opts.raw_requests
        struct = op_msg_structure_hash(hdr, payload, opts.redact_fields)
        line = prettify_json(struct)
        log.debug "RAW OP_MSG #{first_cmd_name}: #{line}"
      else
        log.debug "REQ first #{first_cmd_name}: #{format_for_log(first_doc, opts.redact_fields)}"
      end
    end
  else
    log.info "Client connected: #{peer_host}:#{peer_port}"
  end

  handle_req = lambda do |hdr_local, payload_local|
    if hdr_local[:opcode] == OP_QUERY
      parsed = parse_op_query(payload_local)
      unless parsed
        log.warn "Malformed OP_QUERY from client (unable to parse)"
        sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: error_doc("Invalid OP_QUERY", 2, 'BadValue')))
        return
      end

      db_name  = parsed[:db]
      querydoc = parsed[:query]
      cmd_name = (querydoc.keys.first || '').to_s.downcase
      is_monitor_local = likely_monitoring_connection?(querydoc)

      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local)
        log.debug "REQ OP_QUERY #{cmd_name}: #{format_for_log(querydoc, opts.redact_fields)}"
      end

      begin
        if %w[hello ismaster].include?(cmd_name)
          upstream_doc = upstream.use('admin').database.command(BSON::Document.new({'hello' => 1})).first
          reply_doc = deep_dup_bson(upstream_doc)
          rewrite_hello_reply!(reply_doc, opts.listen_host, opts.listen_port)
          reply_doc = ensure_cursor_reply_shape!(cmd_name, querydoc, db_name, reply_doc)
          unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
            log.debug "RES #{cmd_name} (rewritten): #{format_for_log(reply_doc, opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: reply_doc))
        else
          normalize_command_numeric_types!(querydoc)
          res = upstream.use(db_name).database.command(querydoc)
          first = res.first || BSON::Document.new({'ok'=>1.0})
          first = ensure_cursor_reply_shape!(cmd_name, querydoc, db_name, first)
          unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
            log.debug "RES #{cmd_name}: #{format_for_log(first, opts.redact_fields)}"
          end
          sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: first))
        end
      rescue Mongo::Error::OperationFailure => e
        log.warn "Upstream OP_QUERY operation failure: code=#{e.code} message=#{e.message}"
        doc = BSON::Document.new({'ok'=>0.0,'errmsg'=>e.message,'code'=>e.code,'codeName'=>e.class.name.split('::').last})
        unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
          log.debug "RES error #{cmd_name}: #{format_for_log(doc, opts.redact_fields)}"
        end
        sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
      rescue => e
        log.error "Gateway error during OP_QUERY handling: #{e.class}: #{e.message}"
        doc = error_doc("Gateway error: #{e.class}: #{e.message}", 1, 'InternalError')
        unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
          log.debug "RES error #{cmd_name}: #{format_for_log(doc, opts.redact_fields)}"
        end
        sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
      end

      next
    end

    _flags, cmd = parse_op_msg_first_doc(payload_local)
    unless cmd
      log.warn "Malformed OP_MSG from client (unable to parse first BSON document)"
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: error_doc("Invalid OP_MSG", 2, 'BadValue')))
      next
    end

    cmd_name = (cmd.keys.first || '').to_s.downcase
    db_name  = (cmd['$db'] || cmd[:$db] || 'admin').to_s
    is_monitor_local = likely_monitoring_connection?(cmd)

    unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local)
      if opts.raw_requests
        struct = op_msg_structure_hash(hdr_local, payload_local, opts.redact_fields)
        line = prettify_json(struct)
        log.debug "RAW OP_MSG #{cmd_name}: #{line}"
      else
        log.debug "REQ OP_MSG #{cmd_name}: #{format_for_log(cmd, opts.redact_fields)}"
      end
    end

    if %w[saslstart saslcontinue authenticate].include?(cmd_name)
      log.warn "Client attempted authentication command (#{cmd_name}); gateway uses its own upstream credentials"
      doc = error_doc("Client authentication not supported by gateway; connect without credentials.", 18, 'AuthenticationFailed')
      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
        log.debug "RES #{cmd_name} error: #{format_for_log(doc, opts.redact_fields)}"
      end
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
      next
    end

    if %w[hello ismaster].include?(cmd_name)
      begin
        upstream_doc = upstream.use('admin').database.command(BSON::Document.new({'hello' => 1})).first
      rescue => e
        upstream_doc = BSON::Document.new({'ok'=>1.0, 'isWritablePrimary'=>true, 'maxWireVersion'=>17, 'note'=>"hello synthesized due to upstream error: #{e.class}"})
      end
      reply_doc = deep_dup_bson(upstream_doc)
      rewrite_hello_reply!(reply_doc, opts.listen_host, opts.listen_port)
      reply_doc = ensure_cursor_reply_shape!(cmd_name, cmd, db_name, reply_doc)
      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
        log.debug "RES #{cmd_name} (rewritten): #{format_for_log(reply_doc, opts.redact_fields)}"
      end
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: reply_doc))
      next
    end

    begin
      doc_for_upstream = cmd.dup
      doc_for_upstream.delete('$db')
      normalize_command_numeric_types!(doc_for_upstream)
      sess = session_map.fetch(cmd['lsid'] || cmd[:lsid])
      res = upstream.use(db_name).database.command(doc_for_upstream, session: sess)
      first = res.first || BSON::Document.new({'ok'=>1.0})
      first = ensure_cursor_reply_shape!(cmd_name, cmd, db_name, first)
      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
        log.debug "RES #{cmd_name}: #{format_for_log(first, opts.redact_fields)}"
      end
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: first))
    rescue Mongo::Error::OperationFailure => e
      log.warn "Upstream operation failure: code=#{e.code} message=#{e.message}"
      doc = BSON::Document.new({'ok'=>0.0,'errmsg'=>e.message,'code'=>e.code,'codeName'=>e.class.name.split('::').last})
      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
        log.debug "RES #{cmd_name} error: #{format_for_log(doc, opts.redact_fields)}"
      end
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
    rescue => e
      log.error "Gateway error during command handling: #{e.class}: #{e.message}"
      doc = error_doc("Gateway error: #{e.class}: #{e.message}", 1, 'InternalError')
      unless redacted_command?(cmd_name, opts) || (opts.no_monitor_logs && is_monitor_local) || opts.no_responses
        log.debug "RES #{cmd_name} error: #{format_for_log(doc, opts.redact_fields)}"
      end
      sock.write(build_op_msg_reply(response_to: hdr_local[:req_id], doc: doc))
    end
  end

  # Handle first and subsequent frames
  handle_req.call(hdr, payload)

  while (frame = read_frame(sock))
    hdr2 = parse_header(frame)
    unless hdr2[:opcode] == OP_MSG || hdr2[:opcode] == OP_QUERY
      reply = build_op_msg_reply(response_to: hdr2[:req_id], doc: error_doc("Only OP_MSG or OP_QUERY supported in gateway PoC", 2, 'BadValue'))
      log.warn "Unsupported opcode from client: #{opcode_name(hdr2[:opcode])} (#{hdr2[:opcode]}); only OP_MSG or OP_QUERY accepted"
      sock.write(reply)
      next
    end
    payload2 = frame.byteslice(16, frame.bytesize - 16)
    handle_req.call(hdr2, payload2)
  end
rescue EOFError, IOError
  log.info "Client disconnected: #{peer_host}:#{peer_port}" unless (opts.no_monitor_logs && is_monitor)
end

# ===== Main ==================================================================

opts = parse_opts
log  = build_logger
start_gateway!(opts, log)
