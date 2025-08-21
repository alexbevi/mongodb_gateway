# frozen_string_literal: true
module MongodbGateway
  module Codec
    OP_MSG   = 2013
    OP_QUERY = 2004

    MSG_FLAG_CHECKSUM_PRESENT = 1 << 0
    MSG_FLAG_MORE_TO_COME     = 1 << 1
    MSG_FLAG_EXHAUST_ALLOWED  = 1 << 16

    module_function

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

    require 'bson'

    def bson_decode(bytes)
      buf = BSON::ByteBuffer.new(bytes)
      BSON::Document.from_bson(buf)
    end

    def bson_encode(document)
      buf = BSON::ByteBuffer.new
      document.to_bson(buf)
      buf.to_s
    end

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

    def parse_op_query(payload)
      idx = 0
      return nil unless payload.bytesize >= 4

      # flags (int32)
      flags = payload[idx,4].unpack1('l<'); idx += 4

      # fullCollectionName (cstring)
      nul = payload.index("\x00", idx) or return nil
      full_coll = payload[idx...nul]; idx = nul + 1

      # numberToSkip and numberToReturn
      return nil unless payload.bytesize >= idx + 8
      number_to_skip   = payload[idx,4].unpack1('l<'); idx += 4
      number_to_return = payload[idx,4].unpack1('l<'); idx += 4

      # query BSON document
      return nil unless payload.bytesize >= idx + 4
      q_len = payload[idx,4].unpack1('l<')
      return nil unless payload.bytesize >= idx + q_len
      query_raw = payload[idx, q_len]; idx += q_len
      query_doc = bson_decode(query_raw)

      # optional returnFieldsSelector (BSON) may follow
      fields_doc = nil
      if payload.bytesize >= idx + 4
        f_len = payload[idx,4].unpack1('l<')
        if payload.bytesize >= idx + f_len
          fields_raw = payload[idx, f_len]
          begin
            fields_doc = bson_decode(fields_raw)
          rescue => _e
            # if fields selector is malformed, ignore it
            fields_doc = nil
          end
          idx += f_len
        end
      end

      db_name = full_coll.split('.', 2).first || 'admin'

      {
        db: db_name,
        full_collection: full_coll,
        flags: flags,
        number_to_skip: number_to_skip,
        number_to_return: number_to_return,
        query: query_doc,
        fields: fields_doc
      }
    rescue => e
      warn "[warn] failed to parse OP_QUERY: #{e.class}: #{e.message}"
      nil
    end

    def op_msg_structure_hash(hdr, payload, redact_set)
      # If this message is actually an OP_QUERY (legacy opcode), parse it as such
      if hdr[:opcode] == OP_QUERY
        parsed = parse_op_query(payload)
        return { "parseError" => "unable to parse OP_QUERY" } unless parsed

        qdoc = parsed[:query]
        fdoc = parsed[:fields]
        qdoc = (redact_set && !redact_set.empty?) ? redact_doc_for_log(qdoc, redact_set) : qdoc
        fdoc = (fdoc && redact_set && !redact_set.empty?) ? redact_doc_for_log(fdoc, redact_set) : fdoc

        return {
          "header" => {
            "messageLength" => hdr[:len],
            "requestID"     => hdr[:req_id],
            "responseTo"    => hdr[:res_to],
            "opCode"        => hdr[:opcode],
            "opCodeName"    => opcode_name(hdr[:opcode])
          },
          "flagBits" => parsed[:flags],
          "op_query" => {
            "fullCollectionName" => parsed[:full_collection],
            "numberToSkip"       => parsed[:number_to_skip],
            "numberToReturn"     => parsed[:number_to_return],
            "query"              => qdoc,
            "fields"             => fdoc
          }
        }
      end

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
            "body" => (redact_set && !redact_set.empty?) ? redact_doc_for_log(doc, redact_set) : doc
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
            docs << ((redact_set && !redact_set.empty?) ? redact_doc_for_log(doc, redact_set) : doc)
          end

          sections << {
            "kind" => 1,
            "identifier" => ident,
            "documents" => docs
          }
        else
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

    def opcode_name(code)
      {
        1=> 'OP_REPLY', 2001=>'OP_UPDATE', 2002=>'OP_INSERT', 2003=>'RESERVED',
        2004=>'OP_QUERY', 2005=>'OP_GET_MORE', 2006=>'OP_DELETE', 2007=>'OP_KILL_CURSORS',
        2010=>'OP_COMMAND', 2011=>'OP_COMMANDREPLY', 2012=>'OP_COMPRESSED', 2013=>'OP_MSG'
      }[code] || "OP_#{code}"
    end

    # helper used above when redact_set present
    def redact_doc_for_log(doc, redact_set)
      d = deep_dup_bson(doc)
      redact_in!(d, redact_set)
    end

    # Those methods rely on Redactor module; declared as expected to be available
    def deep_dup_bson(obj)
      Redactor.deep_dup_bson(obj)
    end

    def redact_in!(obj, redact_set)
      Redactor.redact_in!(obj, redact_set)
    end
  end
end
