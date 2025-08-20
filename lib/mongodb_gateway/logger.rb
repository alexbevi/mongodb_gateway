# frozen_string_literal: true
require 'logger'
require 'json'

module MongodbGateway
  module Colorizer
    RESET  = "\e[0m"
    KCOL   = "\e[36m"  # keys
    SCOL   = "\e[32m"  # strings
    NCOL   = "\e[35m"  # numbers
    BCOL   = "\e[33m"  # booleans/null
    PCOL   = "\e[90m"  # punctuation

    module_function

    def json_string_literal(s) = JSON.generate(s)

    def prettify_json_value(v, buf)
      case v
      when Hash
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
  end

  class AppLogger
    def initialize(std_logger, redact_fields: nil)
      @logger = std_logger
      @redact_fields = redact_fields || Set.new
    end

    # Delegate standard logger methods
    %i[debug info warn error fatal unknown].each do |m|
      define_method(m) do |msg = nil, &block|
        @logger.send(m, msg, &block)
      end
    end

    # Format a BSON/Hash for logging with redaction and colorization
    def format_for_log(doc, redact_set = nil)
      return doc.inspect if doc.nil?
      redact_set = (@redact_fields || Set.new) if redact_set.nil?
      redacted = if redact_set && !redact_set.empty?
                   d = Redactor.deep_dup_bson(doc)
                   Redactor.redact_in!(d, redact_set)
                 else
                   doc
                 end
      json_obj = redacted.respond_to?(:as_json) ? redacted.as_json : redacted
      Colorizer.prettify_json(json_obj)
    rescue => e
      "[log-format-error: #{e.class}: #{e.message}] #{(redacted || doc).inspect rescue doc.inspect}"
    end

    def prettify_json(obj)
      Colorizer.prettify_json(obj)
    end
  end
end
