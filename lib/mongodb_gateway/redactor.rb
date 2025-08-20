# frozen_string_literal: true
module MongodbGateway
  module Redactor
    module_function

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
  end
end
