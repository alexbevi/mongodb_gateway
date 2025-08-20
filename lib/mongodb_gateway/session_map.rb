# frozen_string_literal: true
module MongodbGateway
  class SessionMap
    def initialize(client)
      @client = client
      @map = {}
      @mutex = Mutex.new
    end

    def fetch(lsid)
      return nil unless lsid.is_a?(Hash) || lsid.is_a?(BSON::Document)
      key = lsid['id'] || lsid[:id]
      return nil unless key
      hex = key.respond_to?(:data) ? key.data.unpack1('H*') : key.to_s
      @mutex.synchronize { @map[hex] ||= @client.start_session }
    end
  end
end
