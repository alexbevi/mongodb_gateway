# frozen_string_literal: true
require 'socket'
require 'optparse'
require 'logger'
require 'time'
require 'set'
require 'json'
require 'thread'

require 'mongo'

require_relative 'codec'
require_relative 'redactor'
require_relative 'session_map'
require_relative 'server'

module MongodbGateway
  class Gateway
    Options = Struct.new(
      :listen_host, :listen_port, :upstream_uri,
      :redact_fields, :redact_cmds, :raw_requests, :no_monitor_logs,
      :no_responses,
      :sweep_interval,
      keyword_init: true
    )

    def self.run(opts, log)
      Mongo::Logger.logger.level = Logger::WARN
      upstream = Mongo::Client.new(opts.upstream_uri)
      Server.new(opts, log, upstream).start
    end
  end
end