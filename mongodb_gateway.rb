#!/usr/bin/env ruby
#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative 'lib/mongodb_gateway/logger'
$LOAD_PATH.unshift File.expand_path('lib', __dir__)
require 'mongodb_gateway/gateway'

require 'optparse'
require 'logger'
require 'set'
require 'time'

Options = MongodbGateway::Gateway::Options

def parse_list(str)
  str.split(',').map { |s| s.strip }.reject(&:empty?)
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
    opts.banner = "Usage: mongodb_gateway [options]"
    opts.on("--listen HOST:PORT", String, "Local listen address") do |v|
      h,p = v.split(':',2); o.listen_host=h; o.listen_port=Integer(p)
    end
    opts.on("--upstream-uri URI", String, "MongoDB URI for upstream (required)") { |v| o.upstream_uri = v }
    opts.on("--redact-fields LIST", String, "Comma-separated field names to redact") do |v|
      parse_list(v).each { |k| o.redact_fields << k }
    end
    opts.on("--redact-commands LIST", String, "Comma-separated command names to suppress in logs") do |v|
      parse_list(v).each { |c| o.redact_cmds << c.downcase }
    end
    opts.on("--raw-requests", "Print structured OP_MSG for each request") { o.raw_requests = true }
    opts.on("--no-monitoring-logs", "Suppress logging for monitoring connections") { o.no_monitor_logs = true }
    opts.on("--no-responses", "Suppress logging of responses") { o.no_responses = true }
    opts.on("--sweep-interval SECONDS", Float, "Background sweep interval (default: #{o.sweep_interval}s)") { |v| o.sweep_interval = v }
    opts.on("-h","--help","Show help"){ puts opts; exit }
  end.parse!

  unless o.upstream_uri
    warn "Error: --upstream-uri is required"; exit 1
  end
  expand_redaction_keys!(o.redact_fields)
  o
end

def build_logger(redact_fields)
  std = Logger.new($stdout, level: Logger::DEBUG)
  std.formatter = proc { |sev,_t,_p,msg| "[#{Time.now.utc.iso8601}] #{sev.downcase}: #{msg}\n" }
  MongodbGateway::AppLogger.new(std, redact_fields: redact_fields)
end

opts = parse_opts
log  = build_logger(opts.redact_fields)
MongodbGateway::Gateway.run(opts, log)
