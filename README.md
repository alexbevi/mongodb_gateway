# mongodb_gateway

An application-layer MongoDB proxy/gateway written in Ruby.

Clients connect to the gateway using a plain [MongoDB connection string](https://www.mongodb.com/docs/manual/reference/connection-string/) (ex: `mongodb://host:port` - no TLS, no auth).
The gateway connects to your upstream cluster using the official Ruby driver and a full
connection string and forwards commands while letting you inspect and log what flows through.

> [!WARNING]
> This project is intended for experimentation, debugging, and education.
> It is not a drop-in security boundary or production proxy. Use responsibly.

## Why

* Inspect [opcodes](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#opcodes), commands and responses between a MongoDB client and cluster.
* Redact sensitive fields from logs, or shorten log lines by filtering fields out (eg. `lsid`, `$clusterTime`).
* Suppress noisy [`hello`/`ismaster`](https://www.mongodb.com/docs/manual/reference/command/hello/) chatter through command filtration.
* Dump a structured [`OP_MSG`](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#op_msg) view (header, flags, sections, checksum) for deep debugging.
* Clean exits on Ctrl+C with background sweeping of stale connections.

## Features

* Speaks [MongoDB Wire Protocol](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/) to clients:
  * `OP_MSG` (modern commands)
  * Accepts `OP_QUERY` only for legacy hello/ismaster handshake
* Forwards to upstream via the Ruby driver using `--upstream-uri` (e.g. Atlas SRV URI)
* `hello`/`ismaster` reply rewritten so clients see the proxy’s `host:port`
* Session-aware forwarding (keeps driver sessions by `lsid`)
* Type normalization (converts `txnNumber`/`getMore` to `Int64`)
* Cursor reply shaping so `cursor.id` is always `Int64` and `ns/firstBatch/nextBatch` are sane
* Verbose logging:
  * Request/response bodies are formatted as single-line JSON and are colorized using the built-in AppLogger
  * Redaction with `--redact-fields`
  * Command-level suppression with `--redact-commands` (eg. `hello`,`ismaster`)
  * Structured `OP_MSG` output with `--raw-requests` (header, flags, sections, checksum)
* Monitoring-aware: detects monitoring connections (`hello`/`ismaster` without `lsid`) and can hide their logs with `--no-monitoring-logs`
* Operational niceties: clean shutdown, non-blocking accept loop, background sweeper (`--sweep-interval`)

## Requirements

* Ruby 3.1+ (tested with 3.4.x)
* Gems:
  * `mongo`
  * `bson`

Install gems:

```bash
gem install mongo bson
```

## Quick Start

```bash
ruby mongodb_gateway.rb \
  --listen 127.0.0.1:27018 \
  --upstream-uri "mongodb+srv://USERNAME:PASSWORD@your-cluster.mongodb.net/?retryWrites=true&w=majority" \
  --redact-fields lsid,$clusterTime,operationTime \
  --redact-commands hello,ismaster
```

Then point a client at the gateway:

```bash
mongosh "mongodb://127.0.0.1:27018/test"
# Connect WITHOUT credentials/TLS to the gateway.
# The gateway authenticates upstream using --upstream-uri.
```

## Usage

```bash
ruby mongodb_gateway.rb [options]
```

### Options & Defaults

| Option                     | Description                                                                                          | Default           |
| -------------------------- | ---------------------------------------------------------------------------------------------------- | ----------------- |
| `--listen HOST:PORT`       | Address for the gateway to listen on.                                                                | `127.0.0.1:27018` |
| `--upstream-uri URI`       | **Required.** Full MongoDB connection string for the real cluster (supports SRV/TLS/auth).           | *(none)*          |
| `--redact-fields LIST`     | Comma-separated field names to redact (matches both `name` and `$name`). Example: `lsid,$clusterTime`. | *(none)*        |
| `--redact-commands LIST`   | Suppress logging for whole commands. Example: `hello,ismaster,ping`.                                 | *(none)*          |
| `--raw-requests`           | Also log a structured OP_MSG object (header, flag bits & booleans, sections, checksum).             | `false`           |
| `--no-monitoring-logs`     | Hide logs for monitoring connections (connection still handled).                                     | `false`           |
| `--no-responses`           | Suppress logging of responses.                                                                       | `false`           |
| `--sweep-interval SECONDS` | Background interval to prune closed sockets/threads.                                                 | `5.0`             |

**Redaction matching:** If you pass `clusterTime`, the gateway also matches `$clusterTime` automatically (and vice-versa).

## What gets logged

* Client connection: IP:port and (if handshake present) a fingerprint like driver/app/platform.

* Requests/Responses (REQ/RES):
  * Pretty single-line JSON with ANSI colors via AppLogger (always enabled in this build)
  * Respect `--redact-fields` and `--redact-commands`

* Raw `OP_MSG` structure when `--raw-requests` is set (shown as JSON):

```json
{
  "header": {
    "messageLength": 123,
    "requestID": 42,
    "responseTo": 0,
    "opCode": 2013,
    "opCodeName": "OP_MSG"
  },
  "flagBits": 65536,
  "flags": {
    "checksumPresent": false,
    "moreToCome": false,
    "exhaustAllowed": true
  },
  "sections": [
    {
      "kind": 0,
      "body": {"find":"foo","filter":{},"$db":"test"}
    }
  ]
}
```

> [!TIP]
> `--raw-requests` output also respects `--redact-fields`.

## Clean Shutdown

* Ctrl+C once: request graceful shutdown.
* Ctrl+C twice: force-stop lingering workers after a brief grace period.
* Non-blocking accept loop + background sweeper ensure fast, quiet exits.

## Limitations

* Security: Gateway accepts plaintext, unauthenticated client connections. Use only on trusted hosts/networks.
* Protocol: Only `OP_MSG` is fully supported; `OP_QUERY` is accepted only for initial handshake.
* Auth: Client-side auth commands (`saslStart`, `saslContinue`, `authenticate`) are rejected; the gateway authenticates upstream using `--upstream-uri`.
* Compression: Not supported from client to gateway, though network compression from gateway to cluster should work as expected.
* Throughput: Focused on observability and correctness; not tuned for high throughput/low latency.
