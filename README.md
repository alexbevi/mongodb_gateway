# mongodb_gateway

An application-layer **MongoDB proxy/gateway** written in Ruby.
Clients connect to the gateway using a plain `mongodb://host:port` URI (no TLS, no auth).
The gateway connects to your real cluster using the official Ruby driver and a full MongoDB connection string (auth, TLS, SRV, etc.) and **forwards commands** while letting you **inspect and log** what flows through.

> [!WARNING]
> This project is intended for experimentation, debugging, and education.
> It is **not** a drop-in security boundary or production proxy. Use responsibly.

## Why?

* Inspect **opcodes**, **commands**, and **responses** between a MongoDB client and cluster.
* Redact sensitive fields from logs (`lsid`, `$clusterTime`, etc.).
* Suppress noisy “hello/ismaster” chatter.
* Dump a **structured OP_MSG** view (header, flags, sections, checksum) for deep debugging.
* Clean exits on Ctrl+C with background sweeping of stale connections.

## Features

* Speaks Mongo wire to clients:
  * **OP_MSG** (modern commands)
  * Accepts **OP_QUERY** only for legacy **hello/ismaster** handshake.
* Forwards to upstream via **Ruby driver** using `--upstream-uri` (e.g. Atlas SRV URI).
* **Hello/ismaster reply rewritten** so clients see the proxy’s `host:port`.
* **Session-aware** forwarding (keeps driver sessions by `lsid`).
* **Type normalization** (e.g., converts `txnNumber`/`getMore` to `Int64`) to avoid server type errors.
* **Cursor reply shaping** so `cursor.id` is always Int64 and `ns/firstBatch/nextBatch` are sane.
* **Verbose logging**:
  * Request/response bodies (pretty single-line JSON with ANSI colors via `--json`).
  * **Redaction** with `--redact-fields`.
  * **Command-level suppression** with `--redact-commands` (e.g., `hello,ismaster`).
  * **Structured OP_MSG** with `--raw-request` (header, flags, sections, checksum).
* **Monitoring-aware**:
  * Detects monitoring connections (hello/ismaster without `lsid`).
  * Optional `--no-monitoring-logs` to hide monitoring connection lines and their REQ/RES logs.
* **Operational niceties**:
  * Clean shutdown (non-blocking accept; second Ctrl+C forces exit).
  * Background **sweeper** prunes closed sockets/threads (`--sweep-interval`).

## Requirements

* **Ruby** 3.1+ (tested with 3.4.x)
* Gems:

  * [`mongo`](https://rubygems.org/gems/mongo)
  * [`bson`](https://rubygems.org/gems/bson)

Install gems:

```bash
gem install mongo bson
```

## Quick Start

```bash
ruby mongodb_gateway.rb \
  --listen 127.0.0.1:27018 \
  --upstream-uri "mongodb+srv://USERNAME:PASSWORD@your-cluster.mongodb.net/?retryWrites=true&w=majority" \
  --json \
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

| Option                     | Description                                                                                            | Default           |
| -------------------------- | ------------------------------------------------------------------------------------------------------ | ----------------- |
| `--listen HOST:PORT`       | Address for the gateway to listen on.                                                                  | `127.0.0.1:27018` |
| `--upstream-uri URI`       | **Required.** Full MongoDB connection string for the real cluster (supports SRV/TLS/auth).             | *(none)*          |
| `--json`                   | Colorized, single-line JSON for REQ/RES logs.                                                          | `false`           |
| `--redact-fields LIST`     | Comma-separated field names to redact (matches both `name` and `$name`). Example: `lsid,$clusterTime`. | *(none)*          |
| `--redact-commands LIST`   | Suppress logging for whole commands. Example: `hello,ismaster,ping`.                                   | *(none)*          |
| `--raw-request`            | Also log a **structured OP\_MSG** object (header, flag bits & booleans, sections, checksum).           | `false`           |
| `--no-monitoring-logs`     | Hide logs for monitoring connections (connection still handled).                                       | `false`           |
| `--sweep-interval SECONDS` | Background interval to prune closed sockets/threads.                                                   | `5.0`             |

**Redaction matching:**
If you pass `clusterTime`, the gateway also matches `$clusterTime` automatically (and vice-versa).

## What gets logged?

* **Client connection**: IP\:port and (if handshake present) a fingerprint like driver/app/platform.
  Example:
  `info: Client connected: 127.0.0.1:58980 [monitoring] (nodejs|mongosh 6.17.0|2.5.5, app=mongosh 2.5.5, platform=Node.js v24.4.0, LE)`

* **Requests/Responses** (`REQ/RES`):

  * Pretty single-line JSON with ANSI colors when `--json` is set.
  * Respect `--redact-fields` and `--redact-commands`.

* **Raw OP\_MSG structure** when `--raw-request` is set (shown as JSON):

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

  * **Flags decoded** from `flagBits`:

    * `checksumPresent` (bit 0)
    * `moreToCome` (bit 1)
    * `exhaustAllowed` (bit 16)
  * **Sections** parsed:

    * **Kind 0**: single BSON document (`body`).
    * **Kind 1**: sequence — includes `identifier` and `documents[]`.

> Tip: `--raw-request` output **also** respects `--redact-fields`.

## How it works (high-level)

```
Client (no TLS/auth) ──► mongodb_gateway.rb ──► Upstream Cluster (TLS/auth via Ruby driver)
             OP_MSG / legacy OP_QUERY hello        using --upstream-uri
```

* **Hello/isMaster**: The gateway fetches the upstream response for each request, then **rewrites** it so the client sees the gateway’s `hosts`/`me`. This keeps clients “pinned” to the proxy.
* **Sessions**: A per-`lsid` map starts/reuses Ruby driver sessions; commands with `lsid` flow on the correct session.
* **Type fixes**: Converts `txnNumber`, `getMore` to `Int64` to align with server expectations.
* **Cursor replies**: Ensures `cursor.id` is Int64 and `ns/firstBatch/nextBatch` are present when needed.

## Clean Shutdown

* **Ctrl+C once**: request graceful shutdown.
* **Ctrl+C twice**: force-stop lingering workers after a brief grace period.
* Non-blocking accept loop + background sweeper ensure fast, quiet exits.

## Atlas & TLS

* With an SRV/TLS connection string in `--upstream-uri`, the **Ruby driver** validates TLS
* The gateway itself **does not** accept TLS from clients in this PoC; clients connect in plaintext to the gateway.

## Limitations

* **Security**: Gateway accepts plaintext, unauthenticated client connections. Use only on trusted hosts/networks.
* **Protocol**: Only **OP\_MSG** is fully supported; **OP\_QUERY** is accepted **only** for initial handshake.
* **Auth**: Client-side auth commands (`saslStart`, `saslContinue`, `authenticate`) are rejected; the gateway authenticates upstream using `--upstream-uri`.
* **Compression**: Not supported from client to gateway, though Network Compression from gateway to cluster should work as expected.
* **Throughput**: Focused on observability and correctness; not tuned for high throughput/low latency.