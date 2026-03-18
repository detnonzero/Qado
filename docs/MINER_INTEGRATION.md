# Qado Miner Integration (Node API v1)

This document is for external miner developers.
It describes everything needed to build a standalone solo miner against a Qado node without reading Qado source code.

The API runs in the same process as the Qado GUI + node.

## Base URL

- `http://127.0.0.1:18080`
- Or from remote hosts: `http://<node-ip>:18080`

Default port is `18080` (no TLS).
If TLS is required, terminate TLS on a reverse proxy.

## Auth

No API authentication is enforced by the node.
Any client that can reach the API endpoint can use it.

## Miner responsibilities

An external miner should only:

1. Check node health and tip.
2. Request a node-generated mining job.
3. Scan nonce values, optionally advancing the timestamp locally.
4. Submit `job_id + nonce + timestamp`.

The node remains responsible for:

- canonical tip selection
- target calculation
- transaction selection
- coinbase construction
- merkle composition
- final block reconstruction
- full consensus validation
- P2P broadcast

Clients must not modify coinbase, transaction selection, merkle composition, or target selection.

## Endpoints

### Recommended support

- `GET /v1/health`
- `GET /v1/tip`
- `POST /v1/mining/job`
- `POST /v1/mining/submit`

`GET /v1/health` and `GET /v1/tip` are not strictly required to hash, but they are strongly recommended for health checks, stale detection, and job refresh behavior.

## High-level mining loop

1. Call `POST /v1/mining/job` with the miner public key.
2. Receive a node-generated template tied to the current canonical tip.
3. Hash against that template.
4. Poll `GET /v1/tip` periodically.
5. If the tip changes, discard the current job and request a new one.
6. If you find a valid nonce, submit it with `POST /v1/mining/submit`.
7. On `accepted = true`, immediately request a new job.
8. On `stale_job`, `job_not_found`, or a tip change, discard the old job and request a new one.

Recommended behavior:

- poll `/v1/tip` every `1-2` seconds
- refresh the active job every `60-90` seconds even if the tip does not change
- do not keep hashing a job beyond its TTL

## Encoding rules

- all hashes and fixed-size binary fields are encoded as lowercase hex
- `miner`, `prev_hash`, `target`, `merkle_root`, and `header_hex_zero_nonce` are hex strings
- `job_id` is a 32-char lowercase hex string
- `height`, `timestamp`, `coinbase_amount`, and `nonce` are decimal strings
- `tx_count` is a JSON integer
- optimized word arrays are encoded as described below; do not assume they use the same endianness as the raw hex fields

## Request bodies

### Mining job request

`POST /v1/mining/job`

```json
{
  "miner": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

Rules:

- `miner` must be a 64-char lowercase hex string
- it represents the miner public key as 32 raw bytes

### Mining submit request

`POST /v1/mining/submit`

```json
{
  "job_id": "0123456789abcdef0123456789abcdef",
  "nonce": "123456789",
  "timestamp": "1770000001"
}
```

Rules:

- `job_id` is the opaque job identifier returned by `/v1/mining/job`
- `nonce` is an unsigned decimal integer string
- `timestamp` is an unsigned decimal Unix seconds string
- `timestamp` may be omitted; if omitted, the node uses the original job timestamp
- if provided, `timestamp` must be greater than or equal to the job timestamp

## Mining job response

`POST /v1/mining/job`

```json
{
  "job_id": "0123456789abcdef0123456789abcdef",
  "height": "15235",
  "prev_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "target": "0000000000000000000000000000000000000000000000000000000001234567",
  "timestamp": "1770000000",
  "merkle_root": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "coinbase_amount": "20000000000",
  "tx_count": 12,
  "header_hex_zero_nonce": "....",
  "precomputed_cv": "....",
  "block1_base": "....",
  "block2": "....",
  "target_words": ["00000000","00000000","00000000","00000000","00000000","00000000","00000001","23456789"]
}
```

Field meanings:

- `job_id`: opaque 16-byte random identifier encoded as 32-char lowercase hex
- `height`: next block height as a decimal string
- `prev_hash`: canonical parent block hash as 64-char lowercase hex
- `target`: 32-byte target as 64-char lowercase hex
- `timestamp`: initial block timestamp as decimal Unix seconds
- `merkle_root`: informational; already committed into the supplied header template
- `coinbase_amount`: informational; already committed into the supplied block template
- `tx_count`: informational; number of transactions in the node-generated block
- `header_hex_zero_nonce`: full hash input header with nonce set to zero
- `precomputed_cv`: optimized chaining value for the first BLAKE3 chunk
- `block1_base`: optimized second chunk words with zero nonce
- `block2`: optimized final chunk words
- `target_words`: optimized target representation for word-wise comparisons

Important:

- external miners do not need to construct the block
- external miners do not need to build the coinbase transaction
- external miners do not need to compute the merkle root
- external miners do not need to calculate target or difficulty

## Submit response

### Success

```json
{
  "accepted": true,
  "hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "height": "15235"
}
```

### Rejected

```json
{
  "accepted": false,
  "reason": "stale_job"
}
```

`reason` values may include:

- `job_not_found`
- `invalid_timestamp`
- `stale_job`
- `invalid_pow`
- `submit_rejected`

### Request validation errors

Bad requests may also return standard API errors such as:

- `invalid_miner`
- `job_build_failed`
- `invalid_request`
- `invalid_nonce`
- `invalid_timestamp`

## PoW algorithm

The proof-of-work hash is:

- algorithm: `BLAKE3`
- input: the 145-byte block header described below
- acceptance rule: `hash <= target` using a big-endian 32-byte comparison

### Header layout

The hash input is exactly 145 bytes in this order:

1. `version` : 1 byte
2. `prev_hash` : 32 bytes
3. `merkle_root` : 32 bytes
4. `timestamp` : 8 bytes, big-endian unsigned integer
5. `target` : 32 bytes
6. `nonce` : 8 bytes, big-endian unsigned integer
7. `miner` : 32 bytes

Total: `1 + 32 + 32 + 8 + 32 + 8 + 32 = 145` bytes

Current values:

- `version` is currently `1`
- `prev_hash`, `merkle_root`, `target`, and `miner` are already embedded by the node in `header_hex_zero_nonce`

### Important byte offsets

Within `header_hex_zero_nonce`:

- `timestamp` starts at byte offset `65`
- `nonce` starts at byte offset `105`

Both are encoded as big-endian unsigned 64-bit integers.

That means a simple CPU miner can work like this:

1. Decode `header_hex_zero_nonce` into 145 bytes.
2. Overwrite bytes `65..72` with the chosen timestamp if you want to advance time.
3. Overwrite bytes `105..112` with the candidate nonce.
4. Compute `BLAKE3(header_bytes)`.
5. Compare the 32-byte hash against `target`.

## Optimized mining fields

The API also returns fields intended for GPU or highly optimized miners.

### `precomputed_cv`

- 8 little-endian `uint32` words, encoded as hex
- represents the BLAKE3 chaining value after hashing the first 64-byte chunk of the header
- chunk 0 is bytes `0..63`

### `block1_base`

- 16 little-endian `uint32` words, encoded as hex
- represents header bytes `64..127`
- contains the current timestamp
- contains a zero nonce

### `block2`

- 16 little-endian `uint32` words, encoded as hex
- represents header bytes `128..144`, followed by zero-padding to 64 bytes
- the effective data length for the final chunk is `17` bytes

### `target_words`

- array of 8 hex strings
- each entry is one 32-bit word of the target interpreted as big-endian
- intended for fast word-wise target comparison in GPU kernels

### BLAKE3 chunking model used by optimized miners

The 145-byte header is split as:

- chunk 0: bytes `0..63`, length `64`, flag `CHUNK_START`
- chunk 1: bytes `64..127`, length `64`, flag `0`
- chunk 2: bytes `128..144`, length `17`, flag `CHUNK_END | ROOT`

Counter low/high are `0` for all three chunks.

A miner may ignore these optimized fields and simply hash `header_hex_zero_nonce` directly.
The optimized fields exist only to reduce repeated work in GPU kernels.

## Timestamp behavior

- the job response includes an initial `timestamp`
- miners may advance the timestamp locally while working on a job
- the submitted timestamp must be `>= job.timestamp`
- if `timestamp` is omitted on submit, the node uses the original job timestamp
- a timestamp too far in the future is rejected by consensus validation

Current future-drift tolerance is 2 hours.

## Job lifetime and stale handling

Mining jobs are short-lived in-memory templates.

Important properties:

- jobs are tied to the current canonical tip
- jobs may become stale when a new block arrives
- jobs also expire by TTL even if the tip does not change

Current TTL:

- `2 minutes`

Recommended miner behavior:

- poll `/v1/tip` every `1-2` seconds
- refresh the job every `60-90` seconds
- discard the current job immediately if the tip changes
- treat `stale_job` exactly like a signal to request a new job

## Minimal cURL smoke tests

### 1) Health

```bash
curl -sS http://127.0.0.1:18080/v1/health
```

### 2) Tip

```bash
curl -sS http://127.0.0.1:18080/v1/tip
```

### 3) Mining job

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/mining/job \
  -H "Content-Type: application/json" \
  -d "{\"miner\":\"<MINER_PUBKEY32_HEX>\"}"
```

### 4) Mining submit

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/mining/submit \
  -H "Content-Type: application/json" \
  -d "{\"job_id\":\"<JOB_ID>\",\"nonce\":\"<NONCE>\",\"timestamp\":\"<TIMESTAMP>\"}"
```

## What a miner does not need to know

An external miner does not need Qado source code to:

- compute difficulty
- derive the next target
- build the coinbase transaction
- select transactions
- build the merkle root
- serialize the final block for P2P
- speak the Qado P2P protocol

Those responsibilities remain entirely on the node side.

## Recommended release checklist

When shipping this API to third-party miner developers, also provide:

- this document
- the exact request/response schemas
- at least one real example job payload
- at least one valid test vector: `job_id + nonce + timestamp -> hash`
- one invalid example showing a rejected submit

With the information above, a developer can build a correct external miner without consulting Qado sources.
