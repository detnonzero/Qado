# Qado Exchange Integration (Node API v1)

This document is for exchange listing and wallet integration teams.
The API runs in the same process as the Qado GUI + node.

## Base URL

- `http://127.0.0.1:18080`
- Or from remote hosts: `http://<node-ip>:18080`

Default port is `18080` (no TLS).  
If TLS is required, terminate TLS on a reverse proxy.

## Auth

No API authentication is enforced by the node.
Any client that can reach the API endpoint can use it.
For exchange use, expose the API only on a trusted internal network or behind a reverse proxy/VPN/TLS terminator.

## Endpoints

### System

- `GET /v1/health`
- `GET /v1/readiness`
- `GET /v1/network`
- `GET /v1/asset`

### Chain

- `GET /v1/tip`
- `GET /v1/block/{block_ref}`

`block_ref` can be:
- Decimal block height (example: `12345`)
- 64-char lowercase block hash

### Address

- `GET /v1/address/{address}`
- `POST /v1/address/validate`
- `GET /v1/address/{address}/incoming`

`address` must be a 32-byte public key encoded as 64-char lowercase hex.
QADO does not require a separate on-chain address derivation step beyond the public key, so exchanges can generate keypairs externally with standard `ed25519` tooling and use `/v1/address/validate` as a format/normalization check.

### Transaction

- `GET /v1/tx/{txid}`
- `GET /v1/tx/{txid}/confirmations`
- `POST /v1/tx/broadcast`

### Mining

- `POST /v1/mining/job`
- `POST /v1/mining/submit`

`txid` must be 64-char lowercase hex.

Optional query parameter on both transaction `GET` endpoints:
- `block_ref=<height|hash>` to resolve one specific block occurrence of a transaction

## Broadcast request body

`POST /v1/tx/broadcast`

```json
{
  "raw_tx_hex": "f0ab...",
  "idempotency_key": "optional-exchange-key-123"
}
```

## Address validation request body

`POST /v1/address/validate`

```json
{
  "address": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
}
```

## Mining job request body

`POST /v1/mining/job`

```json
{
  "miner": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

## Mining submit request body

`POST /v1/mining/submit`

```json
{
  "job_id": "0123456789abcdef0123456789abcdef",
  "nonce": "123456789",
  "timestamp": "1770000001"
}
```

## Minimal response examples

`GET /v1/health`

```json
{
  "status": "ok",
  "network": "mainnet",
  "node_version": "1.0.1+buildmeta",
  "timestamp_utc": "2026-02-12T12:00:00.0000000Z"
}
```

`GET /v1/readiness`

```json
{
  "ready": true,
  "reason": null,
  "initial_block_sync_active": false,
  "tip_height": "15234",
  "tip_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "peer_count": 8,
  "node_version": "1.0.1+buildmeta",
  "timestamp_utc": "2026-03-31T18:00:00.0000000Z"
}
```

`reason` is one of:
- `node_unavailable`
- `tip_unavailable`
- `no_connected_peers`
- `peer_tip_ahead`

`initial_block_sync_active` is diagnostic only. A node can still report `ready=true` while this flag is `true` if no connected peer currently advertises a better tip than the local canonical chain.

`GET /v1/asset`

```json
{
  "chain_name": "qado",
  "symbol": "QADO",
  "decimals": 9,
  "chain_id": "1",
  "network_id": "1",
  "genesis_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "address_format": "hex32_pubkey",
  "public_key_curve": "ed25519",
  "memo_required": false,
  "recommended_deposit_confirmations": "6",
  "tx_broadcast_mode": "pre_signed_raw_tx"
}
```

`GET /v1/tip`

```json
{
  "height": "15234",
  "hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "timestamp_utc": "2026-02-12T11:59:48.0000000Z",
  "chainwork": "932998193"
}
```

`GET /v1/tx/{txid}/confirmations?block_ref=15216`

```json
{
  "txid": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
  "status": "confirmed",
  "confirmations": "18",
  "block_hash": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  "block_height": "15216",
  "tip_height": "15234"
}
```

For deterministic coinbase transactions, the same `txid` can legitimately appear in multiple blocks. Use `block_ref` or the address incoming-event context when you need one specific payout occurrence.

`GET /v1/address/{address}/incoming?cursor=<CURSOR>&limit=200&min_confirmations=6`

```json
{
  "address": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "tip_height": "15234",
  "next_cursor": "15210:3:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "items": [
    {
      "event_id": "15210:3:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "txid": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
      "status": "confirmed",
      "block_height": "15210",
      "block_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "confirmations": "25",
      "timestamp_utc": "2026-02-12T10:12:03.0000000Z",
      "to_address": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "amount_atomic": "2500000000",
      "from_address": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "from_addresses": [
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      ],
      "tx_index": 3,
      "transfer_index": 0
    }
  ]
}
```

`POST /v1/tx/broadcast`

```json
{
  "accepted": true,
  "txid": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "status": "mempool",
  "error": null
}
```

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

`POST /v1/mining/submit`

```json
{
  "accepted": true,
  "hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "height": "15235"
}
```

`POST /v1/address/validate`

```json
{
  "valid": true,
  "normalized": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "address_format": "hex32_pubkey",
  "public_key_curve": "ed25519",
  "reason": null
}
```

## cURL smoke tests

### 1) Health

```bash
curl -sS http://127.0.0.1:18080/v1/health
```

### 2) Readiness

```bash
curl -sS http://127.0.0.1:18080/v1/readiness
```

### 3) Asset metadata

```bash
curl -sS http://127.0.0.1:18080/v1/asset
```

### 4) Address validation

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/address/validate \
  -H "Content-Type: application/json" \
  -d "{\"address\":\"<PUBKEY32_HEX>\"}"
```

### 5) Tip

```bash
curl -sS http://127.0.0.1:18080/v1/tip
```

### 6) Broadcast (replace raw hex)

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/tx/broadcast \
  -H "Content-Type: application/json" \
  -d "{\"raw_tx_hex\":\"<SIGNED_RAW_TX_HEX>\",\"idempotency_key\":\"exch-test-001\"}"
```

### 7) Mining job

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/mining/job \
  -H "Content-Type: application/json" \
  -d "{\"miner\":\"<MINER_PUBKEY32_HEX>\"}"
```

### 8) Mining submit

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/mining/submit \
  -H "Content-Type: application/json" \
  -d "{\"job_id\":\"<JOB_ID>\",\"nonce\":\"<NONCE>\",\"timestamp\":\"<TIMESTAMP>\"}"
```

## Notes for exchanges

- Use `confirmations` from `/v1/tx/{txid}/confirmations` for deposit credit logic.
- Use `/v1/readiness` to decide whether the node is ready for deposit polling and transaction broadcast.
- Use `/v1/asset` as the node-provided chain metadata surface for exchange/backoffice integration.
- For deterministic coinbase lookups, pass `block_ref` on `/v1/tx/{txid}` or `/v1/tx/{txid}/confirmations` to disambiguate a specific block occurrence.
- Prefer `GET /v1/address/{address}/incoming` plus `event_id` for incremental deposit polling. `order=desc` is available if your backend prefers newest-first pages.
- `POST /v1/address/validate` only validates and normalizes the public-key address format; exchanges are expected to manage keypair generation externally.
- The incoming-events `cursor` is opaque; persist and replay `next_cursor` exactly as returned.
- Incoming events are canonical-only and currently return `status = confirmed`.
- Treat `status = orphaned` as not confirmed.
- Amount fields are in atomic units (`decimals = 9` from `/v1/asset` or `/v1/network`).
- `node_version` may include build metadata suffixes such as `1.0.1+<commit>`.
- Do not treat `initial_block_sync_active` alone as a hard not-ready signal; use the top-level `ready` and `reason` values.
- Mining jobs are short-lived in-memory templates tied to the current canonical tip.
- `submit` only accepts node-generated templates; clients cannot modify coinbase, transaction selection, or merkle composition.
- For full schema details, see `Qado/docs/exchange-api-v1.openapi.yaml`.
