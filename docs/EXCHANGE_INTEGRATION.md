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

## Endpoints

### System

- `GET /v1/health`
- `GET /v1/network`

### Chain

- `GET /v1/tip`
- `GET /v1/block/{block_ref}`

`block_ref` can be:
- Decimal block height (example: `12345`)
- 64-char lowercase block hash

### Address

- `GET /v1/address/{address}`

`address` must be 64-char lowercase hex.

### Transaction

- `GET /v1/tx/{txid}`
- `GET /v1/tx/{txid}/confirmations`
- `POST /v1/tx/broadcast`

`txid` must be 64-char lowercase hex.

## Broadcast request body

`POST /v1/tx/broadcast`

```json
{
  "raw_tx_hex": "f0ab...",
  "idempotency_key": "optional-exchange-key-123"
}
```

## Minimal response examples

`GET /v1/health`

```json
{
  "status": "ok",
  "network": "mainnet",
  "node_version": "1.0.0.0",
  "timestamp_utc": "2026-02-12T12:00:00.0000000Z"
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

`GET /v1/tx/{txid}/confirmations`

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

`POST /v1/tx/broadcast`

```json
{
  "accepted": true,
  "txid": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "status": "mempool",
  "error": null
}
```

## Three cURL smoke tests

### 1) Health

```bash
curl -sS http://127.0.0.1:18080/v1/health
```

### 2) Tip

```bash
curl -sS http://127.0.0.1:18080/v1/tip
```

### 3) Broadcast (replace raw hex)

```bash
curl -sS -X POST http://127.0.0.1:18080/v1/tx/broadcast \
  -H "Content-Type: application/json" \
  -d "{\"raw_tx_hex\":\"<SIGNED_RAW_TX_HEX>\",\"idempotency_key\":\"exch-test-001\"}"
```

## Notes for exchanges

- Use `confirmations` from `/v1/tx/{txid}/confirmations` for deposit credit logic.
- Treat `status = orphaned` as not confirmed.
- Amount fields are in atomic units (`decimals = 9` from `/v1/network`).
- For full schema details, see `Qado/docs/exchange-api-v1.openapi.yaml`.
