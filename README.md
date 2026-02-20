# Qado

Qado is a from-scratch account-based blockchain node with a built-in WPF wallet UI, P2P networking, block sync, mining, mempool, and exchange API endpoints.

Current status: **alpha / testnet phase**.

## Repository Layout

- `Qado/` - main WPF wallet + node application
- `Qado.SelfTest/` - self-test executable
- `docs/EXCHANGE_INTEGRATION.md` - exchange/node API integration guide
- `docs/exchange-api-v1.openapi.yaml` - OpenAPI schema

## Requirements

- .NET SDK 10.0+
- Windows (WPF target: `net10.0-windows7.0`)

## Build

```powershell
dotnet build Qado.sln -c Release
```

## Run

```powershell
dotnet run --project Qado/Qado.csproj -c Release
```

The app starts:

- wallet UI
- embedded node
- embedded exchange API host

## Network Defaults (current source defaults)

- P2P port: `34000`
- API port: `18080` (override via `QADO_API_PORT`)
- Seed host: `82.165.63.4`

Important: verify network parameters for each release/tag before joining a network.

## API

- Base URL (local): `http://127.0.0.1:18080`
- Docs: `docs/EXCHANGE_INTEGRATION.md`
- OpenAPI: `docs/exchange-api-v1.openapi.yaml`

## Testnet Release Expectations

A release should include:

- source tag/commit
- binaries
- SHA256 checksums
- clear network/start information (UTC start time, chain/network IDs, seed peers)

## Security

See `SECURITY.md` for reporting guidance.

## License

This project is licensed under the MIT License. See `LICENSE`.

