# Node Host Testing

`Qado.NodeHost` is a small headless runner for multi-node soak and sync tests without the WPF GUI.

## Build

```powershell
dotnet build Qado.sln -c Release
```

## Basic Run

```powershell
dotnet run --project Qado.NodeHost -c Release -- --label node-1
```

## Useful Options

- `--data-dir <path>`
- `--p2p-port <port>`
- `--api-port <port>`
- `--peer <host:port>` repeatable
- `--no-default-seed`
  - disables the built-in genesis seed/known-peer reconnect path
  - if explicit `--peer` entries are present, the host keeps retrying only those peers

IPv6 bootstrap peers must be written as `[ipv6]:port`.

## Environment Variables

- `QADO_NODE_LABEL`
- `QADO_NODE_DATA_DIR`
- `QADO_NODE_P2P_PORT`
- `QADO_NODE_API_PORT`
- `QADO_NODE_BOOTSTRAP_PEERS`
- `QADO_NODE_NO_DEFAULT_SEED`

## Example 6-Node Mesh

For an isolated 6-node mesh that should not auto-dial the built-in mainnet seed, add `--no-default-seed` everywhere.

Node 1:

```powershell
dotnet run --project Qado.NodeHost -c Release -- --label n1 --api-port 18081 --no-default-seed
```

Node 2:

```powershell
dotnet run --project Qado.NodeHost -c Release -- --label n2 --api-port 18082 --no-default-seed --peer 10.0.0.11:33333
```

Node 3:

```powershell
dotnet run --project Qado.NodeHost -c Release -- --label n3 --api-port 18083 --no-default-seed --peer 10.0.0.11:33333 --peer 10.0.0.12:33333
```

Repeat the same pattern for nodes 4-6 so each node has at least 2 bootstrap edges.

## Suggested First Soak Test

1. Start all 6 nodes.
2. Verify each node forms multiple outbound/inbound sessions.
3. Trigger `Try Sync`-equivalent behavior by restarting one lagging node and observing catch-up.
4. Check logs for:
   - handshake capability negotiation
   - `BlocksBatchData` vs legacy fallback
   - chain adoption after batch progress
   - orphan parent recovery
   - IPv4/IPv6 connection diversity
