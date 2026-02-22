# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Reorg-aware mempool reconciliation:
  - transactions from reorged-out canonical blocks are re-evaluated and requeued when still valid
  - reconcile result now exposes requeued transaction payloads for downstream handling
- Reorg requeue gossip pipeline:
  - requeued transactions are broadcast again after reorg adoption
  - deduped with short-lived LRU set and trickle-style burst pacing
  - per-adoption broadcast cap to avoid gossip storms
- Block sync reachability memory:
  - successful outbound dials are tracked as "dialable" sync candidates
  - sync now prefers exactly one known-dialable peer per pass
- SelfTest coverage for reorg adoption + mempool reconcile behavior.

### Changed

- Block sync transfer mode:
  - windowed in-flight block requests with configurable size (`QADO_BLOCKSYNC_WINDOW`)
  - enforced window clamp range: `360..1440` (default `720`)
- Block sync peer selection:
  - no blind dialing over full peer list during sync loop
  - if no known-dialable sync peer exists, loop idles with explicit reason `no-dialable-peers`
- TIP handling during sync:
  - chainwork-aware comparison before adoption/catch-up decisions
- Immediate sync trigger throttling hardened:
  - separate cooldown for handshake-driven triggers
  - additional per-peer handshake-trigger cooldown
- P2P inbound block handling hardened:
  - per-peer inbound block rate limiting
  - orphan buffering/promotion flow for missing-parent arrivals
  - duplicate "block already known" logs are rate-limited
- PEX behavior tightened:
  - inbound PEX now announce-only for unknown peers (does not refresh `last_seen` of existing rows)
  - outbound PEX only advertises peers with `last_seen > 0`
  - PEX sent/received logs are now suppression-window based to reduce spam
- Peer persistence defaults:
  - peer TTL fallback reduced from `21` days to `1` day
  - prune keeps announce-only rows (`last_seen = 0`) until global/per-IP limits trim them
- Ban behavior:
  - configured seed host is now exempt from fail-ban logic
- GUI updates:
  - `State` tab entries are now hard-sorted by descending balance
  - peers with no observed `last_seen` are shown as `-`
  - peer status now distinguishes `Stale (cooling down)`
- Chain adoption call sites now pass mempool context consistently (`MainWindow`, mining path, persist path).

### Notes

- Release artifacts continue to be produced via `release.ps1` (`win-x64` + `SHA256SUMS.txt`).

## [0.1.3] - 2026-02-20

### Changed

- Peer persistence hardened at storage level:
  - private/non-routable IPv4 endpoints are rejected on insert (`PeerStore.UpsertByEndpoint`)
  - existing private/non-routable peer rows are removed during prune
- Block sync catch-up behavior improved:
  - after reaching a remote tip snapshot, tip is re-queried immediately on the same session
  - sync continues without waiting for the next outer round if the remote tip advanced meanwhile
  - in-sync idle base delay reduced (`30s` -> `10s`)
  - out-of-order gossip block with missing prev now triggers immediate sync request
- UI responsiveness improved after sync/adoption phases:
  - tip adoption is queued on a background worker
  - account/peer reloads and node-status reads moved off the UI thread (request-coalesced workers)

## [0.1.2] - 2026-02-20

### Added

- Automatic startup dialing now targets configured seed plus known peers (`GenesisBootstrapper` -> `ConnectSeedAndKnownPeersAsync`).
- Background reconnect loop for running nodes with jittered intervals:
  - fast retries when disconnected
  - slower steady reconnect attempts when already connected

### Changed

- Peer exchange (PEX) filtering hardened:
  - only public-routable IPv4 endpoints are accepted and advertised
  - self endpoints are filtered for both inbound and outbound PEX
  - configured seed endpoint is excluded from inbound and outbound PEX payload handling
- Bootstrap log message updated to reflect seed+peer dialing and reconnect loop startup.

## [0.1.1] - 2026-02-17

### Added

- Peer status labels in GUI `Peers` tab:
  - `Connected`
  - `Cooling down`
  - `Seen recently`
  - `Stale`

### Changed

- Block sync wake-up behavior improved:
  - after a successful handshake, an immediate sync pass is triggered
  - clicking `Connect` in GUI now leads to immediate block sync trigger (after handshake)
  - 5-second cooldown added for immediate sync triggers to avoid sync spam
- Node status header is now wired and updated live (`Tip Height`, `Tip Hash`, `Mempool`, `Peers`).
- Node status updates are centralized and debounced for smoother UI refresh.
- Left-side GUI column widened (`420` -> `460`) so controls fit better in `Mining`, `Send Transaction`, and `P2P` panels.

### Notes

- Release artifacts built via `release.ps1` (`win-x64` + `SHA256SUMS.txt`).

## [0.1.0-alpha-testnet] - 2026-02-15

### Added

- Embedded exchange API (`/v1/*`) and integration docs.
- Block sync and peer discovery loops for bootstrapping.
- Built-in block, mempool, peers, state, and transactions views in GUI.
- Persistent app log file (`data/app.log`).

### Changed

- Block sync continues after initial canonical catch-up.
- GUI tables improved for larger/full content visibility.
- Shutdown path hardened to stop background services on window close.

### Notes

- Alpha quality intended for testnet usage and iterative hardening.
- Mainnet assumptions should not be made from this release.

