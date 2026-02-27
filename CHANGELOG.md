# Changelog

All notable changes to this project are documented in this file.

## [0.2.2] - 2026-02-27

### Changed
- Header sync batch size increased from `2000` to `144000` headers per message (`HeaderSyncManager.MaxHeadersPerMessage`), with header response timeout increased to `60s`.
- Block sync switched to `Range-first` behavior:
  - `GetBlocksRange` is now preferred for contiguous and sparse-gap fetches
  - small gap spans are requested as bounded ranges (`SparseGapRangeMaxSpan = 64`)
  - `GetData` remains as compatibility fallback when range is unavailable
- `BlocksBatch` handling hardened:
  - range timeout uses a dedicated longer timeout path
  - invalid range payloads mark peer as range-unsupported for a cooldown window and fall back to `GetData`
- Public claim handling tightened:
  - inbound public claims are probed immediately exactly once
  - failed probes mark peers as non-public
  - non-public peers are excluded from sync/header candidate usage
- Peer status in GUI now distinguishes:
  - `Connected (public)`
  - `Connected (non-public)`
- Node header UI now shows sync progress as `synced/target` tip height and refreshes continuously (1s poll).
- Reduced noisy per-block sync logging by removing repetitive canonical-extension/side-candidate info spam.
- Side-invalid handling made safer:
  - unsolicited/out-of-plan invalid side blocks are marked self-only
  - full bad-descendant propagation remains reserved for active-plan invalidation paths
- Added defensive DB access guards in hot sync paths (`BlockStore`, `BlockIndexStore`, UI snapshot readers) to reduce transient `Index out of range` exceptions from concurrent SQLite command disposal/reads.

## [0.2.1] - 2026-02-27

### Changed
- Header sync now uses up to `2` public peers in parallel (`HeaderSyncManager.MaxHeaderSyncPeers = 2`), with per-peer timeout/rotation and fallback behavior when only one peer is available.
- Block download dispatch is now capped to up to `4` peers per pump cycle (`BlockDownloadManager.MaxDownloadPeersPerPump = 4`), while remaining fully functional with a single peer.
- Added a manual `Try Sync` action in the Blocks tab to trigger immediate resync/replan from the UI (`MainWindow` + `P2PNode.RequestSyncNow`), including a short click cooldown to avoid spam.
- Public-claim probing hardened:
  - probe timeout increased (`4s` -> `8s`)
  - probe budget per reconnect tick increased (`2` -> `12`)
  - immediate probe on inbound public claims added
  - probe failure reasons are now logged
- PEX candidate selection updated to mix verified public peers with a controlled unverified share (20%) via `GetPeerCandidatesForPex`.
- Keystore portability mode added via `QADO_KEYSTORE_MODE=portable_plaintext`:
  - private keys can be stored plaintext for cross-machine portability
  - startup warning log added when plaintext mode is active
  - key list loading now fails gracefully with warning instead of crashing UI startup
- `release.ps1` switched back to self-contained single-file publish:
  - `--self-contained true`
  - `PublishSingleFile=true`
  - `EnableCompressionInSingleFile=true`
  - per-RID output directory is cleared before publish

## [0.2.0] - 2026-02-25

### Changed
- Major sync pipeline refactor across `HeaderSyncManager`, `BlockDownloadManager`, and `ValidationWorker`.
- Reworked header/block sync flow, active-plan transitions, and state progression.
- Improved peer selection and sync scheduling behavior.
- Hardened out-of-order handling and orphan buffering/promotion during sync.
- Updated replan/recovery logic for partial and in-flight downloads.
- Reduced redundant sync traffic and duplicate processing paths.
- Added guardrails for sync queue pressure and malformed/invalid sync payloads.

### Impact
- More stable catch-up behavior on long/lossy peer sessions.
- Lower chance of sync stalls and inconsistent in-flight state.
- Better resilience against malformed or untimely sync messages.

### Security
- Performed a mainnet-readiness review across consensus, P2P networking, storage, API surface, and operational defaults.
- PoW profile is intentionally fixed at `Argon2id (4 KiB, 1 iteration, parallelism 1)` for fast validation.
- Header sync currently progresses through a single active peer path per pass.
- Peer cooldown/ban enforcement is disabled by default (`QADO_ENFORCE_PEER_COOLDOWN=false`), and the seed host is exempt from fail-ban logic.
- Exchange/API has no built-in authentication and can bind publicly (`0.0.0.0`).
- Bootstrap remains seed-centered (`82.165.63.4`), and network profile switching remains compile-time (`UseTestnet`).
- SQLite is configured with `WAL + synchronous=NORMAL` (durability vs. performance tradeoff).

### Notes
- Project status remains `alpha / testnet phase`.
- Mainnet hardening is still in progress.


## [0.1.4]

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
