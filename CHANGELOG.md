# Changelog

All notable changes to this project are documented in this file.

## [1.0.0] - 2026-03-29

### Added
- A capability-gated mainnet-compatible P2P upgrade path:
  - new nodes advertise support for direct block/transaction relay, `BlocksBatchData`, IPv6 peer exchange, dual-stack networking, extended sync windows, and sync-window previews
  - older nodes continue to interoperate through the legacy handshake shape and `BlocksChunk` fallback
- True dual-stack peer support across the active networking stack:
  - parallel IPv4 + IPv6 listeners
  - IPv4/IPv6 outbound dialing
  - IPv6-capable peer normalization, storage, and peer exchange
- A headless `Qado.NodeHost` runner plus testing docs for operating relay/sync nodes without the GUI.

### Changed
- The P2P/sync architecture was redesigned toward a bitcoin-style small-network model while preserving compatibility with older live nodes:
  - fresh blocks are still propagated as full `Block` payloads
  - fresh transactions are still propagated as full `Tx` payloads
  - historical catch-up now uses request/response windows via `GetBlocksByLocator` / `GetBlocksFrom`
  - SQLite commit granularity for historical sync is now `128` blocks
- Historical sync now prefers better modern peers and pipelines work more intelligently:
  - capability-rich peers are favored for bulk sync
  - sync windows grow/shrink adaptively per peer
  - parallel historic windows scale dynamically instead of staying fixed
  - timeout handling prunes stalled windows and continues planning instead of eagerly collapsing the whole pipeline
- Relay behavior is now more robust under load:
  - block and transaction relay use per-peer queues with bounded backpressure
  - per-peer relay dedupe avoids re-pushing the same block/tx repeatedly to one peer
  - canonical-tail pushes to slightly lagging peers reuse the same peer-specific block dedupe
- Peer handling is stricter and more deterministic:
  - duplicate sessions are resolved by stable tie-breaking using the exchanged node IDs
  - the preferred surviving session is reflected in the peer view instead of showing long-lived merged `inbound+outbound` ambiguity
  - public-peer probing and peer exchange are more conservative and more IPv6-aware

### Fixed
- Nodes no longer need to upgrade together to share one mainnet:
  - mixed old/new deployments can synchronize and relay together
  - no consensus rules were changed
  - no chain reset is required
- Best-chain convergence after sync progress is much more reliable:
  - adoption is triggered after meaningful batch progress instead of only after coarse sync units
  - stronger chainwork wins more consistently across live gossip, backfill progress, and resumed sync
- A long series of historic-sync race conditions and stall modes was eliminated:
  - stale `batch start` / `batch end` frames no longer trigger unnecessary hard resets
  - preview continuations no longer fail just because the future start hash is not yet local
  - completed continuation windows now wait for earlier prepared windows instead of committing out of order
  - the planner can recover from idle/stalled states instead of silently stopping
  - repeated continuation requests for the same exhausted `fromHash` near remote tip are now suppressed
- Live orphan/sidechain handling is less expensive and less disruptive:
  - clearly out-of-plan sidechain/orphan traffic is deferred into the recovery/request-response path earlier
  - parent recovery is deduped and budgeted more tightly
  - live orphan buffering and related recovery logs are summarized instead of hot-loop spamming
  - early raw persistence of far-behind live sidechain candidates was removed from the hot path
- Transaction and block relay behave better under repeated duplicate arrival:
  - accepted transactions are relayed with peer-specific dedupe/backpressure instead of inline fanout only
  - repeated `Block already known` and duplicate-session noise is reduced substantially
  - zero-block canonical-tail pushes are no longer logged as if useful work happened

### Notes
- `1.0.0` is the first release of the redesigned compatible mainnet stack:
  - direct full-object gossip remains the primary path for fresh blocks and transactions
  - request/response remains the backfill path for missing history and missing parents
  - modern peers can use `BlocksBatchData`, while older peers remain on `BlocksChunk`
- This release is intentionally optimized for small PoW networks with very small blocks, where convergence, stability, and fast re-joining of the best chain matter more than minimizing relay bandwidth.

## [0.5.3] - 2026-03-27

### Fixed
- A block-sync disconnect race no longer leaves a stale active bulk-sync batch behind:
  - disconnects during the batch-prepare window are now aborted/cleaned up correctly
  - affected nodes can resume normal sync instead of getting stuck behind `another batch is already active`
- Nodes under heavy stale-sidechain/orphan pressure now avoid several unnecessary hot-path loops:
  - repeated unsolicited live blocks on clearly underpowered side branches are cached briefly and dropped earlier instead of being reprocessed expensively each time
  - repeated missing-parent live blocks no longer keep retriggering global `block-out-of-plan-missing-block` resync resets for the same parent hash
  - peers that repeatedly hit the live-orphan per-peer cap now enter a short local orphan cooldown instead of continuously reflooding the same hot path
  - nodes that learn about new canonical blocks through normal inbound sync/validation now broadcast fresh `TipState` updates faster instead of waiting only for slower periodic refreshes

### Changed
- P2P/sync logging was made less noisy under stale-sidechain and recovery pressure:
  - repeated `candidate chainwork too far behind` admission-gate rejects are now rate-limited and summarized
  - repeated `Validation queue full` drops are now rate-limited and summarized
  - benign orphan parent-pack send failures caused by canceled/disposed peer streams are now downgraded and summarized instead of spamming warnings
- Additional runtime throttling was added to reduce unnecessary CPU/network churn without changing consensus behavior:
  - inbound public claims are now probed via the bounded reconnect/probe path instead of firing immediate standalone probes per claim
  - duplicate idle node-status polling in the GUI was removed
  - per-peer orphan buffering now keeps newer live candidates by evicting that peer's oldest orphan entry when its local window is full
  - the per-peer orphan window was reduced from `128` to `64`
  - peers that repeatedly overflow their live-orphan window now get a short local backoff (`15s`, then `30s`, max `60s`) for further unsolicited live orphans
  - periodic `TipState` refreshes now run every `5s` instead of every `20s`
  - canonical-chain changes now also trigger an immediate `TipState` broadcast with a short `2s` cooldown
  - block-sync batch timeouts were reduced from `20s` to `10s`, while post-timeout sync-peer cooldowns were increased from `15s` to `60s` to move past stalled peers faster and avoid immediately retrying them

### Notes
- This release is a targeted hotfix/stability update for nodes that could fall behind after mid-batch peer disconnects.

## [0.5.2] - 2026-03-25

### Added
- Startup integrity auditing and guided recovery:
  - canonical chain continuity, canonical payload presence, payload hash consistency, and canonical prev-hash linkage are now checked before normal startup
  - fatal canonical corruption now offers a guided `Backup + Chain-Resync` path instead of continuing with a damaged local chain
  - wallet keys are preserved during startup recovery resyncs

### Changed
- SQLite durability is now configured more conservatively for live nodes:
  - `journal_mode=WAL` remains in place
  - `synchronous` changed from `NORMAL` to `FULL`
- Repairable derived-state drift at startup is now rebuilt automatically from the canonical chain:
  - `accounts`
  - `tx_index`
  - `state_undo`
- Mining job caching now supports multiple concurrent outstanding jobs per miner address with bounded per-miner and global caps.

### Fixed
- Aborted bulk-sync reorg batches no longer leave the local node stranded on a shortened canonical chain:
  - partially applied replacement chunks are rolled back on abort
  - the previously canonical branch is restored before sync resumes
- Known descendants of stateless-invalid blocks are now marked with `bad_ancestor` instead of leaving only the root block marked bad.

### Notes
- SelfTest coverage was expanded for startup audit/recovery, aborted reorg restore, and concurrent mining-job handling.

## [0.5.1] - 2026-03-23

### Changed
- Exchange API `GET /v1/address/{address}/incoming` now accepts optional `order=asc|desc`; default remains `asc`, and `desc` returns newest events first.

## [0.5.0] - 2026-03-21

### Added
- Exchange API transaction lookup now accepts optional `block_ref` on:
  - `GET /v1/tx/{txid}`
  - `GET /v1/tx/{txid}/confirmations`
  - this disambiguates one specific block occurrence when deterministic coinbase txids repeat across blocks

### Changed
- Exchange integration docs and the OpenAPI spec now clarify that deterministic coinbase `txid`s can repeat across blocks:
  - `block_ref` is documented as the way to resolve one specific payout occurrence
  - `GET /v1/address/{address}/incoming` plus `event_id` remains the recommended incremental polling path

### Notes
- SelfTest coverage now includes repeated deterministic coinbase lookups resolved through `block_ref`.

### Fixed
- Sync/recovery validation now preserves stable FIFO processing and no longer amplifies ancestor recovery into synthetic orphan cascades.
- Already-buffered orphans no longer retrigger parent-pack requests, resync nudges, or repeated orphan-buffer log spam.

## [0.4.4] - 2026-03-18
- A new address incoming-events API for incremental pool/exchange deposit polling:
  - `GET /v1/address/{address}/incoming` returns canonical incoming transfer events for one address
  - supports stable polling via `cursor + next_cursor`
  - supports `limit` and `min_confirmations`

## [0.4.3] - 2026-03-18

### Added
- A dedicated node mining API for external solo miners:
  - `POST /v1/mining/job` returns node-generated mining templates plus optimized hashing fields
  - `POST /v1/mining/submit` accepts `job_id + nonce + timestamp` and reconstructs/validates the block on the node side
- New miner integration documentation in `docs/MINER_INTEGRATION.md`:
  - documents the mining flow, request/response bodies, PoW header layout, byte offsets, optimized GPU fields, job lifetime, and submit failure reasons

### Notes
- External miners no longer need to replicate Qado consensus internals such as target calculation, coinbase construction, merkle building, final block serialization, or direct P2P block submission.


## [0.4.2] - 2026-03-17

### Changed
- The integrated `OpenCL` miner was tuned substantially for higher throughput:
  - larger batch-based GPU work scheduling
  - leaner hot-path queue/read handling
  - vendor-aware launch defaults for `OpenCL` devices
- Mining statistics in the GUI were expanded:
  - `Network hashrate` is now estimated from the last `60` canonical blocks
  - `Probability to find the next block` is now shown as a percentage derived from local vs. estimated network hashrate
- Mining/controls layout was hardened for smaller windows:
  - the top header/key area now remains accessible via scrollable overflow
  - the left-side control column (`Mining`, `Send Transaction`, `P2P`) now stays reachable inside its own scroll container

### Fixed
- `OpenCL` mining stats now reset correctly when a new block template starts after an external tip change.
- Mining stat rows in the GUI were aligned and reformatted for clearer value display.

## [0.4.1] - 2026-03-16

### Changed
- Integrated optional `OpenCL` mining directly into the GUI:
  - mining backend can now be switched between `CPU` and `OpenCL`
  - detected `OpenCL` devices can be selected from the mining panel
  - the selected mining backend and device are persisted across restarts
- Wallet/key selection UX was improved:
  - the last selected key is now restored on the next application start

### Fixed
- Stopping mining in `OpenCL` mode no longer races the active worker and logs a spurious `NullReferenceException`.
- Connected public peers now refresh `last_seen` in hourly batched updates so long-lived live peers do not disappear from the peer list just because the original handshake is old.

## [0.4.0] - 2026-03-13

### Changed
- Account nonce storage in SQLite was moved from `INTEGER` to `BLOB(8)`:
  - `accounts.nonce` now stores the raw `ulong` value directly
  - nonce read paths remain tolerant so serialized historical data can still be interpreted correctly during validation/apply

### Fixed
- Nonce handling was hardened consistently across mempool admission, block validation, and state application:
  - sender nonces must match the exact next expected nonce
  - nonce exhaustion is now handled explicitly instead of relying on unchecked `senderNonce + 1`
  - nonce edge cases around rollback/reorg and pending-transaction sequencing were covered by additional SelfTests
- Fixed the old `INTEGER`-storage ceiling mismatch where a block could validate near the nonce limit but fail later during state persistence.
- Local node/process coordination was hardened:
  - only one `Qado` instance per machine and network profile can now run at a time
  - the P2P listener now binds exclusively instead of allowing shared local port reuse
  - self-peer detection no longer treats a shared public WAN IP as `self`, avoiding false positives for separate hosts/VMs behind the same NAT

## [0.3.3] - 2026-03-09

### Changed
- The P2P/sync stack was redesigned for the small-network block-first model:
  - live gossip now prefers direct full `Block` push
  - peer coordination now uses `Hello` + `TipState`
  - parent recovery now uses `GetAncestorPack`
  - initial sync now runs only via `GetBlocksByLocator` / `GetBlocksFrom` plus `BlocksBatchStart` / `BlocksChunk` / `BlocksBatchEnd`
- The runtime was split into clearer components:
  - `SmallNetPeerFlow` for peer control-plane handling
  - `BlockIngressFlow` for live/requested block intake
  - `BulkSyncRuntime` for chunked initial-sync commit/finalize behavior
- Live-gossip and bulk-sync behavior were decoupled:
  - live blocks are prioritized in validation
  - known side branches are retained instead of being dropped early
  - missing parents are buffered as orphans and actively requested from the same peer
- Initial sync remains chunked for performance:
  - `64` blocks per chunk
  - up to `4096` blocks per batch
  - SQL commit remains chunkwise
  - in-memory sync history/ancestor caching is used during validation
- Legacy active runtime paths were removed from the wire flow:
  - `InvBlock` / `GetData`
  - `GetTip` / `Tip`
  - old mixed legacy sync dispatch paths

### Fixed
- Fixed long-lived peer divergence where fresh competing tip/side blocks could be ignored or mishandled outside the old active-plan path.
- Fixed orphan/parent handling so unknown-parent blocks are validated statelessly, buffered, and trigger immediate parent recovery instead of being silently lost.
- Fixed mempool drift by only removing transactions after true canonical acceptance/adoption.
- Fixed repeated initial-sync restart loops at the `4096` batch boundary:
  - continuation now resumes from the current local canonical tip
  - `more-available` continuation keeps the validation/sync gate across batch boundaries
  - coordinator-driven parallel catch-up requests are suppressed while direct block sync is already active
  - UI/local adoption and local mining no longer interfere with the active initial-sync continuation path
- Fixed diagnostics around batch continuation failures by logging both local and expected forkpoints.

### Notes
- `0.3.3` is intentionally not P2P-wire-compatible with older binaries:
  - handshake version is now `2`
  - SmallNet protocol version is now `2`
  - upgrade all peers together; mixed old/new nodes are not supported
- SelfTest coverage was expanded for orphan promotion, bulk-sync continuation, disconnect resume, active-sync suppression of parallel catch-up requests, and the new block-first peer flow.

## [0.3.2] - 2026-03-09

### Changed
- Direct block-sync batch size was increased from `1440` to `4096` blocks while keeping `64` blocks per chunk.

### Notes
- This keeps the chunk/frame format unchanged, so mixed-version peers remain compatible; older peers simply continue capping batches at `1440`.

## [0.3.1] - 2026-03-08

### Changed
- Catch-up block sync was substantially accelerated:
  - streamed `BlockChunk`s are now committed atomically per chunk instead of one SQL transaction per block
  - linear sync validation now reuses an in-memory ancestor/history cache for MTP + difficulty checks
  - this greatly reduces repeated SQLite reads and per-block commit overhead on long small-block syncs
- The legacy header-first sync path was removed:
  - `HeaderSyncManager` and the old `SyncManager` were deleted
  - outbound/inbound `GetHeaders` handling is no longer part of the active sync flow
  - direct locator-based block sync is now the only active catch-up path
- Legacy resync churn was cleaned up from the old download pipeline:
  - `inv-before-download` / `inv-out-of-plan` style resync triggers were removed
  - the remaining `BlockDownloadManager` path is now focused on inventory/payload handling instead of mixed legacy planning
- Peer liveness probing was relaxed:
  - latency probe interval increased from `15s` to `60s`
  - probe timeout set to `10s`
  - probe timeout no longer disconnects peers
- Node status in the GUI was simplified:
  - `Synced/Tip Height x/y` was removed
  - the status header now shows only the canonical `Height`
- Storage/schema cleanup for the direct-block era:
  - `header_store` was removed
  - unused `peers.pubkey` was removed
  - obsolete `block_index.file_id/file_offset/file_size` metadata was removed
  - canonical tip mirroring via `meta.LatestBlockHash` / `meta.LatestHeight` was removed
- Old PoW-era leftovers were cleaned up:
  - the unused Argon2 utility and package reference were removed
  - dead PoW tuning constants were removed
  - `BlockHeader.PowHeaderSize` was renamed to `HashInputSizeBytes`

### Fixed
- Fixed sync slowdowns caused by repeated ancestor/difficulty backwalks through SQLite during linear catch-up.
- Fixed block-presence checks to use actual payload availability in `block_payloads` instead of stale file-location metadata.
- Fixed unnecessary sync disruption from aggressive ping timeout disconnects during long chunk validation/commit phases.
- Fixed mixed legacy sync behavior where old download/resync hooks could interfere with the active direct block-sync pipeline.

### Notes
- `0.3.1` expects a fresh database after the schema cleanup. No SQLite migrations are provided for these removals; delete `data/` and resync.
- Release artifacts continue to be produced via `release.ps1` (`win-x64` + `SHA256SUMS.txt`).


## [0.3.0] - 2026-03-05

### Changed
- Consensus hashing switched to `BLAKE3`:
  - PoW/block hashing now uses the BLAKE3-based header hash path
  - transaction IDs now use BLAKE3
  - Merkle roots now use BLAKE3-based hashing with domain separation
- Block storage moved fully into SQLite:
  - serialized blocks are now stored as BLOBs in `block_payloads`
  - `blocks.dat` is no longer used/created
  - sync serving now streams serialized block payloads directly from SQLite
- Data directory layout simplified:
  - both mainnet and testnet now use the single `data/` directory
  - `data-mainnet/` and `data-testnet/` are no longer active storage targets
- Block header nonce widened from `uint32` to `ulong` to expand miner nonce space.
- Consensus block size limit increased by `4` bytes (`18045` -> `18049`) to preserve effective transaction capacity after the 64-bit nonce change.
- Initial/catch-up block sync was redesigned to direct block batches instead of header-first sync:
  - locator-driven forkpoint discovery
  - continuation via `GetBlocksFrom(last_committed_hash, max_blocks)`
  - streamed chunk delivery (`64` blocks per chunk, up to `1440` blocks per batch)
  - strict sequential validation/commit with resume/fallback behavior on disconnects or invalid batches

### Fixed
- Fixed self-mined block validation inside an open SQLite transaction:
  - newly mined tip-extending blocks no longer fail with `Rejecting mined block (current state): local tip unknown`
- Fixed self-transfer state application for `sender == recipient`:
  - old nodes could incorrectly overwrite the debit path and create inflationary balance effects
  - current nodes now apply self-transfers as net `-fee` plus normal coinbase reward when applicable

### Notes
- `0.3.0` is consensus-incompatible with earlier releases:
  - BLAKE3 changes block hashes, txids, and Merkle roots
  - the 64-bit nonce changes the serialized block header format
- Existing `data-mainnet/` / `data-testnet/` directories are not migrated automatically; the node now uses `data/` only.
- SelfTest coverage was expanded for locator/forkpoint sync, chunked batch transfer, disconnect resume, invalid-batch fallback, and the recent state/validation regressions.


## [0.2.3] - 2026-03-03

### Changed
- Sync resync-triggering was throttled to reduce header-sync churn under gossip pressure:
  - `inv-out-of-plan` resync requests are now cooldown-gated (`12s`) while download is active.
  - `block-out-of-plan-missing-header` resync requests are now cooldown-gated (`12s`).
- Download-plan retry behavior was hardened:
  - active-plan hashes previously marked `Invalid` are now eligible for retry on `plan/replan` paths, reducing stalls where `missing` could stay non-zero.
- `BlocksBatch` backpressure handling was tightened:
  - when the validation queue is full, the peer is temporarily marked range-unsupported and cooled down, then sync repumps using fallback behavior (`GetData` path).

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
