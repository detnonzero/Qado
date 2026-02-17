# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

- Ongoing hardening, node stability, and UX improvements.

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

