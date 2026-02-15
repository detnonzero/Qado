# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

- Ongoing hardening, node stability, and UX improvements.

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

