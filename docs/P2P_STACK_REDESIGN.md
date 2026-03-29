# P2P Stack Redesign

## 1. Ist-Analyse

- Frische `Tx`- und `Block`-Payloads wurden bereits direkt gegossiped.
- Historischer Block-Sync lief über `BlocksBatchStart` + `BlocksChunk` + `BlocksBatchEnd`.
- Das bisherige Bulk-Sync-Modell hielt viel Zustand im Runtime-Pfad und konnte Canon/Runtime während eines laufenden Syncs temporär verschieben.
- Listener, Dialing, Peer-Normalisierung und PEX waren an mehreren Stellen faktisch IPv4-zentriert.
- Peer-Exchange war kompatibel, aber IPv6 wurde weder sauber propagiert noch konsequent gespeichert.

## 2. Zielarchitektur

- Frische Blöcke: direkter `MsgType.Block`-Gossip an verbundene Peers.
- Frische Transaktionen: direkter `MsgType.Tx`-Gossip an verbundene Peers.
- Historischer Sync: weiter Request/Response über `GetBlocksByLocator` und `GetBlocksFrom`.
- Sync-Daten: kein Chunk-Modell mehr als Primärpfad, sondern Batch-Commit in 128er Einheiten.
- Neue Peers: `BlocksBatchData` für 128er Block-Batches.
- Alte Peers: Fallback auf bestehende `BlocksChunk`-Frames.
- Adoption: nach jedem committed Batch sofort `ChainSelector.MaybeAdoptNewTip(...)`.
- Dual Stack: IPv4 und IPv6 für Listener, Outbound-Dialing, Peer-Normalisierung, PeerStore und PEX.

## 3. Konkrete Änderungen

### Netzwerk / Handshake

- `Qado/Networking/P2PNode.cs`
  - IPv4- und IPv6-Listener parallel.
  - Outbound-Dialing über geordnete IPv4/IPv6-Adressauflösung.
  - Handshake bleibt bei Version `2`, erweitert aber die Payload optional um Capability-Bits.
- `Qado/Networking/HandshakeCapabilities.cs`
  - Capability-Flags für `DirectTransactions`, `DirectBlocks`, `BlocksBatchData`, `PeerExchangeIpv6`, `DualStack`, `LegacyChunkSync`.
- `Qado/Networking/PeerSession.cs`
  - Capability-Zustand pro Peer.

### Adressierung / Dual Stack

- `Qado/Networking/PeerAddress.cs`
  - Host-Normalisierung, IPv4-mapped-IPv6-Normalisierung, Endpoint-Key-Format, Public-Routability-Prüfung, Dial-Reihenfolge.
- `Qado/Networking/EndpointLogFormatter.cs`
  - saubere IPv6-Ausgabe mit Klammern.
- `Qado/Networking/PeerDialPolicy.cs`
- `Qado/Networking/SelfPeerGuard.cs`
- `Qado/Storage/PeerStore.cs`
  - konsistente Adressnormalisierung und Public-Routability auch für IPv6.

### Historischer Sync / Batching

- `Qado/Networking/Messages.cs`
  - neuer optionaler Nachrichtentyp `BlocksBatchData`.
- `Qado/Networking/BlockSyncProtocol.cs`
  - neue Batchgröße `128`.
  - neuer Frame `BlockBatchFrame`.
  - neuer Payload-Pfad `BuildBlockBatch` / `TryParseBlockBatch`.
  - Legacy-Chunk-Code nur noch als Fallback.
- `Qado/Networking/SmallNetSyncProtocol.cs`
  - `BuildBlocksBatchData` / `TryParseBlocksBatchData`.
- `Qado/Networking/BlockSyncServer.cs`
  - sendet `BlocksBatchData` nur an Peers mit Capability.
  - fällt sonst auf `BlocksChunk` zurück.
- `Qado/Networking/BlockSyncClient.cs`
  - sammelt eingehende Blöcke peer-seitig.
  - committed immer in 128er Batches oder am Batch-Ende.
  - bevorzugt stärkere Chains und batchfähige Peers.
  - setzt Resume-Punkt auf zuletzt committed Hash.
- `Qado/Networking/BlockDownloadManager.cs`
  - kontrolliert paralleleres Backfill (`MaxInflightGlobal = 4`, sequentielle Fenstergröße `128`).

### Persistenz / Adoption / Reorg-Verhalten

- `Qado/Networking/BulkSyncRuntime.cs`
  - entfernt das alte Chunk-/Rollback-Live-Modell.
  - validiert und persistiert Batch-Blöcke atomar in SQLite.
  - versucht nach jedem Commit sofort die Best-Chain-Adoption.
  - `AbortBatchAsync` ist jetzt ein No-Op, weil kein temporärer Canon-Zwischenzustand mehr erzeugt wird.

## 4. Alt/Neu-Kompatibilität

- Handshake bleibt formatkompatibel:
  - alte Payload-Länge `36` Byte bleibt gültig.
  - neue Nodes lesen fehlende Capability-Felder als `None`.
  - alte Nodes akzeptieren längere Handshakes weiter, weil sie nur die Mindestlänge erwarten.
- Frische `Tx`- und `Block`-Gossip-Pfade bleiben bei bekannten Message-Typen.
- Neues `BlocksBatchData` wird capability-basiert nur an neue Peers gesendet.
- Alte Peers erhalten weiterhin `BlocksChunk`.
- IPv6-PEX wird nur an Peers mit `PeerExchangeIpv6` propagiert.
- Alte IPv4-only Peers bleiben dadurch nutzbare Teilnehmer und werden nicht durch unbekannte Peer-Daten oder neue Sync-Frames gestört.

## 5. Vermeidete Fallstricke

- Keine temporäre Canon-Umschaltung mehr während eines laufenden Bulk-Syncs.
- Adoption läuft nach jedem Batch-Commit statt erst nach sehr groben Einheiten.
- Persistenz und Runtime-Tip bleiben enger gekoppelt.
- Direct Block Gossip erzeugt zwar Redundanz, verlässt sich aber nicht auf INV/GetData.
- Fehlende Eltern bleiben über Backfill-/Recovery-Pfade nachladbar.
- Resume nach Disconnect startet vom letzten sinnvollen Commitpunkt oder re-anchor-t per Locator, wenn der Zustand unsicher ist.
- IPv4-mapped-IPv6-Adressen werden normalisiert, damit keine doppelten Peer-Identitäten entstehen.

## 6. Offene Punkte / Risiken

- `Qado/Networking/GenesisDetector.cs` ist weiterhin IPv4-zentriert und sollte bei Bedarf separat dual-stack-fähig gemacht werden. Er liegt aktuell nicht auf dem kritischen Sync-Pfad.
- `BlocksBatchData` erhöht kurzzeitig die Payload-Größe pro Frame; Stabilität wurde bewusst über Bandbreiten-Minimierung priorisiert.
- Mit nur zwei neuen Mainnet-Nodes bleibt der Nutzen von IPv6-PEX und BatchData zunächst begrenzt, bis mehr Peers diese Capabilities sprechen.

## 7. Empfohlene Umsetzungsreihenfolge im Mainnet

1. Zuerst die zwei neuen öffentlichen Nodes mit dem neuen Handshake-, Dual-Stack- und Sync-Stack ausrollen.
2. Danach Live beobachten:
   - Peer-Mix alt/neu
   - IPv4/IPv6 Reachability
   - Batch-Commit-Latenz
   - Chain-Adoption nach Backfill
   - Orphan-/Recovery-Raten
3. Erst nach stabiler Beobachtung weitere öffentliche Nodes auf den Capability-Pfad umstellen.
4. Legacy-Chunk-Fallback erst entfernen, wenn praktisch keine alten Mainnet-Peers mehr darauf angewiesen sind.
