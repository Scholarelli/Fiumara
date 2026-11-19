# Fiumara

Eventually it'll turn into a beatiful torrent client, maybe even webclient?
Give it some time, let it grow.

## Where it stands

- Downloads single-file torrents from the peers an HTTP tracker returns.
- One event loop per torrent owns all the state with actors talking to it through events.
- Picks partial pieces first, then the rarest ones, and keeps a small pipeline of requests per peer.
- Hands expired requests to other peers and stops asking peers that snub us, disconnecting them after a while.
- Verifies every piece with SHA-1 and knows which pieces it has (verified and written to disk).
- Keeps in memory only the pieces in progress: a piece's buffer is released once it is on disk.
- Uploads too: sends its bitfield, unchokes the interested peers that give it the most plus an optimistic one (never
  giving a regular slot to a peer that snubs it), and serves their requests by reading the blocks back from disk.

## Roadmap

### Now: becoming a seeder

- [x] **Piece state.** Answer "do I have piece i?" and "do I have everything?".
- [ ] **Shutdown and an explicit lifecycle.** One way to stop ticker, peers, storage and loop, used when the
  download ends, when the host asks (`StopAsync`/`DisposeAsync`) and when the loop crashes. `Completion` means
  "the engine stopped".
- [x] **Upload on the connections we already have.** Our bitfield, a choker written as a pure function, and
  requests served from disk, with `Cancel` honoured and the queue of a peer we choke dropped.
- [ ] **Incoming connections.** Listen on `ListenPort` and accept the inbound handshake.
- [ ] **Resume after a restart.** Hash-check the pieces already on disk at startup.

### Then: a well-behaved peer

- [x] **Interest.** Be interested in a peer when it has a piece we lack, not when the picker has a free block for it.
- [ ] **Choking, the finer points.** Once we have everything, rank peers by what we send them (or take turns);
  unchoke at once a peer that becomes interested while a slot is free; favour newcomers for the optimistic slot.
- [ ] **Missing timeouts.** TCP connect, sending keep-alives, dropping silent peers, peers that choke us forever.
- [ ] **Tracker.** Re-announce at the interval the tracker gives, report real `uploaded`/`downloaded`/`left`,
  send the `started`/`completed`/`stopped` events.
- [ ] **Options.** Reject a `TickPeriod` above what `PeriodicTimer` accepts.

### Later: a session above the torrents

- [ ] **A session** owning what is shared: the listener, the peer id (a static today), a global choker and
  connection limits, disk I/O as an actor of its own. Shared resources get injected into `TorrentHandle`.
- [ ] **Many torrents at once**, deciding between one loop per torrent and one per session.
- [ ] **Progress for UIs**: an immutable snapshot with verified bytes and transfer rates.
- [ ] **Multi-file torrents.**
- [ ] **Disk cache** to avoid reading the same piece from disk for multiple peers.
- [ ] **Optimization** to reduce allocations.
- [ ] **Web UI.**

### Maybe: protocol extensions

- [ ] Endgame mode.
- [ ] Fast extension (BEP 6).
- [ ] Magnet links and metadata exchange (BEP 9, BEP 10).
- [ ] DHT (BEP 5) and peer exchange (BEP 11).
- [ ] UDP trackers (BEP 15).
