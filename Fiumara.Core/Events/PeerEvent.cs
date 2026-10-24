namespace Fiumara.Core.Events;


public abstract record EngineEvent;

public record Tick() : EngineEvent;

public abstract record PeerEvent(PeerConnection peer) : EngineEvent;

public record BitfieldReceived(PeerConnection Peer) : PeerEvent(Peer);
public record HaveReceived(PeerConnection Peer, int PieceIndex) : PeerEvent(Peer);
public record ChokeStateChanged(PeerConnection Peer, bool IsChoked) : PeerEvent(Peer);
public record BlockReceived(PeerConnection Peer, int PieceIndex, int Offset, byte[] Data) : PeerEvent(Peer);
public record PeerDisconnected(PeerConnection Peer) : PeerEvent(Peer);
public record InterestedStateChanged(PeerConnection Peer, bool IsInterested) : PeerEvent(Peer);