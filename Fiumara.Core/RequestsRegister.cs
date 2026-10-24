using System.Diagnostics.CodeAnalysis;

namespace Fiumara.Core;

// Single source of truth for which blocks are currently requested, from which peer and since when.
// Not thread-safe: it is meant to be used only from the engine's event loop.
public class RequestsRegister
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly record struct PendingRequest(PeerConnection Peer, long RequestedAt);

    // Of course blocks are compared by reference
    private readonly Dictionary<Block, PendingRequest> _pending = new(ReferenceEqualityComparer.Instance);
    private readonly TimeProvider _time;

    public TimeSpan Timeout { get; }

    public RequestsRegister(TimeSpan? timeout = null, TimeProvider? timeProvider = null)
    {
        Timeout = timeout ?? DefaultTimeout;
        if (Timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), Timeout, "The request timeout must be positive.");

        _time = timeProvider ?? TimeProvider.System;
    }

    public bool TryAdd(Block block, PeerConnection peer) =>
        _pending.TryAdd(block, new PendingRequest(peer, _time.GetTimestamp()));

    public bool TryRemove(Block block, [NotNullWhen(true)] out PeerConnection? requestedFrom)
    {
        if (_pending.Remove(block, out var request))
        {
            requestedFrom = request.Peer;
            return true;
        }

        requestedFrom = null;
        return false;
    }

    public bool IsPending(Block block) => _pending.ContainsKey(block);

    public int CountPeerPending(PeerConnection peer) => _pending.Values.Count(r => r.Peer == peer);

    public IReadOnlyList<Block> RemovePeerPending(PeerConnection peer)
    {
        var released = _pending.Where(kvp => kvp.Value.Peer == peer).Select(kvp => kvp.Key).ToList();
        foreach (var block in released)
            _pending.Remove(block);

        return released;
    }

    public IReadOnlyList<(Block Block, PeerConnection Peer)> RemoveExpired()
    {
        var now = _time.GetTimestamp();
        var expired = _pending
            .Where(kvp => _time.GetElapsedTime(kvp.Value.RequestedAt, now) > Timeout)
            .Select(kvp => (Block: kvp.Key, kvp.Value.Peer))
            .ToList();

        foreach (var (block, _) in expired)
            _pending.Remove(block);

        return expired;
    }
}
