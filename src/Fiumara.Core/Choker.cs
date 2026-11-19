namespace Fiumara.Core;

internal readonly record struct ChokeCandidate(PeerConnection Peer, bool Interested, bool Snubbing, long BytesReceived);

internal sealed record ChokeDecision(IReadOnlySet<PeerConnection> Unchoked, PeerConnection? Optimistic);

internal static class Choker
{
    public static ChokeDecision Decide(IReadOnlyList<ChokeCandidate> candidates, int slots,
        PeerConnection? optimistic, bool rotateOptimistic, Random random)
    {
        var interested = candidates.Where(c => c.Interested).ToList();
        var regular = interested.Where(c => !c.Snubbing).OrderByDescending(c => c.BytesReceived).Take(slots - 1)
            .Select(c => c.Peer).ToList();
        var others = interested.Select(c => c.Peer).Where(peer => !regular.Contains(peer)).ToList();

        if (rotateOptimistic || optimistic is null || !others.Contains(optimistic))
            optimistic = PickOptimistic(others, optimistic, random);

        var unchoked = new HashSet<PeerConnection>(regular, ReferenceEqualityComparer.Instance);
        if (optimistic is not null)
            unchoked.Add(optimistic);
        return new ChokeDecision(unchoked, optimistic);
    }

    private static PeerConnection? PickOptimistic(List<PeerConnection> others, PeerConnection? current, Random random)
    {
        var newcomers = others.Where(peer => !ReferenceEquals(peer, current)).ToList();
        var pool = newcomers.Count > 0 ? newcomers : others;
        return pool.Count == 0 ? null : pool[random.Next(pool.Count)];
    }
}
