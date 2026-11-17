using System.Runtime.CompilerServices;

namespace Fiumara.Core;


public interface ISwarmOptions
{
    int MaxPeers { get; }
    TimeSpan HandshakeTimeout { get; }
    TimeSpan SnubTimeout { get; }
    int UnchokeSlots { get; }
    TimeSpan RechokeInterval { get; }
    TimeSpan OptimisticUnchokeInterval { get; }
}

// Behaviour the host (CLI, tests, a future web UI) can change; the defaults are what the engine always did.
// Protocol constants (block size, message ids, handshake layout) are not settings and stay in the code.
public record FiumaraOptions : ISwarmOptions
{
    public static FiumaraOptions Default { get; } = new();

    public string DownloadPath { get; init; } = "download.txt";

    // Peers kept connected at the same time
    public int MaxPeers { get; init; } = 15;

    // Block requests kept in flight per peer
    public int MaxPipelineSize { get; init; } = 3;

    // How long a block request may stay unanswered before the block is handed to another peer
    public TimeSpan RequestTimeout { get; init; } = RequestsRegister.DefaultTimeout;

    // How often the engine checks for expired requests and restarts idle peers
    public TimeSpan TickPeriod { get; init; } = TimeSpan.FromMilliseconds(500);

    public TimeSpan HandshakeTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan SnubTimeout { get; init; } = TimeSpan.FromMinutes(1);

    public int UnchokeSlots { get; init; } = 4;
    public TimeSpan RechokeInterval { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan OptimisticUnchokeInterval { get; init; } = TimeSpan.FromSeconds(30);

    // Port announced to the tracker (nothing listens on it yet)
    public int ListenPort { get; init; } = 6881;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(DownloadPath);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxPeers);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxPipelineSize);
        ThrowIfNotPositive(RequestTimeout);
        ThrowIfNotPositive(TickPeriod);
        ThrowIfNotPositive(HandshakeTimeout);
        ThrowIfNotPositive(SnubTimeout);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(UnchokeSlots);
        ThrowIfNotPositive(RechokeInterval);
        ThrowIfNotPositive(OptimisticUnchokeInterval);
        ArgumentOutOfRangeException.ThrowIfLessThan(ListenPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ListenPort, 65535);
    }

    private static void ThrowIfNotPositive(TimeSpan value, [CallerArgumentExpression(nameof(value))] string? name = null)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(name, value, "Must be positive.");
    }
}
