using System.Runtime.CompilerServices;

namespace Fiumara.Core;

// Behaviour the host (CLI, tests, a future web UI) can change; the defaults are what the engine always did.
// Protocol constants (block size, message ids, handshake layout) are not settings and stay in the code.
public record FiumaraOptions
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
        ArgumentOutOfRangeException.ThrowIfLessThan(ListenPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ListenPort, 65535);
    }

    private static void ThrowIfNotPositive(TimeSpan value, [CallerArgumentExpression(nameof(value))] string? name = null)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(name, value, "Must be positive.");
    }
}
