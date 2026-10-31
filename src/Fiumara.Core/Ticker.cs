using System.Runtime.CompilerServices;
using Fiumara.Core.Events;

namespace Fiumara.Core;

public static class Ticker
{
    public static async IAsyncEnumerable<Tick> StartAsync(TimeSpan period, TimeProvider? timeProvider,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var timer = new PeriodicTimer(period, timeProvider ?? TimeProvider.System);

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            yield return new Tick();
        }
    }
}