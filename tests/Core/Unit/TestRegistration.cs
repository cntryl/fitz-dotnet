namespace Cntryl.Fitz.Core.Tests.Unit;

internal sealed class TestRegistration : IDisposable
{
    private readonly Action? _onDispose;
    private int _disposed;

    internal TestRegistration(Action? onDispose = null)
    {
        _onDispose = onDispose;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _onDispose?.Invoke();
    }
}
