namespace Cntryl.Fitz.Core.Tests.Unit;

sealed class TestRegistration : IDisposable
{
    readonly Action? _onDispose;
    int _disposed;

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
