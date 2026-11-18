using System.Collections.Generic;

namespace Cntryl.Fitz.Connection;

sealed class RequestGate
{
    readonly Lock _gate = new();
    readonly LinkedList<RequestWaiter> _waiters = new();
    int _maxConcurrency;
    readonly int _maxQueueSize;
    int _activeCount;
    bool _closed;

    internal RequestGate(int maxConcurrency, int maxQueueSize)
    {
        _maxConcurrency = Math.Max(1, maxConcurrency);
        _maxQueueSize = Math.Max(0, maxQueueSize);
    }

    internal ValueTask<Releaser> AcquireAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (_closed)
            {
                throw new ConnectionException("Connection closed");
            }

            if (_activeCount < _maxConcurrency)
            {
                // Uncontended: complete synchronously rather than allocating a Task per request.
                _activeCount++;
                return new ValueTask<Releaser>(new Releaser(this));
            }

            if (_waiters.Count >= _maxQueueSize)
                throw new RequestQueueFullException();

            var waiter = new RequestWaiter(this);
            _waiters.AddLast(waiter.Node);
            waiter.RegisterCancellation(ct);
            return new ValueTask<Releaser>(waiter.Task);
        }
    }

    internal void SetMaxConcurrency(int maxConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConcurrency);
        List<RequestWaiter> granted = [];
        lock (_gate)
        {
            _maxConcurrency = maxConcurrency;
            while (!_closed && _activeCount < _maxConcurrency && _waiters.First is { } first)
            {
                _waiters.RemoveFirst();
                _activeCount++;
                granted.Add(first.Value);
            }
        }
        foreach (var waiter in granted)
        {
            waiter.Grant(new Releaser(this));
        }
    }

    internal void Close()
    {
        List<RequestWaiter> waiters;
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            waiters = [.. _waiters];
            _waiters.Clear();
        }

        foreach (var waiter in waiters)
        {
            waiter.Fail(new ConnectionException("Connection closed"));
        }
    }

    void Release()
    {
        RequestWaiter? granted = null;

        lock (_gate)
        {
            if (_activeCount > 0)
            {
                _activeCount--;
            }

            if (!_closed && _activeCount < _maxConcurrency && _waiters.First is { } first)
            {
                _waiters.RemoveFirst();
                _activeCount++;
                granted = first.Value;
            }
        }

        granted?.Grant(new Releaser(this));
    }

    internal readonly struct Releaser : IDisposable
    {
        readonly RequestGate? _owner;

        internal Releaser(RequestGate owner)
        {
            _owner = owner;
        }

        public void Dispose() => _owner?.Release();
    }

    void Cancel(RequestWaiter waiter, CancellationToken ct)
    {
        lock (_gate)
        {
            // Removing the node claims completion. A release or Close may already own it.
            if (waiter.Node.List is null)
                return;
            _waiters.Remove(waiter.Node);
        }

        waiter.Cancel(ct);
    }

    sealed class RequestWaiter
    {
        readonly RequestGate _owner;
        readonly TaskCompletionSource<Releaser> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenRegistration _registration;

        internal RequestWaiter(RequestGate owner)
        {
            _owner = owner;
            Node = new LinkedListNode<RequestWaiter>(this);
        }

        internal LinkedListNode<RequestWaiter> Node { get; }
        internal Task<Releaser> Task => _completion.Task;

        internal void RegisterCancellation(CancellationToken ct)
        {
            _registration = ct.UnsafeRegister(static (state, token) =>
            {
                var waiter = (RequestWaiter)state!;
                waiter._owner.Cancel(waiter, token);
            }, this);
        }

        internal void Grant(Releaser releaser)
        {
            _registration.Dispose();
            _completion.SetResult(releaser);
        }

        internal void Fail(Exception exception)
        {
            _registration.Dispose();
            _completion.SetException(exception);
        }

        internal void Cancel(CancellationToken ct) => _completion.SetCanceled(ct);
    }
}
