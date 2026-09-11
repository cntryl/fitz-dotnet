using System.Collections.Generic;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Connection;

sealed class RequestGate
{
    readonly object _gate = new();
    readonly Queue<RequestWaiter> _waiters = new();
    readonly int _maxConcurrency;
    readonly int _maxQueueSize;
    int _activeCount;
    bool _closed;

    internal RequestGate(int maxConcurrency, int maxQueueSize)
    {
        _maxConcurrency = Math.Max(1, maxConcurrency);
        _maxQueueSize = Math.Max(0, maxQueueSize);
    }

    internal ValueTask<Releaser> AcquireAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
            {
                throw new RequestQueueFullException();
            }

            var waiter = new RequestWaiter(this, cancellationToken);
            _waiters.Enqueue(waiter);
            return new ValueTask<Releaser>(waiter.Task);
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

            while (!_closed && _activeCount < _maxConcurrency && _waiters.Count > 0)
            {
                var next = _waiters.Dequeue();
                if (next.IsCanceled)
                {
                    continue;
                }

                _activeCount++;
                granted = next;
                break;
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

    sealed class RequestWaiter
    {
        readonly object _gate = new();
        readonly CancellationTokenRegistration _registration;
        readonly TaskCompletionSource<Releaser> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        bool _completed;

        internal RequestWaiter(RequestGate owner, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled)
            {
                _registration = cancellationToken.Register(static state =>
                {
                    ((RequestWaiter)state!).Cancel();
                }, this);
            }
        }

        internal Task<Releaser> Task => _tcs.Task;

        internal bool IsCanceled => _tcs.Task.IsCanceled;

        internal void Grant(Releaser releaser)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    releaser.Dispose();
                    return;
                }

                _completed = true;
            }

            _registration.Dispose();
            _tcs.TrySetResult(releaser);
        }

        internal void Fail(Exception exception)
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                _completed = true;
            }

            _registration.Dispose();
            _tcs.TrySetException(exception);
        }

        void Cancel()
        {
            lock (_gate)
            {
                if (_completed)
                {
                    return;
                }

                _completed = true;
            }

            _registration.Dispose();
            _tcs.TrySetCanceled();
        }
    }
}
