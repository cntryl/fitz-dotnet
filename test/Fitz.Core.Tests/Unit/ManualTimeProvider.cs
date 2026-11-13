namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// A <see cref="TimeProvider"/> whose clock moves only when a test calls <see cref="Advance"/>.
/// Due timers fire in due-time order on the advancing thread.
/// </summary>
sealed class ManualTimeProvider : TimeProvider
{
    readonly object _sync = new();
    readonly List<ManualTimer> _timers = [];
    DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    long _elapsedTicks;

    public override DateTimeOffset GetUtcNow()
    {
        lock (_sync)
            return _now;
    }

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp()
    {
        lock (_sync)
            return _elapsedTicks;
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ManualTimer(this, callback, state);
        lock (_sync)
            _timers.Add(timer);
        timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Moves the clock forward, firing every timer that falls due on the way.</summary>
    /// <param name="duration">How far to move the clock.</param>
    public void Advance(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(duration, TimeSpan.Zero);
        long target;
        lock (_sync)
            target = _elapsedTicks + duration.Ticks;

        while (true)
        {
            ManualTimer? due;
            lock (_sync)
            {
                due = _timers
                    .Where(timer => timer.DueAt <= target)
                    .MinBy(timer => timer.DueAt);
                var next = due?.DueAt ?? target;
                _now += TimeSpan.FromTicks(next - _elapsedTicks);
                _elapsedTicks = next;
                due?.Reschedule();
            }

            if (due is null)
                return;

            due.Fire();
        }
    }

    /// <summary>
    /// Waits, in real time, until code under test has scheduled a timer due exactly
    /// <paramref name="dueIn"/> from now. Advancing before then would race the code that sets it.
    /// </summary>
    /// <param name="dueIn">The timer's due time relative to the current clock.</param>
    /// <returns>A task that completes once the timer exists.</returns>
    public async Task WaitForTimerAsync(TimeSpan dueIn)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!HasTimerDueIn(dueIn))
            await Task.Delay(TimeSpan.FromMilliseconds(1), deadline.Token).ConfigureAwait(false);
    }

    bool HasTimerDueIn(TimeSpan dueIn)
    {
        lock (_sync)
            return _timers.Exists(timer => timer.DueAt == _elapsedTicks + dueIn.Ticks);
    }

    sealed class ManualTimer(ManualTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        long _period = Timeout.Infinite;

        // Guarded by the owner's lock.
        internal long? DueAt { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._sync)
            {
                if (!owner._timers.Contains(this))
                    return false;

                _period = period == Timeout.InfiniteTimeSpan ? Timeout.Infinite : period.Ticks;
                DueAt = dueTime == Timeout.InfiniteTimeSpan ? null : owner._elapsedTicks + dueTime.Ticks;
                return true;
            }
        }

        // Called under the owner's lock when the timer falls due.
        internal void Reschedule() => DueAt = _period > 0 ? DueAt + _period : null;

        internal void Fire() => callback(state);

        public void Dispose()
        {
            lock (owner._sync)
            {
                DueAt = null;
                owner._timers.Remove(this);
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
