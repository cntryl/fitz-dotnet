namespace Cntryl.Fitz.Runtime;

/// <summary>
/// A live subscription before it is wrapped in a public handle: how to withdraw it, and the
/// signal that completes or faults with local delivery. Owns no resources.
/// </summary>
/// <param name="Unsubscribe">Withdraws the registration; safe to call more than once.</param>
/// <param name="Completion">Completes on unsubscribe and faults if delivery terminates.</param>
readonly record struct SubscriptionBinding(Func<CancellationToken, ValueTask> Unsubscribe, Task Completion);
