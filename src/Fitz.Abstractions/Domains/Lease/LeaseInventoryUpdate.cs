namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// One steady-state change applied to a <see cref="ILeaseInventoryObserver"/>'s view.
/// </summary>
/// <param name="Route">The lease route that changed.</param>
/// <param name="Item">
/// The current entry for <paramref name="Route"/>, or <see langword="null"/> when the route is no
/// longer held and was removed from the view.
/// </param>
public sealed record LeaseInventoryUpdate(string Route, LeaseListItem? Item);
