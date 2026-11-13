namespace Cntryl.Fitz;

/// <summary>
/// Signals that a frame write was cancelled part-way, leaving the stream unusable.
/// </summary>
/// <remarks>
/// The connection treats this as transport loss rather than as the caller's cancellation,
/// because a partially written frame desynchronizes the byte stream.
/// </remarks>
sealed class TransportSendCanceledException : OperationCanceledException
{
    const string DefaultMessage = "TCP frame write was canceled; the transport is no longer usable.";

    /// <summary>Initializes the exception with the default message.</summary>
    public TransportSendCanceledException()
        : base(DefaultMessage)
    {
    }

    /// <summary>Initializes the exception with a message.</summary>
    /// <param name="message">Description of the cancellation.</param>
    public TransportSendCanceledException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception with a message and underlying cause.</summary>
    /// <param name="message">Description of the cancellation.</param>
    /// <param name="innerException">The underlying cause.</param>
    public TransportSendCanceledException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes the exception from the cancellation that interrupted the write.</summary>
    /// <param name="innerException">The cancellation raised by the write.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    public TransportSendCanceledException(OperationCanceledException innerException, CancellationToken cancellationToken)
        : base(DefaultMessage, innerException, cancellationToken)
    {
    }
}
