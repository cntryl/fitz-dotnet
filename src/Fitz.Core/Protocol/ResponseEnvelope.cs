namespace Cntryl.Fitz.Protocol;

/// <summary>Everything a domain exception carries about a failed broker response.</summary>
/// <param name="Message">Description of the failure.</param>
/// <param name="Code">Symbolic error code.</param>
/// <param name="Status">Wire status byte, when the response carried one.</param>
/// <param name="DomainCode">Numeric domain code from an error envelope.</param>
/// <param name="ErrorClass">Error class from an error envelope.</param>
/// <param name="ErrorEffect">Error effect from an error envelope.</param>
readonly record struct WireFailure(
    string Message,
    string Code,
    byte? Status = null,
    uint? DomainCode = null,
    byte? ErrorClass = null,
    byte? ErrorEffect = null);

/// <summary>Creates a domain's exception type for a failed broker response.</summary>
/// <typeparam name="TException">The domain exception type.</typeparam>
/// <param name="failure">The failure details.</param>
/// <returns>The exception to throw.</returns>
delegate TException WireFailureFactory<out TException>(WireFailure failure)
    where TException : FitzException;

/// <summary>
/// Reads the status-prefixed response envelope shared by every domain: status <c>0</c> followed by
/// the operation's payload, or status <c>1</c> followed by an error envelope.
/// </summary>
/// <remarks>
/// Codes are <c>{OPERATION}_FAILED</c> for a broker-reported error, and
/// <c>{OPERATION}_INVALID_RESPONSE</c> for an empty response, an unknown status, or a malformed error envelope.
/// </remarks>
static class ResponseEnvelope
{
    const byte StatusOk = 0;
    const byte StatusError = 1;

    /// <summary>Returns a reader positioned after the success status, or throws the domain error.</summary>
    /// <typeparam name="TException">The domain exception type.</typeparam>
    /// <param name="response">The full response payload.</param>
    /// <param name="operation">Operation name used in messages and codes.</param>
    /// <param name="create">Creates the domain exception.</param>
    /// <param name="mapCode">Optional mapping from a broker domain code to a symbolic code.</param>
    /// <returns>A reader over the success payload.</returns>
    internal static BinaryBufferReader ReadSuccess<TException>(
        ReadOnlyMemory<byte> response,
        string operation,
        WireFailureFactory<TException> create,
        Func<uint, string>? mapCode = null)
        where TException : FitzException
    {
        if (response.IsEmpty)
            throw create(new WireFailure($"{operation} response is empty", InvalidResponseCode(operation)));

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status == StatusOk)
            return reader;

        throw ReadFailure(response, operation, status, create, mapCode);
    }

    /// <summary>Builds the domain exception for a response whose status is not success.</summary>
    /// <typeparam name="TException">The domain exception type.</typeparam>
    /// <param name="response">The full response payload, including its status byte.</param>
    /// <param name="operation">Operation name used in messages and codes.</param>
    /// <param name="status">The response's status byte.</param>
    /// <param name="create">Creates the domain exception.</param>
    /// <param name="mapCode">Optional mapping from a broker domain code to a symbolic code.</param>
    /// <returns>The exception to throw.</returns>
    internal static TException ReadFailure<TException>(
        ReadOnlyMemory<byte> response,
        string operation,
        byte status,
        WireFailureFactory<TException> create,
        Func<uint, string>? mapCode = null)
        where TException : FitzException
    {
        if (status != StatusError)
            return create(new WireFailure($"{operation} response has unknown status {status}", InvalidResponseCode(operation), status));

        ResponseErrorEnvelope error;
        try
        {
            error = ResponseError.Read(response);
        }
        catch (ProtocolException exception)
        {
            return create(new WireFailure($"{operation} error response is malformed: {exception.Message}", InvalidResponseCode(operation), status));
        }

        return create(new WireFailure(
            $"{operation} failed: {error.Message}",
            mapCode?.Invoke(error.Code) ?? $"{operation}_FAILED",
            status,
            error.Code,
            error.Class,
            error.Effect));
    }

    static string InvalidResponseCode(string operation) => $"{operation}_INVALID_RESPONSE";
}
