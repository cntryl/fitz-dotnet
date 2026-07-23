using System.Diagnostics.CodeAnalysis;

// These enums mirror one-byte protocol fields; changing their underlying type would
// obscure the wire contract without improving the public API.
[assembly: SuppressMessage("Design", "CA1028:Enum Storage should be Int32", Justification = "The enum is a one-byte Fitz wire value.", Scope = "type", Target = "~T:Cntryl.Fitz.Abstractions.Domains.Kv.KvMode")]
[assembly: SuppressMessage("Design", "CA1028:Enum Storage should be Int32", Justification = "The enum is a one-byte Fitz wire value.", Scope = "type", Target = "~T:Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability")]
[assembly: SuppressMessage("Design", "CA1028:Enum Storage should be Int32", Justification = "The enum is a one-byte Fitz wire value.", Scope = "type", Target = "~T:Cntryl.Fitz.Abstractions.Domains.Schedule.ScheduleDeliveryMode")]

// These names and values are protocol vocabulary and cannot be changed independently.
[assembly: SuppressMessage("Naming", "CA1720:Identifier contains type name", Justification = "Single is the protocol-defined delivery mode name.", Scope = "member", Target = "~F:Cntryl.Fitz.Abstractions.Domains.Schedule.ScheduleDeliveryMode.Single")]
[assembly: SuppressMessage("Design", "CA1008:Enums should have zero value", Justification = "Zero is not a valid filtered reason in the Fitz protocol.", Scope = "type", Target = "~T:Cntryl.Fitz.Abstractions.Domains.Stream.StreamFilteredReason")]

// Binary payload arrays are intentionally exposed by this preview API. Replacing
// them is a breaking public-contract change and belongs in a dedicated release.
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "The public contract currently exposes an owned binary payload.", Scope = "type", Target = "~T:Cntryl.Fitz.Abstractions.Domains.Schedule.ScheduleEntry")]
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "The public contract currently exposes owned binary payloads.", Scope = "member", Target = "~P:Cntryl.Fitz.Abstractions.Domains.Stream.StreamRecord.Body")]
[assembly: SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "The public contract currently exposes owned binary payloads.", Scope = "member", Target = "~P:Cntryl.Fitz.Abstractions.Domains.Stream.StreamRecord.Metadata")]
