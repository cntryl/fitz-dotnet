# fitz-dotnet

.NET client SDK for Fitz.

## Repository layout

- src/Core/Core.csproj: core SDK package and client implementation.
- src/Abstractions/Abstractions.csproj: shared interfaces/contracts.
- src/DependencyInjection/DependencyInjection.csproj: DI registration extensions.
- tests/Core/Core.Tests.csproj: single test project with two directories:
	- tests/Core/Unit
	- tests/Core/Integration

## Commands

- Build: `dotnet build Fitz.sln`
- Test: `dotnet test tests/Core/Core.Tests.csproj`
- List solution projects: `dotnet sln Fitz.sln list`
