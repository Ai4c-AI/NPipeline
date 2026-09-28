using System.Runtime.CompilerServices;
using System.Text;
using NPipeline.Connectors.Files;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.Tests.Files;

public sealed record LineSourceOptions : FileSourceOptions;

public sealed record LineSinkOptions : FileSinkOptions;

/// <summary>A minimal text format: one record per line. A line reading "bad" fails to map.</summary>
public sealed class LineSource(LineSourceOptions options) : FileSourceNode<string>(options)
{
    public bool NeedsSeekableStream { get; init; }

    public bool Compressible { get; init; } = true;

    public List<bool> SeekableStreamsSeen { get; } = [];

    protected override string ConnectorName => "lines";

    protected override IReadOnlyList<string> DirectoryFileExtensions => [".txt"];

    protected override bool RequiresSeekableStream => NeedsSeekableStream;

    protected override bool SupportsCompression => Compressible;

    protected override async IAsyncEnumerable<string> ReadAsync(Stream stream, FileReadContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        SeekableStreamsSeen.Add(stream.CanSeek);
        using var reader = new StreamReader(stream, Encoding.UTF8, false, context.BufferSize, true);
        long recordNumber = 0;

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            recordNumber++;

            if (line == "bad")
            {
                await context.HandleRowErrorAsync(recordNumber, new FormatException("bad line"), line, cancellationToken);
                continue;
            }

            yield return line;
        }
    }
}

/// <summary>Writes one line per record. Throws on "explode", to test cleanup.</summary>
public sealed class LineSink(LineSinkOptions options) : FileSinkNode<string?>(options)
{
    public bool NeedsSeekableStream { get; init; }

    public bool Compressible { get; init; } = true;

    public bool CanWriteNulls { get; init; }

    public bool? SawSeekableStream { get; private set; }

    protected override string ConnectorName => "lines";

    protected override bool RequiresSeekableStream => NeedsSeekableStream;

    protected override bool SupportsCompression => Compressible;

    protected override bool SupportsNullItems => CanWriteNulls;

    protected override async Task WriteAsync(Stream stream, IAsyncEnumerable<string?> items, FileWriteContext context, CancellationToken cancellationToken)
    {
        SawSeekableStream = stream.CanSeek && stream.CanRead;
        var writer = new StreamWriter(stream, new UTF8Encoding(false), context.BufferSize, true);

        await using (writer)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                if (item == "explode")
                    throw new InvalidOperationException("boom");

                await writer.WriteLineAsync(item ?? "<null>");
            }
        }
    }
}

/// <summary>An in-memory provider that can move objects (like the file system) and records every URI it opens.</summary>
public sealed class MoveableProvider(InMemoryStorageProvider inner) : IStorageProvider, IMoveableStorageProvider, IDeletableStorageProvider
{
    public List<StorageUri> Reads { get; } = [];

    public List<(StorageUri From, StorageUri To)> Moves { get; } = [];

    public StorageScheme Scheme => inner.Scheme;

    public bool CanHandle(StorageUri uri) => inner.CanHandle(uri);

    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        Reads.Add(uri);
        return inner.OpenReadAsync(uri, cancellationToken);
    }

    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) => inner.OpenWriteAsync(uri, cancellationToken);

    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) => inner.ExistsAsync(uri, cancellationToken);

    public IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken cancellationToken = default) =>
        inner.ListAsync(prefix, recursive, cancellationToken);

    public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default) => inner.DeleteAsync(uri, cancellationToken);

    public async Task MoveAsync(StorageUri sourceUri, StorageUri destinationUri, CancellationToken cancellationToken = default)
    {
        Moves.Add((sourceUri, destinationUri));
        inner.Put(destinationUri, inner.Get(sourceUri));
        await inner.DeleteAsync(sourceUri, cancellationToken);
    }
}
