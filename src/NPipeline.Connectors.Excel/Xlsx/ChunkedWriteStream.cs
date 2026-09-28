using System.Buffers;

namespace NPipeline.Connectors.Excel.Xlsx;

/// <summary>
///     A write-only stream that collects what the zip writer writes, for the sink to hand to storage asynchronously in
///     chunks. <see cref="System.IO.Compression.ZipArchive" /> writes synchronously, and synchronous writes to a network
///     stream would block a thread per upload.
/// </summary>
internal sealed class ChunkedWriteStream : Stream
{
    private readonly ArrayBufferWriter<byte> _buffer = new(128 * 1024);

    /// <summary>The bytes collected since the last drain.</summary>
    public int Pending => _buffer.WrittenCount;

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <summary>Writes the collected bytes to <paramref name="target" /> and starts collecting again.</summary>
    public async ValueTask DrainAsync(Stream target, CancellationToken cancellationToken)
    {
        if (_buffer.WrittenCount == 0)
            return;

        await target.WriteAsync(_buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        _buffer.ResetWrittenCount();
    }

    public override void Write(byte[] buffer, int offset, int count) => _buffer.Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer) => _buffer.Write(buffer);

    public override void WriteByte(byte value) => _buffer.Write([value]);

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();
}
