using System.Buffers;
using System.Text.Json;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Finds the records in a JSON stream without parsing them: the elements of a root array, the elements of an array at
///     a path inside a root object, or a sequence of top-level values (NDJSON). Each record is exposed as its UTF-8
///     bytes, so it can be deserialized directly, and a record that fails to deserialize does not stop the stream.
/// </summary>
/// <remarks>
///     <see cref="Utf8JsonReader" /> is a ref struct, so it cannot live across awaits. Each step creates a reader over the
///     buffered bytes from the saved <see cref="JsonReaderState" />; a step that runs out of data commits nothing, the
///     buffer is refilled (and grown when a record is larger than it), and the step is retried.
/// </remarks>
internal sealed class JsonValueScanner : IDisposable
{
    private readonly JsonReaderOptions _options;
    private readonly Stream _stream;
    private byte[] _buffer;
    private int _end;
    private bool _eof;
    private int _start;
    private JsonReaderState _state;
    private int _valueLength;
    private int _valueStart;

    public JsonValueScanner(Stream stream, int bufferSize, JsonReaderOptions options)
    {
        _stream = stream;
        _options = options;
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(bufferSize, 4096));
        _state = new JsonReaderState(options);
    }

    private enum Step
    {
        Token,
        Value,
    }

    /// <summary>The current record's UTF-8 bytes, valid until the next call on the scanner.</summary>
    public ReadOnlyMemory<byte> Current => _buffer.AsMemory(_valueStart, _valueLength);

    /// <summary>Whether records are top-level values rather than array elements.</summary>
    public bool IsSequence { get; private set; }

    /// <summary>Positions the scanner before the first record. Returns <c>false</c> for an empty file.</summary>
    /// <exception cref="JsonException">The file does not have the shape the options describe.</exception>
    public async ValueTask<bool> StartAsync(JsonFormat format, IReadOnlyList<string>? itemsPath, CancellationToken cancellationToken)
    {
        var first = await PeekAsync(cancellationToken).ConfigureAwait(false);

        if (first is null)
            return false;

        if (itemsPath is not null)
        {
            await FindArrayAsync(itemsPath, cancellationToken).ConfigureAwait(false);
            return true;
        }

        var isArray = format switch
        {
            JsonFormat.Array => true,
            JsonFormat.NewlineDelimited => false,
            _ => first == (byte)'[',
        };

        if (!isArray)
        {
            IsSequence = true;
            _state = new JsonReaderState(_options with { AllowMultipleValues = true });
            return true;
        }

        var (token, _) = await NextTokenAsync(cancellationToken).ConfigureAwait(false);

        if (token != JsonTokenType.StartArray)
            throw new JsonException($"Expected a JSON array, but the file starts with {Describe(token)}.");

        return true;
    }

    /// <summary>Moves to the next record. Returns <c>false</c> after the last one.</summary>
    /// <exception cref="JsonException">The JSON is malformed.</exception>
    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryStep(Step.Value, out var token, out _))
                return token is not (JsonTokenType.None or JsonTokenType.EndArray);

            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     After malformed JSON in a sequence, skips to the start of the next line so the read can go on. Returns the text
    ///     that was skipped, for the error report.
    /// </summary>
    public async ValueTask<string> SkipLineAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            // Past the previous record's line ending first, so the bad line itself is skipped.
            var pending = _buffer.AsSpan(_start, _end - _start);
            var content = pending.IndexOfAnyExcept(" \t\r\n"u8);

            if (content < 0 && !_eof)
            {
                _start = _end;
                await FillAsync(cancellationToken).ConfigureAwait(false);
                continue;
            }

            _start += content < 0 ? pending.Length : content;
            var newline = _buffer.AsSpan(_start, _end - _start).IndexOf((byte)'\n');

            if (newline >= 0 || _eof)
            {
                var length = newline >= 0 ? newline + 1 : _end - _start;
                var skipped = System.Text.Encoding.UTF8.GetString(_buffer, _start, Math.Min(length, 4096));
                _start += length;
                _state = new JsonReaderState(_options with { AllowMultipleValues = true });
                return skipped;
            }

            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);

    private async ValueTask FindArrayAsync(IReadOnlyList<string> path, CancellationToken cancellationToken)
    {
        var (token, _) = await NextTokenAsync(cancellationToken).ConfigureAwait(false);
        var where = "the root";

        if (token != JsonTokenType.StartObject)
            throw new JsonException($"ItemsPath '{string.Join('.', path)}' needs a root object, but the file starts with {Describe(token)}.");

        for (var i = 0; i < path.Count; i++)
        {
            var seen = new List<string>();

            while (true)
            {
                (token, var name) = await NextTokenAsync(cancellationToken).ConfigureAwait(false);

                if (token == JsonTokenType.EndObject)
                {
                    throw new JsonException(
                        $"ItemsPath '{string.Join('.', path)}': {where} has no property '{path[i]}'. Properties: {(seen.Count == 0 ? "none" : string.Join(", ", seen))}.");
                }

                if (!string.Equals(name, path[i], StringComparison.OrdinalIgnoreCase))
                {
                    if (seen.Count < 20)
                        seen.Add(name!);

                    // Skip the property's value, however large.
                    _ = await MoveNextAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var last = i == path.Count - 1;
                (token, _) = await NextTokenAsync(cancellationToken).ConfigureAwait(false);
                var expected = last ? JsonTokenType.StartArray : JsonTokenType.StartObject;

                if (token != expected)
                    throw new JsonException($"ItemsPath '{string.Join('.', path)}': '{name}' is {Describe(token)}, not {Describe(expected)}.");

                where = $"'{name}'";
                break;
            }
        }
    }

    private async ValueTask<(JsonTokenType Token, string? PropertyName)> NextTokenAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryStep(Step.Token, out var token, out var name))
                return (token, name);

            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Returns the first byte that is not whitespace or a byte order mark, or <c>null</c> for an empty stream.</summary>
    private async ValueTask<byte?> PeekAsync(CancellationToken cancellationToken)
    {
        while (_end - _start < 3 && !_eof)
        {
            await FillAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_buffer.AsSpan(_start, _end - _start).StartsWith("﻿"u8))
            _start += 3;

        while (true)
        {
            var span = _buffer.AsSpan(_start, _end - _start);
            var index = span.IndexOfAnyExcept(" \t\r\n"u8);

            if (index >= 0)
                return span[index];

            if (_eof)
                return null;

            await FillAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Runs one step on the buffered bytes. Returns <c>false</c>, committing nothing, when the step needs more data.</summary>
    private bool TryStep(Step step, out JsonTokenType token, out string? propertyName)
    {
        var reader = new Utf8JsonReader(_buffer.AsSpan(_start, _end - _start), _eof, _state);
        propertyName = null;

        if (!reader.Read())
        {
            token = JsonTokenType.None;

            if (!_eof)
                return false;

            Commit(ref reader);
            return true;
        }

        token = reader.TokenType;

        if (step == Step.Token)
        {
            if (token == JsonTokenType.PropertyName)
                propertyName = reader.GetString();
        }
        else if (token is not JsonTokenType.EndArray)
        {
            var tokenStart = (int)reader.TokenStartIndex;

            if (token is JsonTokenType.StartObject or JsonTokenType.StartArray && !reader.TrySkip())
                return false;

            _valueStart = _start + tokenStart;
            _valueLength = (int)reader.BytesConsumed - tokenStart;
        }

        Commit(ref reader);
        return true;
    }

    private void Commit(ref Utf8JsonReader reader)
    {
        _start += (int)reader.BytesConsumed;
        _state = reader.CurrentState;
    }

    private async ValueTask FillAsync(CancellationToken cancellationToken)
    {
        if (_eof)
            throw new JsonException("The JSON ends unexpectedly.");

        if (_start > 0)
        {
            Buffer.BlockCopy(_buffer, _start, _buffer, 0, _end - _start);
            _end -= _start;
            _start = 0;
        }

        // A record larger than the buffer: grow, so the retried step can see all of it.
        if (_end == _buffer.Length)
        {
            var larger = ArrayPool<byte>.Shared.Rent(_buffer.Length * 2);
            Buffer.BlockCopy(_buffer, 0, larger, 0, _end);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = larger;
        }

        var read = await _stream.ReadAsync(_buffer.AsMemory(_end), cancellationToken).ConfigureAwait(false);

        if (read == 0)
            _eof = true;
        else
            _end += read;
    }

    private static string Describe(JsonTokenType token) =>
        token switch
        {
            JsonTokenType.StartObject => "an object",
            JsonTokenType.StartArray => "an array",
            JsonTokenType.String => "a string",
            JsonTokenType.Number => "a number",
            JsonTokenType.True or JsonTokenType.False => "a boolean",
            JsonTokenType.Null => "null",
            JsonTokenType.None => "nothing",
            _ => token.ToString(),
        };
}
