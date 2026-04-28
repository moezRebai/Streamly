using System.Globalization;
using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;

namespace Streamly.Infrastructure.Serialization;

/// <summary>
/// MessagePack-based serializer — compact binary format, typically 3-5× faster
/// than JSON on the wire and significantly smaller payloads.
///
/// Uses ContractlessStandardResolverAllowPrivate so no [MessagePackObject] attributes
/// are required on your models, and internal transport types are handled correctly.
/// Custom formatters are included for decimal and DateOnly.
///
/// Register before AddStreamlyServer / AddStreamlyClient:
///   services.AddSingleton&lt;IMessageSerializer, MessagePackMessageSerializer&gt;();
/// </summary>
public sealed class MessagePackMessageSerializer : MessageSerializerBase
{
    private static readonly MessagePackSerializerOptions Options = BuildOptions();

    private static MessagePackSerializerOptions BuildOptions()
    {
        var resolver = CompositeResolver.Create(
            [
                DecimalFormatter.Instance,
                DateOnlyFormatter.Instance
            ],
            [ContractlessStandardResolverAllowPrivate.Instance]);

        return MessagePackSerializerOptions.Standard.WithResolver(resolver);
    }

    public override byte[] Serialize<T>(T obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        try
        {
            return MessagePackSerializer.Serialize(obj, Options);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"[MessagePack] Serialize<{typeof(T).Name}> failed: {ex.Message}", ex);
        }
    }

    public override T Deserialize<T>(byte[] data)
    {
        if (data is null || data.Length == 0)
            throw new ArgumentException("Data cannot be null or empty", nameof(data));
        try
        {
            var result = MessagePackSerializer.Deserialize<T>(data, Options);
            if (result is null)
                throw new InvalidOperationException($"Deserialization returned null for {typeof(T).Name}");
            return result;
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException(
                $"[MessagePack] Deserialize<{typeof(T).Name}> failed: {ex.Message}", ex);
        }
    }

    public override T Deserialize<T>(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ArgumentException("Data cannot be empty", nameof(data));
        try
        {
            // MessagePack has no native Span overload — promote to array (one allocation)
            var result = MessagePackSerializer.Deserialize<T>(data.ToArray(), Options);
            if (result is null)
                throw new InvalidOperationException($"Deserialization returned null for {typeof(T).Name}");
            return result;
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException(
                $"[MessagePack] Deserialize<{typeof(T).Name}> (span) failed: {ex.Message}", ex);
        }
    }

    // ── Custom formatters for types not covered by ContractlessStandardResolver ──

    private sealed class DecimalFormatter : IMessagePackFormatter<decimal>
    {
        public static readonly DecimalFormatter Instance = new();

        public void Serialize(ref MessagePackWriter writer, decimal value, MessagePackSerializerOptions options)
            => writer.Write(value.ToString("G", CultureInfo.InvariantCulture));

        public decimal Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
            => reader.TryReadNil() ? 0m
                : decimal.Parse(reader.ReadString()!, CultureInfo.InvariantCulture);
    }

    private sealed class DateOnlyFormatter : IMessagePackFormatter<DateOnly>
    {
        public static readonly DateOnlyFormatter Instance = new();
        private const string Format = "yyyy-MM-dd";

        public void Serialize(ref MessagePackWriter writer, DateOnly value, MessagePackSerializerOptions options)
            => writer.Write(value.ToString(Format, CultureInfo.InvariantCulture));

        public DateOnly Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
            => reader.TryReadNil() ? DateOnly.MinValue
                : DateOnly.ParseExact(reader.ReadString()!, Format, CultureInfo.InvariantCulture);
    }

}
