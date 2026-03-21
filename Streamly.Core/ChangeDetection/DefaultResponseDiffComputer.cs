using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Streamly.Core.Abstractions;

namespace Streamly.Core.ChangeDetection;

/// <summary>
/// Default diff computer built on compiled expression trees.
/// All comparers and accessors are compiled once at startup — zero reflection on the hot path.
///
/// Comparison strategy per property type:
///   - Value types, string, DateTime, Guid, decimal… → compiled != expression (fastest)
///   - Collections (IEnumerable, IDictionary)        → reference equality, then JSON hash
///   - Nested complex objects                         → reference equality, then JSON hash
///
/// Update object granularity: top-level properties only.
/// A changed nested object or collection is copied whole — no recursive partial objects.
///
/// TResponse must have a public parameterless constructor so partial Update objects can be created.
/// </summary>
public class DefaultResponseDiffComputer<TResponse> : IResponseDiffComputer<TResponse> where TResponse : class
{
    private readonly PropertyEntry[]    _entries;
    private readonly HashSet<string>    _alwaysPublishProperties;

    public DefaultResponseDiffComputer()
    {
        if (typeof(TResponse).GetConstructor(Type.EmptyTypes) == null)
            throw new InvalidOperationException(
                $"'{typeof(TResponse).Name}' must have a public parameterless constructor " +
                $"to be used with DefaultResponseDiffComputer.");

        var properties = typeof(TResponse)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

        var entries              = new List<PropertyEntry>();
        _alwaysPublishProperties = [];

        foreach (var prop in properties)
        {
            if (prop.GetCustomAttribute<IgnoreInComparisonAttribute>() != null)
                continue;

            if (prop.GetCustomAttribute<AlwaysPublishAttribute>() != null)
            {
                _alwaysPublishProperties.Add(prop.Name);
                continue;
            }

            entries.Add(new PropertyEntry(prop));
        }

        _entries = entries.ToArray();
    }

    public ResponseDiff<TResponse> Compute(TResponse? currentImage, TResponse newResponse)
    {
        // First message — full snapshot, no diff needed
        if (currentImage == null)
            return new ResponseDiff<TResponse>(newResponse, newResponse, null);

        var changed = new HashSet<string>();

        foreach (var entry in _entries)
        {
            if (entry.HasChanged(currentImage, newResponse))
                changed.Add(entry.PropertyName);
        }

        if (changed.Count == 0)
            return new ResponseDiff<TResponse>(currentImage, null, null);

        // Include AlwaysPublish properties whenever anything changed
        foreach (var name in _alwaysPublishProperties)
            changed.Add(name);

        // Build partial Update — only changed properties are meaningful
        var update = Activator.CreateInstance<TResponse>()!;

        foreach (var entry in _entries)
        {
            if (changed.Contains(entry.PropertyName))
                entry.SetValue(update, entry.GetValue(newResponse));
        }

        // Copy AlwaysPublish properties (they sit outside _entries)
        foreach (var name in _alwaysPublishProperties)
        {
            var entry = Array.Find(_entries, e => e.PropertyName == name);
            entry?.SetValue(update, entry.GetValue(newResponse));
        }

        return new ResponseDiff<TResponse>(newResponse, update, changed);
    }

    public TResponse Merge(TResponse localImage, TResponse delta, IReadOnlySet<string> changedProperties)
    {
        foreach (var entry in _entries)
        {
            if (changedProperties.Contains(entry.PropertyName))
                entry.SetValue(localImage, entry.GetValue(delta));
        }

        // AlwaysPublish properties are included in changedProperties when relevant — handled above
        return localImage;
    }

    // ── Property entry ───────────────────────────────────────────────────────

    private sealed class PropertyEntry
    {
        public  string                          PropertyName { get; }
        private readonly Func<TResponse, TResponse, bool>  _hasChanged;
        private readonly Func<TResponse, object?>          _getValue;
        private readonly Action<TResponse, object?>        _setValue;

        public PropertyEntry(PropertyInfo prop)
        {
            PropertyName = prop.Name;
            _getValue    = BuildGetter(prop);
            _setValue    = BuildSetter(prop);
            _hasChanged  = IsSimpleType(prop.PropertyType)
                ? BuildSimpleComparison(prop)
                : BuildReferenceComparison(_getValue);
        }

        public bool    HasChanged(TResponse a, TResponse b) => _hasChanged(a, b);
        public object? GetValue(TResponse instance)         => _getValue(instance);
        public void    SetValue(TResponse instance, object? value) => _setValue(instance, value);

        // ── Compiled accessors ────────────────────────────────────────────────

        private static Func<TResponse, object?> BuildGetter(PropertyInfo prop)
        {
            var param    = Expression.Parameter(typeof(TResponse), "x");
            var property = Expression.Property(param, prop);
            var convert  = Expression.Convert(property, typeof(object));
            return Expression.Lambda<Func<TResponse, object?>>(convert, param).Compile();
        }

        private static Action<TResponse, object?> BuildSetter(PropertyInfo prop)
        {
            var instanceParam = Expression.Parameter(typeof(TResponse), "x");
            var valueParam    = Expression.Parameter(typeof(object), "v");
            var property      = Expression.Property(instanceParam, prop);
            var assign        = Expression.Assign(
                property,
                Expression.Convert(valueParam, prop.PropertyType));
            return Expression
                .Lambda<Action<TResponse, object?>>(assign, instanceParam, valueParam)
                .Compile();
        }

        // ── Comparison builders ───────────────────────────────────────────────

        private static Func<TResponse, TResponse, bool> BuildSimpleComparison(PropertyInfo prop)
        {
            var left  = Expression.Parameter(typeof(TResponse), "a");
            var right = Expression.Parameter(typeof(TResponse), "b");

            var leftProp  = Expression.Property(left, prop);
            var rightProp = Expression.Property(right, prop);

            Expression comparison;

            if (prop.PropertyType.IsValueType)
            {
                comparison = Expression.NotEqual(leftProp, rightProp);
            }
            else
            {
                // string and nullable value types
                var equals = typeof(object).GetMethod(
                    nameof(Equals), [typeof(object), typeof(object)])!;
                comparison = Expression.Not(Expression.Call(
                    equals,
                    Expression.Convert(leftProp, typeof(object)),
                    Expression.Convert(rightProp, typeof(object))));
            }

            return Expression.Lambda<Func<TResponse, TResponse, bool>>(
                comparison, left, right).Compile();
        }

        private static Func<TResponse, TResponse, bool> BuildReferenceComparison(
            Func<TResponse, object?> getter)
        {
            return (a, b) =>
            {
                var va = getter(a);
                var vb = getter(b);

                // Same reference (or both null) → not changed
                if (ReferenceEquals(va, vb)) return false;
                if (va == null || vb == null) return true;

                // Structural comparison via JSON — avoids requiring IEquatable on element types
                return JsonSerializer.Serialize(va) != JsonSerializer.Serialize(vb);
            };
        }

        // ── Type helpers ──────────────────────────────────────────────────────

        private static bool IsSimpleType(Type type)
        {
            if (type.IsValueType)  return true;
            if (type == typeof(string)) return true;

            var underlying = Nullable.GetUnderlyingType(type);
            return underlying != null && IsSimpleType(underlying);
        }
    }
}
