using System.Linq.Expressions;
using System.Reflection;
using Streamly.Core.Abstractions;

namespace Streamly.Core.ChangeDetection;

/// <summary>
/// Default implementation using expression trees
/// </summary>
public class DefaultResponseChangeDetector<TResponse> : IResponseChangeDetector<TResponse>
{
    private readonly PropertyComparer[] _comparers;
    private readonly HashSet<string> _alwaysPublishProperties;

    public DefaultResponseChangeDetector()
    {
        var properties = typeof(TResponse).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var comparers = new List<PropertyComparer>();
        _alwaysPublishProperties = new HashSet<string>();

        foreach (var prop in properties)
        {
            // Skip [IgnoreInComparison]
            if (prop.GetCustomAttribute<IgnoreInComparisonAttribute>() != null)
                continue;

            // Track [AlwaysPublish]
            if (prop.GetCustomAttribute<AlwaysPublishAttribute>() != null)
            {
                _alwaysPublishProperties.Add(prop.Name);
                continue; // Don't compare, just always include
            }

            // Build compiled comparer
            comparers.Add(new PropertyComparer(prop));
        }

        _comparers = comparers.ToArray();
    }

    public HashSet<string> GetChangedProperties(TResponse? lastResponse, TResponse newResponse)
    {
        var changed = new HashSet<string>();

        // First response - all properties changed
        if (lastResponse == null)
        {
            foreach (var comparer in _comparers)
                changed.Add(comparer.PropertyName);
            
            // Include AlwaysPublish properties
            foreach (var prop in _alwaysPublishProperties)
                changed.Add(prop);
            
            return changed;
        }

        // Compare properties
        foreach (var comparer in _comparers)
        {
            if (comparer.HasChanged(lastResponse, newResponse))
                changed.Add(comparer.PropertyName);
        }

        // If any property changed, include AlwaysPublish properties
        if (changed.Count > 0)
        {
            foreach (var prop in _alwaysPublishProperties)
                changed.Add(prop);
        }

        return changed;
    }

    private class PropertyComparer
    {
        public string PropertyName { get; }
        private readonly Func<TResponse, TResponse, bool> _hasChanged;

        public PropertyComparer(PropertyInfo property)
        {
            PropertyName = property.Name;
            _hasChanged = BuildComparer(property);
        }

        public bool HasChanged(TResponse last, TResponse current)
        {
            return _hasChanged(last, current);
        }

        private static Func<TResponse, TResponse, bool> BuildComparer(PropertyInfo prop)
        {
            var lastParam = Expression.Parameter(typeof(TResponse), "last");
            var currentParam = Expression.Parameter(typeof(TResponse), "current");

            var lastProp = Expression.Property(lastParam, prop);
            var currentProp = Expression.Property(currentParam, prop);

            Expression comparison;

            if (prop.PropertyType.IsValueType)
            {
                comparison = Expression.NotEqual(lastProp, currentProp);
            }
            else
            {
                var equalsMethod = typeof(object).GetMethod(nameof(Equals), new[] { typeof(object), typeof(object) })!;
                var equalsCall = Expression.Call(
                    equalsMethod,
                    Expression.Convert(lastProp, typeof(object)),
                    Expression.Convert(currentProp, typeof(object)));
                comparison = Expression.Not(equalsCall);
            }

            return Expression.Lambda<Func<TResponse, TResponse, bool>>(
                comparison,
                lastParam,
                currentParam).Compile();
        }
    }
}

[AttributeUsage(AttributeTargets.Property)]
public class IgnoreInComparisonAttribute : Attribute { }

/// <summary>
/// Include this property in response even if it hasn't changed
/// Only applied when OTHER properties have changed
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class AlwaysPublishAttribute : Attribute { }