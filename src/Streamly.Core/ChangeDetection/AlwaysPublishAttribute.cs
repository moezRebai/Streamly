namespace Streamly.Core.ChangeDetection;

/// <summary>
/// Include this property in every delta even if it hasn't changed.
/// Only triggered when at least one other property has changed — does not
/// override the skip when nothing changed at all.
/// Typical use: identity fields (CurrencyPair, Tenor) that the subscriber
/// needs on every message to make sense of the data.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class AlwaysPublishAttribute : Attribute { }