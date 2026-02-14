namespace Streamly.Core.Runtime.Configuration;

internal record HandlerRegistration(
    Type RequestType,
    Type ResponseType,
    Type HandlerType,
    string StreamName);