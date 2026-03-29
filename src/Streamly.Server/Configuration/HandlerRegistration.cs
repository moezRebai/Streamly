namespace Streamly.Server.Configuration;

internal record HandlerRegistration(
    Type    RequestType,
    Type    ResponseType,
    Type    HandlerType,
    string  StreamName,
    Type?   DiffComputerType = null);
