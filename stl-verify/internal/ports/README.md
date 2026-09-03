# Ports Layer

Ports define the boundaries of the application through interfaces.

## Outbound Ports (Secondary)

Located in `outbound/` - These are interfaces that our application uses to interact
with the outside world. They are implemented by adapters.

Examples:
- `EventPublisher` - Event publishing interface
- `ExternalAPIClient` - External service interface
