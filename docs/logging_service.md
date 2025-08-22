# Logging Service

The application now emits logs to a dedicated logging microservice.  The
service persists log events in a shared database and exposes an HTTP API for
ingestion and querying.

## Deployment

1. **Database** – provide a connection URL via `LOG_DB_URL`.  Any database
   supported by SQLAlchemy can be used.  For production a PostgreSQL instance
   is recommended, e.g.

   ```bash
   export LOG_DB_URL=postgresql://user:pass@db/logs
   ```

2. **Authentication** – set a shared bearer token with
   `LOGGING_SERVICE_TOKEN` to protect the endpoint.

3. **Run the service** – start the Flask application in its own process or
   container:

   ```bash
   python -m services.logging.server  # or use gunicorn/uwsgi
   ```

   The service listens on port `9090` by default.

## Application configuration

The main application publishes events to the logging service over HTTP.  Set
`LOGGING_SERVICE_URL` to the service base URL and `LOGGING_SERVICE_TOKEN` to the
same token configured on the server:

```bash
export LOGGING_SERVICE_URL=https://logs.example.com/logs
export LOGGING_SERVICE_TOKEN=supersecret
```

## API

- `POST /logs` – submit a log event.  Requires an `Authorization: Bearer`
  header when a token is configured.
- `GET /logs` – fetch recent log events (primarily for administrative use).

Both endpoints respond with `401 Unauthorized` if the bearer token is missing or
invalid.
