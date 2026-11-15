# events

Events and listeners for Python applications, with optional queued delivery through [jobs](https://github.com/sourceant/jobs).

The core has no runtime dependencies. It does not depend on an application, framework, database, or ownership model.

## Install

```bash
pip install 'sourceant-events @ git+https://github.com/sourceant/events.git@feat/event-primitives'
```

Use an immutable commit or release tag when pinning an application dependency. Install the `jobs` extra for queued delivery.

## Events and listeners

```python
from sourceant_events import Event, EventBus

bus = EventBus()
subscription = bus.listen("render.finished", lambda event: print(event.data["film"]))
bus.dispatch(Event("render.finished", data={"film": "demo"}))
bus.unsubscribe(subscription)
```

An event carries a unique ID, type, UTC occurrence time, data, optional metadata, and envelope version. There is no required workspace or review. `Event.to_dict()` creates a detached JSON-compatible envelope; `Event.from_dict()` restores its identity and timestamp. Local listeners can receive live Python objects, but those objects must be converted to JSON values before delivery.

A listener receives the event. Smaller priorities run first, and equal priorities follow registration order. Subscribe to `"*"` to receive every type. A dispatch uses a snapshot: subscribing or unsubscribing during a callback affects the next dispatch. The bus retains listeners until they are unsubscribed; separate bus instances do not share subscriptions.

`dispatch()` propagates listener errors and rejects coroutine listeners before running any callback. `await bus.dispatch_async(event)` accepts synchronous and asynchronous listeners and waits for each one in order. It does not schedule callbacks to run after dispatch returns. Both methods return `ListenerResult` entries containing the subscription and callback result.

For best-effort delivery, pass a synchronous `on_error(subscription, exception)` callback. Its return value becomes that listener's result and dispatch continues. Without it, an error stops dispatch. Cancellation and other `BaseException` subclasses always propagate.

## Caller context

A listener can opt into receiving runtime context without adding it to the event payload:

```python
bus.listen("render.finished", lambda event, context: context.save(event), with_context=True)
bus.dispatch(event, context=storage)
```

Ordinary listeners still receive only the event. Context is not serialized or included in outgoing requests.

## Queued delivery

```python
from jobs import BACKGROUND, Contributions, JobHandler, JobRequest, SQLJobStore, Worker
from sourceant_events import Event, EventBus
from sourceant_events.jobs import EventHandler, QueuedListener

store = SQLJobStore(engine, create_schema=True)
publisher = EventBus()
publisher.listen(
    "render.finished",
    QueuedListener(
        store.enqueue,
        lambda event: JobRequest(BACKGROUND, "events.deliver", max_attempts=12),
    ),
    with_context=True,
)

with engine.begin() as connection:
    publisher.dispatch(Event("render.finished", {"film": "demo"}), context=connection)

consumer = EventBus()
consumer.listen("render.finished", lambda event: print(event.data["film"]))
handlers = Contributions()
handlers.contribute(JobHandler, EventHandler("events.deliver", consumer.dispatch))
Worker(store, BACKGROUND, services=handlers).work()
```

`QueuedListener` replaces the request's payload with the serialized event. The request factory controls the queue lane, tenant, attempts, deadline, and other job settings. Context is passed as the enqueue operation's `session` argument, so queued work commits or rolls back with the caller's SQLAlchemy session or connection.

`EventHandler` restores the event and calls a synchronous listener. Invalid envelopes fail without retry; listener failures request a retry without persisting the exception text. Asynchronous listeners must be run through an explicitly awaited application adapter.

A transaction cannot undo an HTTP request or another external side effect. Queue those effects inside the transaction, and perform them in the worker after commit. Delivery can repeat, so consumers must use the event ID to prevent duplicate effects.

## Signed HTTP

```python
from sourceant_events.http import SignedHTTP

handler = EventHandler("events.deliver", SignedHTTP(destination, signing_secret))
```

The sender signs the exact JSON body using HMAC-SHA256 over `timestamp + "." + body`. It sends `X-Events-Timestamp` and `X-Events-Signature`. The receiver must verify the signature, enforce an acceptable timestamp age, and deduplicate event IDs. HTTP errors and timeouts raise `DeliveryError`; the jobs adapter requests a retry.

Header names, timeout, and the event-to-payload encoder are configurable for an existing receiver. No environment variables or destination conventions are built into the package.

## Development

```bash
python -m pip install -e '.[dev,jobs]'
python -m pytest
python -m black --check src tests
python -m build
```
