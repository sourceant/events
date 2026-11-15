import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pytest
from jobs import BACKGROUND, Contributions, JobHandler, JobRequest, SQLJobStore, Worker
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, select

from sourceant_events import Event, EventBus
from sourceant_events.http import SignedHTTP
from sourceant_events.jobs import EventHandler, QueuedListener


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'events.sqlite'}")
    clock = [datetime.now(timezone.utc)]
    queue = SQLJobStore(engine, create_schema=True, now=lambda: clock[0])
    yield queue, engine, clock
    engine.dispose()


@pytest.fixture
def receiver():
    received = []
    state = {"status": 503}

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            timestamp = self.headers["X-Events-Timestamp"]
            signature = hmac.new(
                b"test-signing-secret", timestamp.encode() + b"." + body, hashlib.sha256
            ).hexdigest()
            assert hmac.compare_digest(signature, self.headers["X-Events-Signature"])
            received.append(json.loads(body))
            self.send_response(state["status"])
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Receiver)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/events", received, state
    server.shutdown()
    server.server_close()
    thread.join()


def test_signed_http_delivery_retries_the_same_event_through_the_real_worker(
    store, receiver
):
    queue, engine, clock = store
    url, received, state = receiver
    sender = EventBus()
    queued = QueuedListener(
        queue.enqueue,
        lambda event: JobRequest(BACKGROUND, "events.deliver", max_attempts=2),
    )
    sender.listen("render.finished", queued, with_context=True)
    event = Event("render.finished", {"film": "demo"})
    identifier = sender.dispatch(event)[0].value
    services = Contributions()
    services.contribute(
        JobHandler,
        EventHandler("events.deliver", SignedHTTP(url, "test-signing-secret")),
    )
    worker = Worker(queue, BACKGROUND, name="test-events", services=services)
    worker.work(max_jobs=1)
    assert queue.read(identifier).state == "queued"
    assert queue.read(identifier).error == "Event listener failed"
    state["status"] = 200
    clock[0] += timedelta(seconds=10)
    worker.work(max_jobs=1)
    assert queue.read(identifier).state == "succeeded"
    assert [entry["id"] for entry in received] == [event.id, event.id]
    assert received[1]["data"] == {"film": "demo"}


def test_business_state_and_queued_event_share_the_callers_transaction(store):
    queue, engine, clock = store
    state = Table("business_state", MetaData(), Column("id", Integer, primary_key=True))
    state.create(engine)
    bus = EventBus()
    bus.listen(
        "render.finished",
        QueuedListener(
            queue.enqueue, lambda event: JobRequest(BACKGROUND, "events.deliver")
        ),
        with_context=True,
    )
    with pytest.raises(RuntimeError, match="Rollback"):
        with engine.begin() as connection:
            connection.execute(insert(state).values(id=1))
            identifier = bus.dispatch(Event("render.finished"), context=connection)[
                0
            ].value
            raise RuntimeError("Rollback")
    assert queue.read(identifier) is None
    with engine.connect() as connection:
        assert connection.execute(select(state)).all() == []
    with engine.begin() as connection:
        connection.execute(insert(state).values(id=2))
        kept = bus.dispatch(Event("render.finished"), context=connection)[0].value
    assert queue.read(kept).state == "queued"


def test_queued_events_can_dispatch_to_an_independent_applications_listeners(store):
    queue, engine, clock = store
    consumer = EventBus()
    seen = []
    consumer.listen("review.failed", lambda event: seen.append(event.id))
    publisher = QueuedListener(
        queue.enqueue, lambda event: JobRequest(BACKGROUND, "events.local")
    )
    event = Event("review.failed", {"review": "review-1"})
    identifier = publisher(event)
    services = Contributions()
    services.contribute(JobHandler, EventHandler("events.local", consumer.dispatch))
    Worker(queue, BACKGROUND, name="test-local", services=services).work(max_jobs=1)
    assert seen == [event.id]
    assert queue.read(identifier).state == "succeeded"


def test_invalid_persisted_events_are_not_retried(store):
    queue, engine, clock = store
    event = Event("render.finished").to_dict()
    event["version"] = 2
    identifier = queue.enqueue(
        JobRequest(BACKGROUND, "events.deliver", payload=event, max_attempts=3)
    )
    services = Contributions()
    services.contribute(JobHandler, EventHandler("events.deliver", lambda event: None))
    Worker(queue, BACKGROUND, name="test-invalid", services=services).work(max_jobs=1)
    assert queue.read(identifier).state == "failed"
    assert queue.read(identifier).error == "Invalid event envelope"


def test_coroutine_listeners_cannot_be_registered_as_synchronous_job_handlers():
    async def listener(event):
        return event.id

    with pytest.raises(TypeError, match="synchronous listener"):
        EventHandler("events.deliver", listener)


def test_awaitables_cannot_be_reported_as_successful_delivery(store):
    queue, engine, clock = store
    delivered = []

    async def listener(event):
        delivered.append(event.id)

    queued = QueuedListener(
        queue.enqueue, lambda event: JobRequest(BACKGROUND, "events.deliver")
    )
    identifier = queued(Event("render.finished"))
    services = Contributions()
    services.contribute(
        JobHandler, EventHandler("events.deliver", lambda event: listener(event))
    )
    Worker(queue, BACKGROUND, name="test-awaitable", services=services).work(max_jobs=1)
    assert delivered == []
    assert queue.read(identifier).error == "Queued listeners must not return awaitables"
    assert queue.read(identifier).state == "dead"
