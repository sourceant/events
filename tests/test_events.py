import asyncio
from datetime import datetime

import pytest

from sourceant.events import Event, EventBus


def test_events_are_independent_of_application_ownership_and_round_trip():
    event = Event("render.finished", {"film": "demo"})
    other = Event("review.failed", {"review": "review-1"})
    assert event.id != other.id
    assert event.occurred_at.tzinfo is not None
    assert event.metadata == {}
    assert Event.from_dict(event.to_dict()) == event


@pytest.mark.parametrize(
    "fields",
    [
        {"type": ""},
        {"type": "*"},
        {"version": 2},
        {"version": True},
        {"occurred_at": datetime(2026, 1, 1)},
        {"data": []},
        {"metadata": []},
    ],
)
def test_invalid_envelopes_are_rejected(fields):
    with pytest.raises(ValueError):
        Event(**({"type": "render.finished"} | fields))


def test_local_objects_can_be_dispatched_but_cannot_be_delivered_as_json():
    local = object()
    event = Event("client.connected", {"client": local})
    bus = EventBus()
    bus.listen(event.type, lambda received: received.data["client"])
    assert bus.dispatch(event)[0].value is local
    with pytest.raises(ValueError, match="JSON-serializable"):
        event.to_dict()


def test_serialized_payloads_are_detached_from_the_original_event():
    event = Event("render.finished", {"scenes": ["intro"]})
    wire = event.to_dict()
    wire["data"]["scenes"].append("outro")
    assert event.data["scenes"] == ["intro"]


def test_priorities_wildcards_context_and_result_identity():
    bus = EventBus()
    seen = []
    late = bus.listen("render.finished", lambda event: seen.append("late"), priority=10)
    first = bus.listen("*", lambda event: seen.append("first"), priority=-1)
    context = object()
    contextual = bus.listen(
        "render.finished", lambda event, current: current, with_context=True
    )
    result = bus.dispatch(Event("render.finished"), context=context)
    assert seen == ["first", "late"]
    assert [item.subscription for item in result] == [first, contextual, late]
    assert result[1].value is context


def test_subscription_changes_apply_to_the_next_dispatch_and_buses_are_isolated():
    bus = EventBus()
    seen = []
    bus.listen("render.finished", lambda event: bus.unsubscribe(later), priority=-1)
    later = bus.listen("render.finished", lambda event: seen.append(event.id))
    event = Event("render.finished")
    bus.dispatch(event)
    bus.dispatch(event)
    assert seen == [event.id]
    assert not bus.unsubscribe(later)
    assert EventBus().dispatch(event) == []


@pytest.mark.parametrize("callable_object", [False, True])
def test_sync_dispatch_rejects_async_listeners_before_any_side_effect(callable_object):
    bus = EventBus()
    seen = []
    bus.listen("render.finished", lambda event: seen.append(event))

    async def asynchronous(event):
        seen.append(event)

    class AsyncListener:
        async def __call__(self, event):
            await asynchronous(event)

    bus.listen("render.finished", AsyncListener() if callable_object else asynchronous)
    with pytest.raises(TypeError, match="synchronous listeners"):
        bus.dispatch(Event("render.finished"))
    assert seen == []


def test_async_dispatch_waits_for_each_listener_in_order():
    bus = EventBus()
    seen = []

    async def first(event):
        await asyncio.sleep(0)
        seen.append("first")
        return event.id

    bus.listen("render.finished", first)
    bus.listen("render.finished", lambda event: seen.append("second"))
    event = Event("render.finished")
    result = asyncio.run(bus.dispatch_async(event))
    assert seen == ["first", "second"]
    assert result[0].value == event.id


def test_errors_propagate_unless_the_caller_explicitly_handles_them():
    bus = EventBus()

    def failure(event):
        raise RuntimeError("Listener unavailable")

    failing = bus.listen("review.failed", failure)
    bus.listen("review.failed", lambda event: "kept going")
    with pytest.raises(RuntimeError, match="Listener unavailable"):
        bus.dispatch(Event("review.failed"))
    caught = []
    result = asyncio.run(
        bus.dispatch_async(
            Event("review.failed"),
            on_error=lambda subscription, error: caught.append(subscription),
        )
    )
    assert caught == [failing]
    assert result[1].value == "kept going"


def test_cancellation_is_not_swallowed_by_error_handling():
    bus = EventBus()

    async def cancel(event):
        raise asyncio.CancelledError()

    bus.listen("render.started", cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            bus.dispatch_async(Event("render.started"), on_error=lambda *args: None)
        )
