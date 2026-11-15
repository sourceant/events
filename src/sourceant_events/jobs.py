from __future__ import annotations

from dataclasses import replace
import inspect
from typing import Callable

from jobs import JobOutcome, JobRequest

from .event import Event


class QueuedListener:
    def __init__(self, enqueue: Callable, request: Callable[[Event], JobRequest]):
        self.enqueue = enqueue
        self.request = request

    def __call__(self, event: Event, context=None):
        request = replace(self.request(event), payload=event.to_dict())
        return self.enqueue(request, session=context)


class EventHandler:
    def __init__(self, type: str, listener: Callable[[Event], object]):
        if inspect.iscoroutinefunction(listener) or inspect.iscoroutinefunction(
            getattr(listener, "__call__", None)
        ):
            raise TypeError("Queued event handlers require a synchronous listener")
        self.type = type
        self.listener = listener

    def run(self, job) -> JobOutcome:
        try:
            event = Event.from_dict(job.payload)
            event.to_dict()
        except ValueError:
            return JobOutcome.failed("Invalid event envelope")
        try:
            result = self.listener(event)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                return JobOutcome.failed("Queued listeners must not return awaitables")
        except Exception:
            return JobOutcome.failed("Event listener failed", retry=True)
        return JobOutcome.ok()
