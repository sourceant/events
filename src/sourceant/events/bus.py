from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Callable
from uuid import uuid4

from .event import Event


@dataclass(frozen=True)
class Subscription:
    type: str
    id: str = field(default_factory=lambda: str(uuid4()))


@dataclass(frozen=True)
class ListenerResult:
    subscription: Subscription
    value: Any


@dataclass
class _Listener:
    subscription: Subscription
    callback: Callable
    priority: int
    with_context: bool


class EventBus:
    def __init__(self):
        self._listeners: list[_Listener] = []
        self._lock = RLock()

    def listen(
        self, type: str, callback: Callable, *, priority: int = 0, with_context=False
    ) -> Subscription:
        if not isinstance(type, str) or not type.strip():
            raise ValueError("A subscription requires an event type")
        if not callable(callback):
            raise TypeError("An event listener must be callable")
        subscription = Subscription(type)
        with self._lock:
            self._listeners.append(
                _Listener(subscription, callback, priority, with_context)
            )
        return subscription

    def unsubscribe(self, subscription: Subscription) -> bool:
        with self._lock:
            before = len(self._listeners)
            self._listeners = [
                item for item in self._listeners if item.subscription != subscription
            ]
            return len(self._listeners) != before

    def _matching(self, event):
        with self._lock:
            return sorted(
                (
                    item
                    for item in self._listeners
                    if item.subscription.type in (event.type, "*")
                ),
                key=lambda item: item.priority,
            )

    @staticmethod
    def _call(listener, event, context):
        if listener.with_context:
            return listener.callback(event, context)
        return listener.callback(event)

    def dispatch(self, event: Event, *, context=None, on_error=None):
        listeners = self._matching(event)
        if any(
            inspect.iscoroutinefunction(item.callback)
            or inspect.iscoroutinefunction(getattr(item.callback, "__call__", None))
            for item in listeners
        ):
            raise TypeError("Synchronous event dispatch requires synchronous listeners")
        results = []
        for listener in listeners:
            try:
                value = self._call(listener, event, context)
                if inspect.isawaitable(value):
                    if inspect.iscoroutine(value):
                        value.close()
                    raise TypeError("Synchronous listeners must not return awaitables")
            except Exception as error:
                if on_error is None:
                    raise
                value = on_error(listener.subscription, error)
            results.append(ListenerResult(listener.subscription, value))
        return results

    async def dispatch_async(self, event: Event, *, context=None, on_error=None):
        results = []
        for listener in self._matching(event):
            try:
                value = self._call(listener, event, context)
                if inspect.isawaitable(value):
                    value = await value
            except Exception as error:
                if on_error is None:
                    raise
                value = on_error(listener.subscription, error)
            results.append(ListenerResult(listener.subscription, value))
        return results
