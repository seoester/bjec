import collections
import datetime
import functools
import itertools
from types import TracebackType
from typing import Any, Callable, cast, Iterable, Iterator, List, Optional, Sequence, Type, TypeVar, Union
from typing_extensions import Protocol

_T = TypeVar('_T')

def listify(obj: Union[_T, Sequence[_T], None], none_empty: bool=False) -> List[_T]:
    """Turns ``obj`` into a list. Returns ``[obj]`` if it.

    Returns:
        ``obj`` is simply returned, if it already is a list.
        Otherwise - or if it a string - it is wrapped in a list.
        If ``none_empty`` is set to ``True``, an empty list is returned, if
        ``obj`` is ``None``.
    """

    if obj is None and none_empty:
        return []
    elif isinstance(obj, list):
        return obj
    elif isinstance(obj, (str, bytes)):
        return [cast(_T, obj)]
    elif isinstance(obj, Sequence):
        return list(obj)
    else:
        return [cast(_T, obj)]

def consume(it: Iterable[Any], n: Optional[int]=None) -> None:
    """Advance the iterable it n-steps ahead. If n is None, consume entirely.

    Copied from:
    https://docs.python.org/3.7/library/itertools.html#itertools-recipes
    """
    if n is None:
        collections.deque(it, maxlen=0)
    else:
        next(itertools.islice(it, n, n), None)

min_datetime = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
"""Minimum representable datetime with timezone ("aware") set to UTC."""

max_datetime = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
"""Maximum representable datetime with timezone ("aware") set to UTC."""


class HandlersCollector(Protocol):
    def callback(
        self,
        callback: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., Any]: ...


class HandlersList(object):
    def __init__(self) -> None:
        self._handlers: List[Callable[[], None]] = []

    def callback(
        self,
        callback: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., Any]:
        self._handlers.append(functools.partial(callback, *args, **kwargs))
        return callback

    def __iadd__(self, other: Iterable[Callable[[], None]]) -> 'HandlersList':
        self._handlers += other
        return self

    def clear(self) -> None:
        self._handlers.clear()

    def __iter__(self) -> Iterator[Callable[[], None]]:
        return iter(self._handlers)

    def __call__(self) -> None:
        for handler in self._handlers:
            handler()


class CallbackOnException(object):
    """Context manager calling a function on exit only if an exception occurred.
    """

    def __init__(self, f: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        self._callback: Callable[[], None] = functools.partial(f, *args, **kwargs)

    def __enter__(self) -> 'CallbackOnException':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        if exc_type is not None:
            self._callback()
        return None
