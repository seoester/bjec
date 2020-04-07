import datetime
from typing import cast, Iterable, List, Sequence, TypeVar, Union

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

min_datetime = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
"""Minimum representable datetime with timezone ("aware") set to UTC."""

max_datetime = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
"""Maximum representable datetime with timezone ("aware") set to UTC."""
