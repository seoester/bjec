import json
from typing import Any, cast, Dict, Iterable, List, Mapping, TextIO, Union

from .params import MappingResolvable, IterableResolvable, ParamsEvaluable, ParamSet, resolve, Resolvable
from .io import WriteOpenable, WriteableWrapFunc

_JSONValue = Union[Resolvable[Any], IterableResolvable[Any], MappingResolvable[str, Any]]

def _resolve_json(value: _JSONValue, params: ParamSet) -> Any:
    try:
        return cast('ParamsEvaluable[Any]', value).evaluate_with_params(params)
    except (AttributeError, TypeError):
        if isinstance(value, Mapping):
            return {
                resolve(key, params): _resolve_json(value, params) for key, value in value.items()
            }

        if isinstance(value, (str, bytes)):
            return value

        if isinstance(value, Iterable):
            return [_resolve_json(element, params) for element in value]

        return value

def _prepare_json_value(value: _JSONValue) -> _JSONValue:
    if isinstance(value, ParamsEvaluable):
        return value

    if isinstance(value, Mapping):
        return {
            key: _prepare_json_value(value) for key, value in value.items()
        }

    if isinstance(value, (str, bytes)):
        return value

    if isinstance(value, Iterable):
        return [_prepare_json_value(element) for element in value]

    return value


class Writeable(object):
    def __init__(self, value: _JSONValue) -> None:
        self._val: _JSONValue = _prepare_json_value(value)

    def evaluate_with_params(self, params: ParamSet) -> WriteableWrapFunc:
        def write_to(o: WriteOpenable) -> None:
            value = _resolve_json(self._val, params)

            with o.open_text() as f:
                json.dump(value, cast(TextIO, f))

        return WriteableWrapFunc(write_to)
