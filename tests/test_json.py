from io import BufferedIOBase, BytesIO, TextIOBase, TextIOWrapper
import itertools
import pytest # type: ignore[import]
from typing import Callable, cast, Optional

from bjec import json
from bjec.io import Writeable, WriteOpenable
from bjec.params import P, ParamSet, ParamsEvaluable


class WriteOpenableFromBytes(WriteOpenable):
    # TODO: move type to io module

    class _BytesIO(BytesIO):
    	def __init__(
    		self,
    		initial_bytes: bytes = b'',
    		pre_close_cb: Callable[['WriteOpenableFromBytes._BytesIO'], None] = lambda _: None,
    	) -> None:
    		super(WriteOpenableFromBytes._BytesIO, self).__init__(initial_bytes)
    		self._pre_close_cb: Callable[[WriteOpenableFromBytes._BytesIO], None] = pre_close_cb

    	def close(self) -> None:
    		self._pre_close_cb(self)
    		super(WriteOpenableFromBytes._BytesIO, self).close()

    def __init__(self, initial_content: bytes=b'') -> None:
        self._content: bytes = initial_content

    @property
    def content(self) -> bytes:
    	return self._content

    def _set_content_from_bytes_io(self, bytes_io: BytesIO) -> None:
    	self._content = bytes_io.getvalue()

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        return cast(TextIOBase, TextIOWrapper(
            WriteOpenableFromBytes._BytesIO(
            	initial_bytes = self._content,
            	pre_close_cb = self._set_content_from_bytes_io,
            ),
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return cast(BufferedIOBase, WriteOpenableFromBytes._BytesIO(
        	initial_bytes = self._content,
        	pre_close_cb = self._set_content_from_bytes_io,
        ))


def _serialise_with_params(inp: ParamsEvaluable[Writeable], params: ParamSet) -> bytes:
	w = WriteOpenableFromBytes()
	inp.evaluate_with_params(params).write_to(w)
	return w.content

def test_simple() -> None:
	assert _serialise_with_params(json.Writeable(1), {}) == b'1'
	assert _serialise_with_params(json.Writeable(-1.3), {}) == b'-1.3'
	assert _serialise_with_params(json.Writeable('asdf " asdf'), {}) == b'"asdf \\" asdf"'
	assert _serialise_with_params(json.Writeable([0, 1, 2, 3, 4]), {}) == b'[0, 1, 2, 3, 4]'
	assert _serialise_with_params(json.Writeable(range(5)), {}) == b'[0, 1, 2, 3, 4]'

def test_multi_iterable() -> None:
	writeable = json.Writeable(P('a'))
	assert _serialise_with_params(writeable, {'a': 1}) == b'1'
	assert _serialise_with_params(writeable, {'a': 1}) == b'1'

	writeable = json.Writeable(itertools.repeat(1, 5))
	assert _serialise_with_params(writeable, {}) == b'[1, 1, 1, 1, 1]'
	assert _serialise_with_params(writeable, {}) == b'[1, 1, 1, 1, 1]'

	writeable = json.Writeable([itertools.repeat(1, 5)])
	assert _serialise_with_params(writeable, {}) == b'[[1, 1, 1, 1, 1]]'
	assert _serialise_with_params(writeable, {}) == b'[[1, 1, 1, 1, 1]]'

	writeable = json.Writeable({'a': itertools.repeat(1, 5)})
	assert _serialise_with_params(writeable, {}) == b'{"a": [1, 1, 1, 1, 1]}'
	assert _serialise_with_params(writeable, {}) == b'{"a": [1, 1, 1, 1, 1]}'

	writeable = json.Writeable({'a': [P('a')]})
	assert _serialise_with_params(writeable, {'a': [0, 1, 2]}) == b'{"a": [[0, 1, 2]]}'
	assert _serialise_with_params(writeable, {'a': [0, 1, 2]}) == b'{"a": [[0, 1, 2]]}'
