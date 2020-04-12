from io import BufferedIOBase, BytesIO, TextIOBase, TextIOWrapper
import pytest # type: ignore[import]
from tempfile import NamedTemporaryFile
from typing import cast, Iterable, Optional, Tuple

from bjec import csv
from bjec.io import ReadOpenable
from bjec.params import P

input_a = b"""scenario,rate
simple,0.34
complex,0.12
""".replace(b'\n', b'\r\n')

input_b = b"""scenario,rate
simple,0.21
complex,0.09
""".replace(b'\n', b'\r\n')

output_simple = b"""scenario,rate
simple,0.34
complex,0.12
scenario,rate
simple,0.21
complex,0.09
""".replace(b'\n', b'\r\n')

class ReadOpenableFromBytes(ReadOpenable):
    # TODO: move type to io module

    def __init__(self, content: bytes) -> None:
        self._content: bytes = content

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        return cast(TextIOBase, TextIOWrapper(
            BytesIO(self._content),
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return cast(BufferedIOBase, BytesIO(self._content))


def test_simple() -> None:
    with NamedTemporaryFile() as f:
        with csv.Collector(
            path = f.name,
        ) as c:
            c.collect([
                ({}, ReadOpenableFromBytes(input_a)),
                ({}, ReadOpenableFromBytes(input_b)),
            ])

        assert f.read() == output_simple

output_simple_headers = b"""scenario,rate
simple,0.34
complex,0.12
simple,0.21
complex,0.09
""".replace(b'\n', b'\r\n')

def test_simple_headers() -> None:
    with NamedTemporaryFile() as f:
        with csv.Collector(
            path = f.name,
            manage_headers = True,
        ) as c:
            c.collect([
                ({}, ReadOpenableFromBytes(input_a)),
                ({}, ReadOpenableFromBytes(input_b)),
            ])

        assert f.read() == output_simple_headers

output_before_after = b"""before_all_0_0
before_all_1_0
before_0,a
before_1
a,scenario,rate,a
a,simple,0.34,a
a,complex,0.12,a
after_0,a
after_1
before_0,b
before_1
b,scenario,rate,b
b,simple,0.21,b
b,complex,0.09,b
after_0,b
after_1
after_all_0_0
after_all_1_0
""".replace(b'\n', b'\r\n')

def test_before_after() -> None:
    with NamedTemporaryFile() as f:
        with csv.Collector(
            path = f.name,
            before_all = [['before_all_0_0'], ['before_all_1_0']],
            after_all = [['after_all_0_0'], ['after_all_1_0']],
            before = [['before_0', P('input')], ['before_1']],
            after = [['after_0', P('input')], ['after_1']],
            before_row = [P('input')],
            after_row = [P('input')],
        ) as c:
            c.collect([
                ({'input': 'a'}, ReadOpenableFromBytes(input_a)),
                ({'input': 'b'}, ReadOpenableFromBytes(input_b)),
            ])

        assert f.read() == output_before_after

output_before_after_headers = b"""before_header,scenario,rate,after_header
before_all_0_0
before_all_1_0
before_0,a
before_1
a,simple,0.34,a
a,complex,0.12,a
after_0,a
after_1
before_0,b
before_1
b,simple,0.21,b
b,complex,0.09,b
after_0,b
after_1
after_all_0_0
after_all_1_0
""".replace(b'\n', b'\r\n')

def test_before_after_headers() -> None:
    with NamedTemporaryFile() as f:
        with csv.Collector(
            path = f.name,
            before_all = [['before_all_0_0'], ['before_all_1_0']],
            after_all = [['after_all_0_0'], ['after_all_1_0']],
            before = [['before_0', P('input')], ['before_1']],
            after = [['after_0', P('input')], ['after_1']],
            before_row = [P('input')],
            after_row = [P('input')],
            manage_headers = True,
            before_header_row = ['before_header'],
            after_header_row = ['after_header'],
        ) as c:
            c.collect([
                ({'input': 'a'}, ReadOpenableFromBytes(input_a)),
                ({'input': 'b'}, ReadOpenableFromBytes(input_b)),
            ])

        assert f.read() == output_before_after_headers
