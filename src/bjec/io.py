from io import BufferedIOBase, BufferedWriter, FileIO, TextIOBase, TextIOWrapper, RawIOBase
from os import fspath, PathLike
import os.path
from shutil import copyfileobj
from typing import Any, BinaryIO, Callable, cast, Optional, TextIO, TYPE_CHECKING, Union
from typing_extensions import Protocol

from .params import ParamSet, Resolvable, resolve

"""

Proper typing of I/O related types is still incomplete in Python.

The lack of an ecosystem of protocol classes representing the read and write
concepts is a problem. The abstract base classes in the standard library's io
package are arguably over-specified as they include many file-specific
concepts along with various mix-ins. This is sensible for sub-classing, but
difficult to use in the construction of a solid io module to be used
throughout bjec.

The recommended path is to define protocols for the arguments accepted by each
function. Thus, the plan for this module is to define several basic and
composable protocols for reading and writing. Until then, ``TextIOBase`` and
``BufferedIOBase`` serve as the basis of this module.

https://github.com/python/typeshed/issues/3225

"""

if TYPE_CHECKING:
    _AnyPathLike = PathLike[Any]
else:
    _AnyPathLike = PathLike

PathType = Union[str, bytes, _AnyPathLike]
PrimitivePathType = Union[str, bytes]
# _Source = Union['Writeable', str, bytes] # or _ExtendedWriteable


class WriteOpenable(Protocol):
    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        ...

    def open_bytes(self) -> BufferedIOBase:
        ...


class WriteOpenableFromPath(WriteOpenable):
    def __init__(self, path: PathType) -> None:
        self._path: PathType = path

    @property
    def path(self) -> PathType:
        return self._path

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        return cast(TextIOBase, open(
            self._path,
            mode = 'wt',
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return cast(BufferedIOBase, open(self._path, mode='wb'))


class WriteOpenableWrapBinaryIO(WriteOpenable):
    def __init__(self, b: BinaryIO) -> None:
        self._b: BinaryIO = b

    def _rebuffered(self) -> BufferedIOBase:
        self._b.flush()
        return BufferedWriter(FileIO(self._b.fileno(), mode='w', closefd=False))

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        # TODO: Both these cast are terrible, but seem to be necessary
        return cast(TextIOBase, TextIOWrapper(
            cast(BinaryIO, self._rebuffered()),
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return self._rebuffered()


class ReadOpenable(Protocol):
    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        ...

    def open_bytes(self) -> BufferedIOBase:
        ...


class ReadOpenableFromPath(ReadOpenable):
    def __init__(self, path: PathType) -> None:
        self._path: PathType = path

    @property
    def path(self) -> PathType:
        return self._path

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        return cast(TextIOBase, open(
            self._path,
            mode = 'rt',
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return cast(BufferedIOBase, open(self._path, mode='rb'))


class ReadOpenableWrapBinaryIO(ReadOpenable):
    def __init__(self, b: BinaryIO) -> None:
        self._b: BinaryIO = b

    def _rebuffered(self) -> BufferedIOBase:
        self._b.flush()
        return BufferedWriter(FileIO(self._b.fileno(), mode='r', closefd=False))

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        # TODO: Both these cast are terrible, but seem to be necessary
        return cast(TextIOBase, TextIOWrapper(
            cast(BinaryIO, self._rebuffered()),
            encoding = encoding,
            errors = errors,
            newline = newline,
        ))

    def open_bytes(self) -> BufferedIOBase:
        return self._rebuffered()


class Writeable(Protocol):
    def write_to(self, w: WriteOpenable) -> None:
        ...


class WriteableWrapFunc(Writeable):
    def __init__(self, func: Callable[[WriteOpenable], None]) -> None:
        self._func: Callable[[WriteOpenable], None] = func

    def write_to(self, w: WriteOpenable) -> None:
        self._func(w)


class WriteableFromPath(Writeable):
    class Parameterised(object):
        def __init__(self, path: Resolvable[PathType]) -> None:
            self._path: Resolvable[PathType] = path

        def evaluate_with_params(self, params: ParamSet) -> 'WriteableFromPath':
            return WriteableFromPath(fspath(cast(PathType, resolve(self._path, params))))


    def __init__(self, path: PathType) -> None:
        self._path: PathType = os.path.abspath(path)

    @property
    def path(self) -> PathType:
        return self._path

    def write_to(self, w: WriteOpenable) -> None:
        # TODO: attempt to use os.sendfile if available
        with open(self._path, 'rb') as src, w.open_bytes() as dst:
            copyfileobj(src, dst)


class WriteableFromStr(Writeable):
    def __init__(
        self,
        content: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> None:
        self._content: str = content
        self._encoding: Optional[str] = encoding
        self._errors: Optional[str] = errors
        self._newline: Optional[str] = newline

    @property
    def content(self) -> str:
        return self._content

    @property
    def encoding(self) -> Optional[str]:
        return self._encoding

    @property
    def errors(self) -> Optional[str]:
        return self._errors

    @property
    def newline(self) -> Optional[str]:
        return self._newline

    def write_to(self, w: WriteOpenable) -> None:
        with w.open_text(
            encoding = self._encoding,
            errors = self._errors,
            newline = self._newline,
        ) as f:
            f.write(self._content)


class WriteableFromBytes(Writeable):
    def __init__(self, content: bytes) -> None:
        self._content: bytes = content

    @property
    def content(self) -> bytes:
        return self._content

    def write_to(self, w: WriteOpenable) -> None:
        with w.open_bytes() as f:
            f.write(self._content)


def resolve_path(path: Resolvable[PathType], params: ParamSet) -> PrimitivePathType:
    return fspath(cast(PathType, resolve(path, params)))

def resolve_abs_path(path: Resolvable[PathType], params: ParamSet) -> PrimitivePathType:
    return os.path.abspath(cast(PathType, resolve(path, params)))

def resolve_writable(
    source: Resolvable[Union[Writeable, str, bytes]],
    params: ParamSet,
) -> Writeable:
    # typing the resolve() call seems to be too complicated for mypy
    resolved_source = cast('Union[Writeable, str, bytes]', resolve(source, params))
    return ensure_writeable(resolved_source)

def ensure_writeable(source: Union[Writeable, str, bytes]) -> Writeable:
    if isinstance(source, str):
        return WriteableFromStr(source)
    elif isinstance(source, bytes):
        return WriteableFromBytes(source)
    else:
        return source
