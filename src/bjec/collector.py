from abc import ABC, abstractmethod
from contextlib import ExitStack
from shutil import copyfileobj
import os
from tempfile import mkstemp
from types import TracebackType
from typing import Any, BinaryIO, Callable, cast, Dict, Generic, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from .params import ParamSet, Resolvable, resolve
from .io import ensure_writeable, PathType, PrimitivePathType, ReadOpenable, resolve_writable, Writeable, WriteOpenableWrapBinaryIO
from .utils import consume, listify

_T = TypeVar('_T')
_S = TypeVar('_S')
_T_contra = TypeVar('_T_contra', contravariant=True)

# TODO: Check covariant-ness
# Does that even matter? No information about individual results is
# extractable from Collector instances and Collector instances are not
# commonly passed as arguments to other functions.
# In that sense, a Collector is a write-only container. Write-only containers
# should be contra-variant: A function accepting a Collector for type _T
# elements will happily work with an collector for any super-type of _T.
# The elements written by the function are both an instance of _T and of any super-type of _T


class Collector(Generic[_T_contra], ABC):
    """Collects and processes per-parameter set results.

    Collector provides the context manager interface. Each collector is a
    non-reentrent context manager. Any long-held resources will only be
    acquired upon entering the context manager, i.e. by opening an aggregation
    file. These resources will be released when exiting the context manager,
    i.e. closing all open files.
    """

    def __enter__(self) -> 'Collector[_T_contra]':
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return None

    @abstractmethod
    def collect(self, results: Iterable[Tuple[ParamSet, _T_contra]]) -> None:
        """Collects and processes all elements within ``results``.

        This method **must** be called while the context manager is in the
        open state.

        Collect may be called never, once or multiple times.

        Args:
            results: Iterable over tuples of the parameter set with the
                associated result.
        """

        raise NotImplementedError()


class Noop(Collector[_T_contra], Generic[_T_contra]):
    def collect(self, results: Iterable[Tuple[ParamSet, _T_contra]]) -> None:
        consume(results)


class Multi(Collector[_T_contra], Generic[_T_contra]):
    def __init__(self, *collectors: Collector[_T_contra]) -> None:
        self._collectors: Tuple[Collector[_T_contra], ...] = collectors
        self._stack: ExitStack = ExitStack()

    @property
    def collectors(self) -> Tuple[Collector[_T_contra], ...]:
        return self._collectors

    def __enter__(self) -> 'Multi[_T_contra]':
        self._stack.__enter__()
        for collector in self._collectors:
            self._stack.enter_context(collector)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return self._stack.__exit__(exc_type, exc_val, exc_tb)

    def collect(self, results: Iterable[Tuple[ParamSet, _T_contra]]) -> None:
        for params, result in results:
            for collector in self._collectors:
                collector.collect(((params, result),))


class Demux(Collector[_T_contra], Generic[_T_contra]):
    """Demux de-multiplexes results, by distributing to different Collectors.

    Args:
        keys: Keys in the parameter set which to consider during demuxing. For
            each distinct combination of values of these keys, a collector is
            maintained.
        factory: Function to call to create a new collector. A reduced
            parameter set is passed as the only argument, containing only
            those parameters specified in ``keys``.
    """

    def __init__(
        self,
        keys: Iterable[str],
        factory: 'Callable[[ParamSet], Collector[_T_contra]]',
    ):
        super(Demux, self).__init__()
        self._keys: Tuple[str, ...] = tuple(keys)
        self._factory: Callable[[ParamSet], Collector[_T_contra]] = factory

        self._stack: ExitStack = ExitStack()
        self._collectors: Dict[Tuple[Any, ...], Collector[_T_contra]] = {}

    @property
    def keys(self) -> Tuple[str, ...]:
        return self._keys

    def __enter__(self) -> 'Demux[_T_contra]':
        self._stack.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return self._stack.__exit__(exc_type, exc_val, exc_tb)

    def collect(self, results: Iterable[Tuple[ParamSet, _T_contra]]) -> None:
        for params, result in results:
            self._route_result(params, result)

    def _route_result(self, params: ParamSet, result: _T) -> None:
        """

        Note:
            mypy prohibits parameters of covariant type. This methods gets
            around that through a cast.
            This is still safe as the access is read-only.
        """
        typed_result = cast(_T_contra, result)

        t = tuple(params[key] for key in self._keys)

        try:
            collector = self._collectors[t]
        except KeyError:
            collector = self._factory({key: params[key] for key in self._keys})
            self._collectors[t] = collector
            self._stack.enter_context(collector)

        collector.collect(((params, typed_result),))


class Convert(Collector[_T_contra], Generic[_T_contra, _S]):
    def __init__(self, f: Callable[[_T_contra], _S], collector: Collector[_S]) -> None:
        self._f: Callable[[_T_contra], _S] = f
        self._collector: Collector[_S] = collector

    @property
    def collector(self) -> Collector[_S]:
        return self._collector

    def __enter__(self) -> 'Convert[_T_contra, _S]':
        self._collector.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return self._collector.__exit__(exc_type, exc_val, exc_tb)

    def collect(self, results: Iterable[Tuple[ParamSet, _T_contra]]) -> None:
        self._collector.collect(map(lambda r: (r[0], self._f(r[1])), results))


class Concatenate(Collector[ReadOpenable]):
    """Concatenates file-like openables into a new file.

    Args:
        path: The file path to be opened as the aggregate file. If ``None`` a
            temporary file is created (which is not deleted).
    """

    def __init__(
        self,
        path: Optional[PathType] = None,
        before_all: Optional[Union[Writeable, str, bytes]] = None,
        after_all: Optional[Union[Writeable, str, bytes]] = None,
        before: Optional[Resolvable[Union[Writeable, str, bytes]]] = None,
        after: Optional[Resolvable[Union[Writeable, str, bytes]]] = None,
    ):
        super(Concatenate, self).__init__()

        self._before_all: Optional[Union[Writeable, str, bytes]] = before_all
        self._after_all: Optional[Union[Writeable, str, bytes]] = after_all
        self._before: Optional[Resolvable[Union[Writeable, str, bytes]]] = before
        self._after: Optional[Resolvable[Union[Writeable, str, bytes]]] = after

        self._aggregate_path: PrimitivePathType
        if path is not None:
            self._aggregate_path = os.fspath(path)
        else:
            fd, self._aggregate_path = mkstemp()
            os.close(fd)
        self._aggregate_file: Optional[BinaryIO] = None

    @property
    def path(self) -> Union[str, bytes]:
        return self._aggregate_path

    def __enter__(self) -> 'Concatenate':
        if self._aggregate_file is not None:
            raise Exception('Wrong usage. _aggregate_file is set but is expected to not be.')

        self._aggregate_file = open(self._aggregate_path, 'wb')

        if self._before_all is not None:
            openable = WriteOpenableWrapBinaryIO(self._aggregate_file)
            ensure_writeable(self._before_all).write_to(openable)

        return self

    def __exit__(self, *args: Any) -> Optional[bool]:
        if self._aggregate_file is None:
            raise Exception('Wrong usage. _aggregate_file is not set but is expected to be.')

        if self._after_all is not None:
            openable = WriteOpenableWrapBinaryIO(self._aggregate_file)
            ensure_writeable(self._after_all).write_to(openable)

        self._aggregate_file.close()
        self._aggregate_file = None

        return None

    def collect(self, results: Iterable[Tuple[ParamSet, ReadOpenable]]) -> None:
        if self._aggregate_file is None:
            raise Exception('Wrong usage. _aggregate_file is not set but is expected to be.')

        for params, result in results:
            if self._before is not None:
                openable = WriteOpenableWrapBinaryIO(self._aggregate_file)
                resolve_writable(self._before, params).write_to(openable)

            with result.open_bytes() as result_file:
                copyfileobj(result_file, self._aggregate_file)

            if self._after is not None:
                openable = WriteOpenableWrapBinaryIO(self._aggregate_file)
                resolve_writable(self._after, params).write_to(openable)
