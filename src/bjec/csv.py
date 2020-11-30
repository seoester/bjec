import csv
from io import TextIOWrapper
import itertools
import os
from tempfile import mkstemp
from typing import Any, BinaryIO, cast, Iterable, Iterator, List, Mapping, Optional, TextIO, Tuple, Union
from typing_extensions import Protocol

from .collector import Collector as CollectorABC
from .io import ReadOpenable, PathType, PrimitivePathType, WriteOpenableWrapBinaryIO
from .params import ensure_multi_iterable, IterableResolvable, ParamsEvaluable, ParamSet, Resolvable, resolve_iterable

_Row = Iterable[Any]
_Rows = Iterable[Iterable[Any]]
_RowResolvable = IterableResolvable[Any]
_RowsResolvable = Union[ParamsEvaluable[_Rows], Iterable[IterableResolvable[Any]]]

def _resolve_rows(rows: _RowsResolvable, params: ParamSet) -> _Rows:
    try:
        return cast('ParamsEvaluable[_Rows]', rows).evaluate_with_params(params)
    except (AttributeError, TypeError):
        return (resolve_iterable(row, params) for row in cast('Iterable[IterableResolvable[Any]]', rows))

def _prepare_rows(rows: Optional[_Rows]) -> _Rows:
    if rows is None:
        return []

    return [list(row) for row in rows]

def _prepare_row(row: Optional[_Row]) -> _Row:
    if row is None:
        return []

    return list(row)

def _prepare_rows_resolvable(rows: Optional[_RowsResolvable]) -> _RowsResolvable:
    if rows is None:
        return []

    if isinstance(rows, ParamsEvaluable):
        return rows

    return [ensure_multi_iterable(row) for row in rows]

def _prepare_row_resolvable(row: Optional[_RowResolvable]) -> _RowResolvable:
    if row is None:
        return []

    return ensure_multi_iterable(row)


_CSVReader = Iterator[List[str]]


class _CSVWriter(Protocol):
    def writerow(self, row: Iterable[Any]) -> Any: ...
    def writerows(self, rows: Iterable[Iterable[Any]]) -> None: ...


class Collector(CollectorABC[ReadOpenable]):
    """Concatenates CSV from file-like read openables into an aggregate file.

    Args:
        path: The file path to be opened as the aggregate file. If ``None`` a
            a file in a system specific location for temporary files is
            created. This file is never deleted by the Collector but may be
            deleted by OS mechanisms. It should not be treated as permanent.
        before_all: Rows (of columns) added before any rows from input files.
            That means these are written at the very beginning of the
            aggregate file. If ``manage_headers`` is ``True``, these rows are
            written **after** the header row into the aggregate file.
        after_all: Rows (of columns) added after all rows from input files.
            That means these are written at the very end of the aggregate
            file.
        before: Rows (of columns) added before any input rows for each input
            file. That means these are written at the beginning of input file
            specific rows in the aggregate file. Parameters of the input file
            may be used in ``before``.
        after: Rows (of columns) added after all input rows for each input
            file. That means these are written at the end of input file
            specific rows in the aggregate file. Parameters of the input file
            may be used in ``after``.
        before_row: Columns inserted before each input row. That means these
            are written at the beginning of each input row in the aggregate
            file. Parameters of the input file may be used in ``before_row``.
        after_row: Columns inserted after each input row. That means these
            are written at the end of each input row in the aggregate file.
            Parameters of the input file may be used in ``after_row``.
        manage_headers: If ``True`` the first row of each input file is
            treated as a header row and only actual data rows in input files
            are concatenated. The header row is written once at the very
            beginning of the aggregate file. For this to work, the headers of
            all input file must be identical. An exception is raised if
            inconsistent
        before_header_row: Columns added before any input headers. That means
            these are written at the front of the header row of the aggregate
            file. Only interpreted if ``manage_headers`` is ``True``.
        after_header_row: Columns added after any input headers. That means
            these are written at the end of the header row of the aggregate
            file. Only interpreted if ``manage_headers`` is ``True``.
        input_encoding: Encoding to use when reading input files. Passed as-is
            to the :obj:`TextIOWrapper` constructor.
        input_error: Error setting to use when reading input files. Passed
            as-is to the :obj:`TextIOWrapper` constructor.
        input_csv_args: Args passed to :func:`csv.reader` when constructing
            readers for input files. This may include the ``dialect`` key.
        output_encoding: Encoding to use when writing output files. Passed
            as-is to the :obj:`TextIOWrapper` constructor.
        output_error: Error setting to use when writing output files. Passed
            as-is to the :obj:`TextIOWrapper` constructor.
        output_csv_args: Args passed to :func:`csv.writer` when constructing
            the writer for output file. This may include the ``dialect`` key.
    """

    def __init__(
        self,
        path: Optional[PathType] = None,

        before_all: Optional[_Rows] = None,
        after_all: Optional[_Rows] = None,
        before: Optional[_RowsResolvable] = None,
        after: Optional[_RowsResolvable] = None,
        before_row: Optional[_RowResolvable] = None,
        after_row: Optional[_RowResolvable] = None,

        manage_headers: bool = False,
        before_header_row: Optional[_Row] = None,
        after_header_row: Optional[_Row] = None,

        input_encoding: Optional[str] = None,
        input_errors: Optional[str] = None,
        input_csv_args: Optional[Mapping[str, Any]] = None,
        output_encoding: Optional[str] = None,
        output_errors: Optional[str] = None,
        output_csv_args: Optional[Mapping[str, Any]] = None,
    ):
        super(Collector, self).__init__()

        self._before_all: _Rows = _prepare_rows(before_all)
        self._after_all: _Rows = _prepare_rows(after_all)
        self._before: _RowsResolvable = _prepare_rows_resolvable(before)
        self._after: _RowsResolvable = _prepare_rows_resolvable(after)
        self._before_row: _RowResolvable = _prepare_row_resolvable(before_row)
        self._after_row: _RowResolvable = _prepare_row_resolvable(after_row)

        if not manage_headers and (before_header_row is not None or after_header_row is not None):
            raise Exception('Invalid initialisation, cannot pass before_header_row or '
                'after_header_row if manage_headers is False.')

        self._manage_headers: bool = manage_headers
        self._headers: Optional[List[str]] = None
        self._before_header_row: _RowResolvable = _prepare_row(before_header_row)
        self._after_header_row: _RowResolvable = _prepare_row(after_header_row)

        self._aggregate_path: PrimitivePathType
        if path is not None:
            self._aggregate_path = os.fspath(path)
        else:
            fd, self._aggregate_path = mkstemp()
            os.close(fd)
        self._aggregate_file: Optional[TextIO] = None
        self._writer: Optional[_CSVWriter] = None

        self._input_encoding: Optional[str] = input_encoding
        self._input_errors: Optional[str] = input_errors
        self._input_csv_args: Mapping[str, Any] = input_csv_args if input_csv_args is not None else {}
        self._output_encoding: Optional[str] = output_encoding
        self._output_errors: Optional[str] = output_errors
        self._output_csv_args: Mapping[str, Any] = output_csv_args if output_csv_args is not None else {}

    @property
    def path(self) -> Union[str, bytes]:
        return self._aggregate_path

    def __enter__(self) -> 'Collector':
        if self._aggregate_file is not None or self._writer is not None:
            raise Exception('Wrong usage. _aggregate_file is set but is expected to not be.')

        f = self._aggregate_file = open(
            self._aggregate_path,
            'wt',
            encoding = self._output_encoding,
            errors = self._output_errors,
            newline = '',
        )
        self._writer = csv.writer(f, **self._output_csv_args)

        if not self._manage_headers:
            self._writer.writerows(self._before_all)
            # Otherwise, _before_all is written after the headers.

        return self

    def __exit__(self, *args: Any) -> Optional[bool]:
        if self._aggregate_file is None or self._writer is None:
            raise Exception('Wrong usage. _aggregate_file is not set but is expected to be.')

        if not self._manage_headers or self._headers is not None:
            # Let's be consistent with _before_all: If _manage_headers,
            # neither _before_all nor _after_all are printed if headers are
            # not known, i.e. no files have been processed.
            self._writer.writerows(self._after_all)

        self._writer = None
        self._aggregate_file.close()
        self._aggregate_file = None

        return None

    def collect(self, results: Iterable[Tuple[ParamSet, ReadOpenable]]) -> None:
        if self._aggregate_file is None or self._writer is None:
            raise Exception('Wrong usage. _aggregate_file is not set but is expected to be.')

        for params, openable in results:
            with TextIOWrapper(
                cast('BinaryIO', openable.open_bytes()),
                encoding = self._input_encoding,
                errors = self._input_errors,
                newline = '',
            ) as file:
                reader = csv.reader(file, **self._input_csv_args)

                self._one(params, reader, self._writer)

    def _one(self, params: ParamSet, reader: _CSVReader, writer: _CSVWriter) -> None:
        if self._manage_headers:
            try:
                header_row = next(reader)
            except StopIteration:
                # TODO
                # Current Strategy: ignore file completely. _before and _after
                # could be written though.
                # Rows cannot be written if self._headers is still None, but
                # could be written otherwise. If self._headers is None,
                # writing would have to be deferred until the self._headers
                # are discovered. If self._headers are never discovered, an
                # exception should probably be thrown at the end.
                # Deferring could be achived by either saving the params or
                # the resolved rows.
                return

            if self._headers is None:
                self._headers = header_row

                writer.writerow(itertools.chain(
                    resolve_iterable(self._before_header_row, params),
                    self._headers,
                    resolve_iterable(self._after_header_row, params),
                ))
                writer.writerows(self._before_all)
            elif header_row != self._headers:
                raise Exception(f'Non-conforming headers encountered')

        before_row = list(resolve_iterable(self._before_row, params))
        after_row = list(resolve_iterable(self._after_row, params))

        writer.writerows(_resolve_rows(self._before, params))

        for row in reader:
            writer.writerow(itertools.chain(
                before_row,
                row,
                after_row,
            ))

        writer.writerows(_resolve_rows(self._after, params))
