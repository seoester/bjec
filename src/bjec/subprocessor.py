from collections import ChainMap
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import functools
import itertools
import os
import subprocess
from tempfile import mkstemp
from threading import Lock
from types import TracebackType
from typing import Any, Callable, Iterable, Iterator, List, Optional, Tuple, Type, Union

from .config import config
from .io import WriteableFromPath, WriteOpenableFromPath
from .params import ParamSet
from .process import FileAccessor, Process, Result
from .processor import Processor


class _ProcessFailedError(Exception):
    pass


class _HandlersList(object):
    def __init__(self) -> None:
        self._handlers: List[Callable[[], None]] = []

    def append(self, handler: Callable[[], None]) -> None:
        return self._handlers.append(handler)

    def __iadd__(self, other: Iterable[Callable[[], None]]) -> '_HandlersList':
        self._handlers += other
        return self

    def clear(self) -> None:
        self._handlers.clear()

    def __iter__(self) -> Iterator[Callable[[], None]]:
        return iter(self._handlers)

    def __call__(self) -> None:
        for handler in self._handlers:
            handler()


class _CallbackOnException(object):
    def __init__(self, f: Callable[..., None], *args: Any, **kwargs: Any) -> None:
        self._callback: Callable[[], None] = functools.partial(f, *args, **kwargs)

    def __enter__(self) -> '_CallbackOnException':
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


class Subprocessor(Processor[Result]):
    """Subprocessor runs Process executions concurrently using threads.

    Args:
        max_processes (int): Maximum number of processes to be run
            concurrently. If ``<= 0``, the configuration option of the same
            name is used instead. ``1`` is used if the configuration option is
            not set.

    Configuration Options:

        * ``max_processes``: Maximum number of processes to be run
            concurrently. The option is used when ``max_processes`` passed to
            the constructor is ``<= 0``.

    Todo:
        * Read further options from configuration
        * Process wrapper which adds Subprocessor specific options which are
            not available in Process.
        * Further options: temporary directory, ...
        * What is the behaviour regarding missing output files after process
            completion?
        * How to efficiently cancel ongoing processes upon __exit__. At least
            don't execute further ones.
    """

    def __init__(self, max_processes: int=0) -> None:
        super(Subprocessor, self).__init__()
        if max_processes <= 0:
            max_processes = config[Subprocessor].get('max_processes', 1)
            if max_processes <= 0:
                raise ValueError('Invalid value for max_processes retrieved (<= 0)')

        self._max_processes: int = max_processes
        self._pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_processes)

        self._cleanup_handlers: _HandlersList = _HandlersList()
        self._cleanup_handlers_lock: Lock = Lock()

    @property
    def max_processes(self) -> int:
        return self._max_processes

    def __exit__(self, *args: Any) -> Optional[bool]:
        self._pool.shutdown(wait=True)

        with self._cleanup_handlers_lock:
            self._cleanup_handlers()
            self._cleanup_handlers.clear()

        return None

    def process(self, runnable: Any, params_it: Iterable[ParamSet]) -> Iterator[Tuple[ParamSet, Result]]:
        if not isinstance(runnable, Process):
            raise Exception(f'{self.__class__.__name__} only supports Process runnables')

        native_runnable = _ProcessRunner(self, runnable)

        return self._pool.map(native_runnable.run, params_it)


class _ProcessRunner(object):
    def __init__(self, subprocessor: Subprocessor, process: Process) -> None:
        self._subprocessor: Subprocessor = subprocessor
        self._process: Process = process

    def run(self, params: ParamSet) -> Tuple[ParamSet, Result]:
        try:
            result, cleanup_handlers = self._run_subprocess(params)
        except _ProcessFailedError as e:
            # TODO: log failure
            print(e)
            raise e

        try:
            self._check_for_failure(result, params)
        except _ProcessFailedError as e:
            # TODO: log failure
            print(e)
            cleanup_handlers()
            raise e
        except:
            cleanup_handlers()

        with self._subprocessor._cleanup_handlers_lock:
            self._subprocessor._cleanup_handlers += cleanup_handlers

        return params, result

    def _check_for_failure(self, result: Result, params: ParamSet) -> None:
        failure_mode = self._process.with_params(params).failure_mode

        if failure_mode.interpret_exit_code is not None:
            if failure_mode.interpret_exit_code(result.exit_code):
                raise _ProcessFailedError(f'Exit code {result.exit_code} interpreted as failure')
        if failure_mode.interpret_stderr is not None:
            if failure_mode.interpret_stderr(result.stderr):
                raise _ProcessFailedError(f'Stderr interpreted as failure')
        if failure_mode.interpret_stdout is not None:
            if failure_mode.interpret_stdout(result.stdout):
                raise _ProcessFailedError(f'Stdout interpreted as failure')

    def _run_subprocess(self, params: ParamSet) -> Tuple[Result, _HandlersList]:
        with ExitStack() as stack:
            cleanup_handlers = _HandlersList()
            stack.enter_context(_CallbackOnException(cleanup_handlers))

            p = self._process.with_params(params)

            stdin: int = subprocess.DEVNULL
            stdin_accessor: Optional[FileAccessor] = None
            if p.stdin.connected:
                stdin_accessor, stdin = self._prepare_stdin(p.stdin, stack, cleanup_handlers)
            stdout: int = subprocess.DEVNULL
            stdout_accessor: Optional[FileAccessor] = None
            if p.stdout.capture:
                stdout_accessor, stdout = self._prepare_stdout(p.stdout, stack, cleanup_handlers)
            stderr: int = subprocess.DEVNULL
            stderr_accessor: Optional[FileAccessor] = None
            if p.stderr.capture:
                stderr_accessor, stderr = self._prepare_stdout(
                    p.stderr, stack, cleanup_handlers, name='stderr',
                )

            input_files: List[FileAccessor] = []
            for input_file in p.input_files:
                accessor = self._prepare_input_file(input_file, stack, cleanup_handlers)
                input_files.append(accessor)

            output_files: List[FileAccessor] = []
            for output_file in p.output_files:
                accessor = self._prepare_output_file(output_file, stack, cleanup_handlers)
                output_files.append(accessor)

            params_accessible_files = itertools.chain(input_files, output_files)
            overlayed_params = dict(params)
            overlayed_params.update({
                f'__file_{file.name}': file.open_path for file in params_accessible_files
            })
            p = self._process.with_params(overlayed_params)
            # TODO
            # p = self._process.with_params(ChainMap({
            #     f'__file_{file.name}': file.open_path for file in params_accessible_files
            # }, params))

            s = subprocess.Popen(
                [p.cmd] + p.args,
                cwd = p.working_directory,
                env = p.environment,
                stdin = stdin,
                stdout = stdout,
                stderr = stderr,
            )
            # TODO: simply add to the stack
            # might close stdin and stdout, this needs to be checked
            s.wait()

        result = Result(
            exit_code = s.returncode,
            stdin = stdin_accessor,
            stdout = stdout_accessor,
            stderr = stderr_accessor,
            input_files = {
                file.name: file for file in input_files
            },
            output_files = {
                file.name: file for file in output_files
            },
        )

        return result, cleanup_handlers

    def _prepare_stdin(
        self,
        desc: Process.Stdin,
        stack: ExitStack,
        cleanup_handlers: _HandlersList,
        name: str = 'stdin',
    ) -> Tuple[FileAccessor, int]:
        accessor = self._prepare_input_file(desc, stack, cleanup_handlers, name=name)
        fd = os.open(accessor.open_path, os.O_RDONLY)
        stack.callback(os.close, fd)
        return accessor, fd

    def _prepare_stdout(
        self,
        desc: Process.Stdout,
        stack: ExitStack,
        cleanup_handlers: _HandlersList,
        name: str = 'stdout',
    ) -> Tuple[FileAccessor, int]:
        accessor = self._prepare_output_file(desc, stack, cleanup_handlers, name=name)
        fd = os.open(accessor.open_path, os.O_WRONLY|os.O_TRUNC)
        stack.callback(os.close, fd)
        return accessor, fd

    def _prepare_input_file(
        self,
        desc: Union[Process.InputFile, Process.Stdin],
        stack: ExitStack,
        cleanup_handlers: _HandlersList,
        name: str = '',
    ) -> FileAccessor:
        if desc.source is None:
            raise Exception('Invalid use, desc.source is None')

        accessor: FileAccessor
        cleanup = True
        write_to = True

        if name == '' and isinstance(desc, Process.InputFile):
            name = desc.name

        if isinstance(desc.source, WriteableFromPath) and desc.path is not None:
            if os.path.samefile(desc.path, desc.source.path):
                raise _ProcessFailedError(
                    f'Input file {name} is to be sourced from its own path (circular dependency)',
                )

        if desc.path is not None:
            try:
                fd = os.open(desc.path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, mode=desc.mode)
                os.close(fd)
            except FileExistsError:
                if desc.must_not_exist:
                    raise _ProcessFailedError(f'Input file {name} already exists')
                else:
                    os.chmod(desc.path, desc.mode & ~_get_umask())

            accessor = FileAccessor(name, desc.path, path=desc.path)
            cleanup = desc.cleanup_after_finish
        else:
            if isinstance(desc.source, WriteableFromPath):
                accessor = FileAccessor(name, os.fspath(desc.source.path))
                cleanup = False
                write_to = False
            else:
                fd, temp_file_path = mkstemp()
                os.close(fd)

                accessor = FileAccessor(name, temp_file_path)

        if write_to:
            desc.source.write_to(WriteOpenableFromPath(accessor.open_path))

        if cleanup:
            cleanup_handlers.append(lambda: os.unlink(accessor.open_path))

        return accessor

    def _prepare_output_file(
        self,
        desc: Union[Process.OutputFile, Process.Stdout],
        stack: ExitStack,
        cleanup_handlers: _HandlersList,
        name: str = '',
    ) -> FileAccessor:
        accessor: FileAccessor
        cleanup = True

        if name == '' and isinstance(desc, Process.OutputFile):
            name = desc.name

        if desc.path is not None:
            if desc.must_not_exist:
                try:
                    fd = os.open(desc.path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, mode=desc.mode)
                    os.close(fd)
                except FileExistsError:
                    raise _ProcessFailedError(f'Output file {name} already exists')
            else:
                path = desc.path
                mode = desc.mode
                def set_modes() -> None:
                    try:
                        os.chmod(path, mode & ~_get_umask())
                    except FileNotFoundError:
                        raise _ProcessFailedError(f'Output file {name} was not created by process')

                stack.callback(set_modes)

            accessor = FileAccessor(name, desc.path, path=desc.path)
            cleanup = desc.cleanup_after_finish
        else:
            fd, temp_file_path = mkstemp()
            os.close(fd)

            accessor = FileAccessor(name, temp_file_path)

        if isinstance(desc, Process.OutputFile) and not desc.create:
            try:
                os.unlink(accessor.open_path)
            except FileNotFoundError:
                pass

        if cleanup:
            cleanup_handlers.append(lambda: os.unlink(accessor.open_path))

        return accessor


def _get_umask() -> int:
    current_umask = os.umask(0o022)
    os.umask(current_umask)
    return current_umask
