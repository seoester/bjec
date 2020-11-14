from collections import ChainMap
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
import functools
import itertools
import os
import subprocess
from tempfile import mkstemp
from threading import Lock
from types import TracebackType
from typing import Any, Callable, Iterable, Iterator, List, Optional, Tuple, Type, Union

from .config import config
from .io import PrimitivePathType, WriteableFromPath, WriteOpenableFromPath
from .params import ParamSet
from .process import FileAccessor, Process, Result
from .processor import Processor
from .utils import CallbackOnException, HandlersCollector, HandlersList


class _ProcessFailedError(Exception):
    pass


@dataclass(frozen=True)
class FileDescriptor(object):
    name: str
    open_path: PrimitivePathType
    process_path: PrimitivePathType
    temporary: bool
    cleanup: bool
    path: Optional[PrimitivePathType] = None

    def accessor(self) -> FileAccessor:
        return FileAccessor(self.name, self.open_path, self.path)


def prepare_input_file(
    spec: Union[Process.InputFile, Process.Stdin],
    exit_handlers: HandlersCollector,
    cleanup_handlers: HandlersCollector,
    name: str = '',
    temp_dir: Optional[str] = None,
) -> FileDescriptor:
    if spec.source is None:
        raise Exception('Invalid use, spec.source is None')

    descriptor: FileDescriptor
    write_to = True

    if name == '' and isinstance(spec, Process.InputFile):
        name = spec.name

    if isinstance(spec.source, WriteableFromPath) and spec.path is not None:
        if os.path.samefile(spec.path, spec.source.path):
            raise _ProcessFailedError(
                f'Input file {name} is to be sourced from its own path (circular dependency)',
            )

    if spec.path is not None:
        if spec.create_parents:
            os.makedirs(os.path.dirname(spec.path), exist_ok=True)

        try:
            fd = os.open(spec.path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, mode=spec.mode)
            os.close(fd)
        except FileExistsError:
            if spec.must_not_exist:
                raise _ProcessFailedError(
                    f'Input file {name} already exists at path {spec.path!s}',
                )
            else:
                os.chmod(spec.path, spec.mode & ~_get_umask())

        descriptor = FileDescriptor(
            name,
            spec.path,
            spec.path,
            temporary = False,
            cleanup = spec.cleanup_after_finish,
            path = spec.path
        )
    else:
        if isinstance(spec.source, WriteableFromPath):
            write_to = False
            descriptor = FileDescriptor(
                name,
                os.fspath(spec.source.path),
                os.fspath(spec.source.path),
                temporary = False,
                cleanup = False,
            )
        else:
            fd, temp_file_path = mkstemp(dir=temp_dir)
            os.close(fd)

            descriptor = FileDescriptor(
                name,
                temp_file_path,
                temp_file_path,
                temporary = True,
                cleanup = True,
            )

    if write_to:
        spec.source.write_to(WriteOpenableFromPath(descriptor.open_path))

    if descriptor.cleanup:
        cleanup_handlers.callback(lambda: os.unlink(descriptor.open_path))

    return descriptor

def prepare_output_file(
    spec: Union[Process.OutputFile, Process.Stdout],
    exit_handlers: HandlersCollector,
    cleanup_handlers: HandlersCollector,
    name: str = '',
    temp_dir: Optional[str] = None,
) -> FileDescriptor:
    if isinstance(spec, Process.Stdout) and not spec.capture:
        raise Exception('Invalid use, spec.capture is False')

    descriptor: FileDescriptor

    if name == '' and isinstance(spec, Process.OutputFile):
        name = spec.name

    if spec.path is not None:
        if spec.create_parents:
            os.makedirs(os.path.dirname(spec.path), exist_ok=True)

        try:
            fd = os.open(spec.path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, mode=spec.mode)
            os.close(fd)
        except FileExistsError:
            if spec.must_not_exist:
                raise _ProcessFailedError(
                    f'Output file {name} already exists at path {spec.path!s}',
                )

        path = spec.path
        mode = spec.mode
        def set_mode() -> None:
            try:
                os.chmod(path, mode & ~_get_umask())
            except FileNotFoundError:
                raise _ProcessFailedError(f'Output file {name} was not created by process')

        exit_handlers.callback(set_mode)

        descriptor = FileDescriptor(
            name,
            spec.path,
            spec.path,
            temporary = False,
            cleanup = spec.cleanup_after_finish,
            path = spec.path,
        )
    else:
        fd, temp_file_path = mkstemp(dir=temp_dir)
        os.close(fd)

        descriptor = FileDescriptor(
            name,
            temp_file_path,
            temp_file_path,
            temporary = True,
            cleanup = True,
        )

    if isinstance(spec, Process.OutputFile) and not spec.create:
        try:
            os.unlink(descriptor.open_path)
        except FileNotFoundError:
            pass

    if descriptor.cleanup:
        cleanup_handlers.callback(lambda: os.unlink(descriptor.open_path))

    return descriptor

def _get_umask() -> int:
    current_umask = os.umask(0o022)
    os.umask(current_umask)
    return current_umask

def _path_to_str(path: PrimitivePathType) -> str:
    if isinstance(path, str):
        return path
    else:
        return os.fsdecode(path)


class Subprocessor(Processor[Result]):
    """Subprocessor runs Process executions concurrently using threads.

    Args:
        max_processes: Maximum number of processes to be run
            concurrently. If ``<= 0``, the configuration option of the same
            name is used instead. ``1`` is used if the configuration option is
            not set.

    Configuration Options:

        * ``max_processes``: Maximum number of processes to be run
            concurrently. The option is used when ``max_processes`` passed to
            the constructor is ``<= 0``.

    Todo:
        * Recognise when a process died because of a signal
        * Read further options from configuration
        * Process wrapper which adds Subprocessor specific options which are
            not available in Process.
        * Further options: temporary directory, ...
        * What is the behaviour regarding missing output files after process
            completion?
        * How to cancel ongoing processes upon __exit__. At least don't
            execute further ones.
    """

    def __init__(self, max_processes: int=0) -> None:
        super(Subprocessor, self).__init__()
        if max_processes <= 0:
            max_processes = config[Subprocessor].get('max_processes', 1)
            if max_processes <= 0:
                raise ValueError('Invalid value for max_processes retrieved (<= 0)')

        self._max_processes: int = max_processes
        self._pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_processes)

        self._cleanup_handlers: HandlersList = HandlersList()
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

    def process(
        self, runnable: Any, params_it: Iterable[ParamSet],
    ) -> Iterator[Tuple[ParamSet, Result]]:
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

    def _run_subprocess(self, params: ParamSet) -> Tuple[Result, HandlersList]:
        cleanup_handlers = HandlersList()
        with CallbackOnException(cleanup_handlers):
            with ExitStack() as stack:
                p = self._process.with_params(params)

                stdin: int = subprocess.DEVNULL
                stdin_descriptor: Optional[FileDescriptor] = None
                if p.stdin.connected:
                    stdin_descriptor, stdin = self._prepare_stdin(
                        p.stdin, stack, cleanup_handlers,
                    )
                stdout: int = subprocess.DEVNULL
                stdout_descriptor: Optional[FileDescriptor] = None
                if p.stdout.capture:
                    stdout_descriptor, stdout = self._prepare_stdout(
                        p.stdout, stack, cleanup_handlers,
                    )
                stderr: int = subprocess.DEVNULL
                stderr_descriptor: Optional[FileDescriptor] = None
                if p.stderr.capture:
                    stderr_descriptor, stderr = self._prepare_stdout(
                        p.stderr, stack, cleanup_handlers, name='stderr',
                    )

                input_files: List[FileDescriptor] = [
                    prepare_input_file(input_file, stack, cleanup_handlers)
                    for input_file in p.input_files
                ]

                output_files: List[FileDescriptor] = [
                    prepare_output_file(output_file, stack, cleanup_handlers)
                    for output_file in p.output_files
                ]

                all_file_descriptors = itertools.chain(input_files, output_files)
                p = self._process.with_params(ChainMap({
                    f'__file_{file.name}': _path_to_str(file.process_path)
                    for file in all_file_descriptors
                }, params))

                s = subprocess.Popen(
                    [p.cmd] + p.args,
                    cwd = p.working_directory,
                    env = p.environment,
                    stdin = stdin,
                    stdout = stdout,
                    stderr = stderr,
                )
                # TODO: simply add s to the stack
                # might close stdin and stdout, this needs to be checked
                # Or keep here to catch keyboard interrupts and perform custom shutdown?
                s.wait()

            result = Result(
                exit_code = s.returncode,
                stdin = stdin_descriptor.accessor() if stdin_descriptor is not None else None,
                stdout = stdout_descriptor.accessor() if stdout_descriptor is not None else None,
                stderr = stderr_descriptor.accessor() if stderr_descriptor is not None else None,
                input_files = {
                    file.name: file.accessor() for file in input_files
                },
                output_files = {
                    file.name: file.accessor() for file in output_files
                },
            )

            return result, cleanup_handlers

    def _prepare_stdin(
        self,
        spec: Process.Stdin,
        exit_handlers: HandlersCollector,
        cleanup_handlers: HandlersCollector,
        name: str = 'stdin',
    ) -> Tuple[FileDescriptor, int]:
        descriptor = prepare_input_file(spec, exit_handlers, cleanup_handlers, name=name)
        fd = os.open(descriptor.open_path, os.O_RDONLY)
        exit_handlers.callback(os.close, fd)
        return descriptor, fd

    def _prepare_stdout(
        self,
        spec: Process.Stdout,
        exit_handlers: HandlersCollector,
        cleanup_handlers: HandlersCollector,
        name: str = 'stdout',
    ) -> Tuple[FileDescriptor, int]:
        descriptor = prepare_output_file(spec, exit_handlers, cleanup_handlers, name=name)
        fd = os.open(descriptor.open_path, os.O_WRONLY|os.O_TRUNC)
        exit_handlers.callback(os.close, fd)
        return descriptor, fd
