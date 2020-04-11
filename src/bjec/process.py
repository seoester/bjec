from dataclasses import dataclass, field
from io import TextIOBase, BufferedIOBase
from os import environ, fspath, PathLike
from shutil import copyfileobj
from typing import cast, Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union
from typing_extensions import Protocol

from .io import PathType, PrimitivePathType, resolve_path, resolve_writable, Writeable, WriteableFromPath
from .params import ensure_multi_iterable, IterableResolvable, ParamSet, resolve, resolve_iterable, Resolvable

"""This module provides the Process abstraction for users and implementers.

Perhaps the most common use case of bjec is the execution of a program. Each
parameter set leads to a distinct execution of a process running the program.
Different runners may implement the execution of processes, hence there is a
common Process abstraction which can be supported by runners.

Todo:
    * Specify the effect of ``create`` on the files presented to the process.
        Achieve this by clearing up naming (files presented to process through
        ``P('__file_NAME')`` and persistent files).
        ``create=False`` ensures that the file is deleted before running the
        process at the moment.
    * Should umask be enforced on existing files?
    * Simplify file accessor interfaces by removing open_text? This can easily
        be substituted with an io.TextIOWrapper (which is what open() does).
    * Run time (and resource consumption) extension. Could use /usr/bin/time or
        information exposed by a batch system.
    * Meta-data which can be passed with files etc. and which can be
        interpreted by the implementer, especially for file specific
        configuration.
    * From MappingResolvable for environment variables etc.
    * Input file reading through result objects
    * Specify behaviour on repeated calls for each fluid method.
    * Append / prepend to env var
    * Failure specification and handling: user function deciding on failure by
        receiving a result (failure state not yet known...), specify common
        handling, i.e. repeat execution x times on failure.
    * Process could be extended to support interacting with the process's file
        descriptors dynamically.
    * Exception types for inconsistencies which are part of the public
        interface and clear and consistent error messages.
    * ProcessFailedError or ExecutionFailedError with temporary field which can
        be used by implementers and users (through failure_mode).
        Another error type which signals that an instantiation failed from a
        high-level view (taking retries and mitigation into account).
"""


_EnvVar = Tuple[Resolvable[str], Resolvable[str]]
_StackEnvVar = Tuple[Resolvable[str], Optional[Resolvable[str]]]

_T = TypeVar('_T')


class Environment(object):
    class Fluid(object):
        def __init__(self) -> None:

            # Each element in the stack represents a set of variables modified
            # by one mutating method. ``None`` as a variable's value takes a
            # special role, it unsets a variable.
            #
            # The stack can be collapsed while remaining ParamSet-Evaluable.
            # This includes unsetting of variables set by earlier
            # calls.
            #
            # However, unsetting can only fully be determined at params
            # evaluation time. This would require Environment to store a stack
            # of functions, where each function accepts the current dict and
            # either
            # a) returns a modified copy or
            # b) passes the dict to the next function. (middleware style)
            # In this case Fluid could probably be folded into Environment.

            self._stack: List[List[_StackEnvVar]] = []

        def from_environment(self, environment: 'Environment') -> 'Environment.Fluid':
            return self + environment._variables

        def inherit(
            self,
            blacklist: Iterable[str] = set(),
            whitelist: Iterable[str] = set(),
        ) -> 'Environment.Fluid':
            blacklist_set = set(blacklist)
            whitelist_set = set(whitelist)
            if len(blacklist_set) > 0 and len(whitelist_set) > 0:
                raise Exception('Cannot specify both a blacklist and a whitelist')

            it: Iterable[Tuple[str, str]] = environ.items()
            if len(blacklist_set) > 0:
                it = filter(lambda item: item[0] not in blacklist_set, it)
            if len(whitelist_set) > 0:
                it = filter(lambda item: item[0] in whitelist_set, it)

            return self + it

        def set(self, **variables: Resolvable[str]) -> 'Environment.Fluid':
            """

            Use __add__ if the keys must be ParamsEvaluable or the variables
            are available as an iterable.
            """
            return self + variables.items()

        def unset(self, *variables: Resolvable[str]) -> 'Environment.Fluid':
            return self.unset_from_iterable(variables)

        def unset_from_iterable(self, variables: Iterable[Resolvable[str]]) -> 'Environment.Fluid':
            return self + ((variable, None) for variable in variables)

        def __add__(self, other: Iterable[_StackEnvVar]) -> 'Environment.Fluid':
            """

            ``None`` leads to the variable being unset.
            """
            return Environment.Fluid._with_stack(self._stack + [list(other)])

        def build(self) -> 'Environment':
            flat_variables: List[_EnvVar] = list()

            for variables in self._stack:
                for key, value in variables:
                    if value is None:
                        flat_variables = list(filter(lambda var: var[0] != key, flat_variables))
                    else:
                        flat_variables = list(filter(lambda var: var[0] != key, flat_variables))
                        flat_variables.append((key, value))

            return Environment(flat_variables)

        @classmethod
        def _with_stack(cls, stack: List[List[_StackEnvVar]]) -> 'Environment.Fluid':
            fluid: 'Environment.Fluid' = Environment.Fluid()
            fluid._stack = stack
            return fluid


    def __init__(self, variables: Iterable[_EnvVar]) -> None:
        self._variables: List[_EnvVar] = list(variables)

    def evaluate_with_params(self, params: ParamSet) -> Dict[str, str]:
        return {
            resolve(key, params): resolve(value, params) for key, value in self._variables
        }


class Process(object):
    """Process template which may contain parameter reference.

    *Implementer* refers to a component which interprets a Process instance
    and executes a program accordingly for each ParamSet. It then constructs
    a Result which is treated as the result for each ParamSet.

    Implementers should receive the information about a process execution
    through :meth:`Process.with_params()`. All fields are resolved and
    simplified as far as possible through property accessors in
    :obj:`Process.WithParams`.

    Regarding all file related methods of the Fluid builder:
    Further configuration options may be made available as part of an
    implementer's configuration. This might include details such as the
    directory for temporary files, the temporary file class to use, buffering
    details, network transfer options, ...

    Lifecycle: Construction using Fluid. Passing to processor / runner.
    Deferred result passing to following stages (linked to process instance?
    this could perform checking, e.g. is stdout available?). Process execution
    for each ParamSet. Construction and return of a Result instance. Finish of
    result processing (causes cleanup) after the following stage is done with
    the Result instance.
    """

    class Fluid(object):
        def __init__(self) -> None:
            self._stack: List[Callable[[Process], None]] = []

        def cmd(self, cmd: Resolvable[str]) -> 'Process.Fluid':
            """Sets the command to be executed.

            The command has to be set, a process cannot execute without
            setting this. If unset, :meth:`Process.validate()` will raise.

            How the command is resolved to a path is up to the implementer.
            """

            def f(p: Process) -> None:
                p._cmd = cmd

            return self + f

        def args(self, *args: Resolvable[str]) -> 'Process.Fluid':
            """Sets the argument lists with which the process is started.

            """

            def f(p: Process) -> None:
                p._args = args

            return self + f

        def args_from_iterable(self, args: IterableResolvable[str]) -> 'Process.Fluid':
            """Sets the argument lists with which the process is started.

            """

            args_list = ensure_multi_iterable(args)

            def f(p: Process) -> None:
                p._args = args_list

            return self + f

        def working_directory(self, dir: Resolvable[str]) -> 'Process.Fluid':
            """Sets the working directory of the process.

            If unset, implementers may execute in any directory.
            """

            def f(p: Process) -> None:
                p._working_directory = dir

            return self + f

        def environment(self, environment: Environment) -> 'Process.Fluid':
            """Sets the environment variables of the process.

            Check :obj:`Environment` and :obj:`Environment.Fluid` for details
            on how to construct this.
            """

            def f(p: Process) -> None:
                p._environment = environment

            return self + f

        def connect_stdin(
            self,
            source: Optional[Resolvable[Union[Writeable, str, bytes]]] = None,
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = True,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Process.Fluid':
            """Configures a file to connect to the process's stdin.

            Args:
                source: Source of the file's content. Use
                    :obj:`WriteableFromPath` to refer to a file in the file
                    system. The value ``None`` is the same as specifying an
                    empty file.
                path: If not ``None`` the file is made available at this path
                    when the result is yielded. Otherwise the implementer may
                    use a temporary file.
                must_not_exist: If ``True`` the execution is considered failed
                    if the file already exists before the process is started.
                    This is evaluated before the process is started and before
                    the file is created from ``source``. Only considered if
                    ``path`` is not ``None``, as otherwise the implementer
                    manages the file.
                mode: Mode bits of the file, see ``os.open()`` for details.
                    Only considered when ``path`` is not ``None`` and
                    ``source`` is not ``None``.
                cleanup_after_finish: If ``True`` the file is deleted when the
                    `finish` lifetime stage is reached. Only considered when
                    ``path`` is not ``None``.
            """

            def f(p: Process) -> None:
                p._stdin = Process._Stdin(
                    source = source,
                    path = path,
                    must_not_exist = must_not_exist,
                    mode = mode,
                    cleanup_after_finish = cleanup_after_finish,
                )

            return self + f

        def capture_stdout(
            self,
            capture: bool = True,
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = True,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Process.Fluid':
            """Configure whether and how stdout is captured.

            Args:
                capture: If ``True`` stdout is captured and made available in
                    ``Result`` instances. Subsequent calls may disable
                    capturing by setting ``False``.
                path: If not ``None`` the stdout is made available at this
                    path. Otherwise the implementer may use a temporary file
                    or store content in-memory.
                must_not_exist: If ``True`` the execution is considered failed
                    if the file already exists before the process is started.
                    This is evaluated before the process is started. Only
                    considered if ``path`` is not ``None``, as otherwise the
                    implementer manages the file.
                mode: Mode bits of the file, see ``os.open()`` for details.
                    Only considered when ``path`` is not ``None``.
                cleanup_after_finish: If ``True`` the stdout file is deleted
                    when the `finish` lifetime stage is reached. Only
                    considered when ``path`` is not ``None``.

            Raises:
                ValueError: If the combination of arguments is not valid.
            """

            if not capture and path is not None:
                raise ValueError('A file path was passed but stdout is not supposed to be captured')

            def f(p: Process) -> None:
                p._stdout = Process._Stdout(
                    capture = capture,
                    path = path,
                    must_not_exist = must_not_exist,
                    mode = mode,
                    cleanup_after_finish = cleanup_after_finish,
                )

            return self + f

        def capture_stderr(
            self,
            capture: bool = True,
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = True,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Process.Fluid':
            """Configure whether and how stderr is captured.

            Args:
                capture: If ``True`` stderr is captured and made available in
                    ``Result`` instances. Subsequent calls may disable
                    capturing by setting ``False``.
                path: If not ``None`` the stderr is made available at this
                    path. Otherwise the implementer may use a temporary file
                    or store content in-memory.
                must_not_exist: If ``True`` the execution is considered failed
                    if the file already exists before the process is started.
                    This is evaluated before the process is started. Only
                    considered if ``path`` is not ``None``, as otherwise the
                    implementer manages the file.
                mode: Mode bits of the file, see ``os.open()`` for details.
                    Only considered when ``path`` is not ``None``.
                cleanup_after_finish: If ``True`` the stderr file is deleted
                    when the `finish` lifetime stage is reached. Only
                    considered when ``path`` is not ``None``.

            Raises:
                ValueError: If the combination of arguments is not valid.
            """

            if not capture and path is not None:
                raise ValueError('A file path was passed but stderr is not supposed to be captured')

            def f(p: Process) -> None:
                p._stderr = Process._Stdout(
                    capture = capture,
                    path = path,
                    must_not_exist = must_not_exist,
                    mode = mode,
                    cleanup_after_finish = cleanup_after_finish,
                )

            return self + f

        def add_input_file(
            self,
            name: str,
            source: Resolvable[Union[Writeable, str, bytes]],
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = True,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Process.Fluid':
            """Adds an input file to the Process.

            Args:
                name: Name through which the file is available for
                    referencing. The file's path is available as
                    ``P('__file_NAME')`` during evaluation of all
                    ``ParamsEvaluable`` constructs of the ``Process``.
                    If an input file with this name already exists, its
                    configuration is overwritten.
                    The same name must not be used for an input file and an
                    output file, :meth:`Process.validate` will raise if this
                    is the case.
                source: Source of the file's content. Use
                    :obj:`WriteableFromPath` to refer to a file in the file
                    system.
                path: If not ``None`` the input file is made available at this
                    path when the result is yielded. Otherwise the implementer
                    may use a temporary file.
                must_not_exist: If ``True`` the execution is considered failed
                    if the file already exists before the process is started.
                    This is evaluated before the process is started and before
                    the file is created from ``source``. Only considered if
                    ``path`` is not ``None``, as otherwise the implementer
                    manages the file.
                mode: Mode bits of the file, see ``os.open()`` for details.
                    Only considered when ``path`` is not ``None`` and
                    ``source`` is not ``None``.
                cleanup_after_finish: If ``True`` the input file is deleted
                    when the `finish` lifetime stage is reached. Only
                    considered when ``path`` is not ``None``.

            Todo:
                * Specify suffix for the file path in ``P('__file_NAME')``.
                    Relevant, as some programs interpret the file ending.
            """

            def f(p: Process) -> None:
                p._input_files[name] = Process._InputFile(
                    name = name,
                    source = source,
                    path = path,
                    must_not_exist = must_not_exist,
                    mode = mode,
                    cleanup_after_finish = cleanup_after_finish,
                )

            return self + f

        def remove_input_file(self, name: str) -> 'Process.Fluid':
            """Removes an input file from the Process by name.
            """

            def f(p: Process) -> None:
                del p._input_files[name]

            return self + f

        def add_output_file(
            self,
            name: str,
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = True,
            create: bool = True,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Process.Fluid':
            """Adds an output file to the Process.

            Args:
                name: Name through which the file is available for
                    referencing. The file's path is available as
                    ``P('__file_NAME')`` during evaluation of all
                    ``ParamsEvaluable`` constructs of the ``Process``.
                    If an output file with this name already exists, its
                    configuration is overwritten.
                    The same name must not be used for an input file and an
                    output file, :meth:`Process.validate` will raise if this
                    is the case.
                path: If not ``None`` the output file is made available at this
                    path when the result is yielded. Otherwise the implementer
                    may use a temporary file.
                must_not_exist: If ``True`` the execution is considered failed
                    if the file already exists before the process is started.
                    This is evaluated before the process is started and before
                    the file is created via ``create``. Only considered if
                    ``path`` is not ``None``, as otherwise the implementer
                    manages the file.
                    If ``False`` it is considered a failure if the process did
                    not create the file.
                create: If ``True`` the file is ensured to be present before
                    the process is started. If ``False`` the file is ensured
                    to not be present, meaning any file at the path will be
                    deleted.
                mode: Mode bits of the file, see ``os.open()`` for details.
                    Only considered when ``path`` is not ``None``. If
                    ``create`` is ``True``, the bits are set before the
                    process is started, otherwise after the process has
                    finished.
                cleanup_after_finish: If ``True`` the output file is deleted
                    when the `finish` lifetime stage is reached. Only
                    considered when ``path`` is not ``None``.

            Todo:
                * Specify suffix for the file path in ``P('__file_NAME')``.
                    Relevant, as some programs interpret the file ending.
            """

            def f(p: Process) -> None:
                p._output_files[name] = Process._OutputFile(
                   name = name,
                   path = path,
                   must_not_exist = must_not_exist,
                   create = create,
                   mode = mode,
                   cleanup_after_finish  = cleanup_after_finish,
                )

            return self + f

        def remove_output_file(self, name: str) -> 'Process.Fluid':
            """Removes an output file from the Process by name.
            """

            def f(p: Process) -> None:
                del p._output_files[name]

            return self + f

        def failure_mode(
            self,
            interpret_exit_code: Optional[Callable[[int], bool]] = None,
            interpret_stderr: Optional[Callable[['FileAccessor'], bool]] = None,
            interpret_stdout: Optional[Callable[['FileAccessor'], bool]] = None,
        ) -> 'Process.Fluid':
            """Configure when a process execution is considered to be failed.

            The default behaviour is to consider any execution returning a
            non-0 exit code as failed. If any argument is passed, this
            behaviour is disabled.

            If *any* predicate evaluates to ``True``, the execution is
            considered a failure.

            Args:
                interpret_exit_code: Predicate function to interpret the exit
                    code. Return ``True`` if the exit code is considered a
                    failure.
                interpret_stderr: Predicate function to interpret the stderr
                    stream. This only works if stderr capturing is configured
                    via :meth:`.capture_stderr`, otherwise
                    :meth:`Process.validate` will raise. Return ``True`` if
                    the exit code is considered a failure.
                interpret_stdout: Predicate function to interpret the stdout
                    stream. This only works if stdout capturing is configured
                    via :meth:`.capture_stdout`, otherwise
                    :meth:`Process.validate` will raise. Return ``True`` if
                    the exit code is considered a failure.
            """

            def f(p: Process) -> None:
                p._failure_mode = Process._FailureMode(
                    interpret_exit_code = interpret_exit_code,
                    interpret_stderr = interpret_stderr,
                    interpret_stdout = interpret_stdout,
                )

            return self + f

        def __add__(self, other: Callable[['Process'], None]) -> 'Process.Fluid':
            return Process.Fluid._with_stack(self._stack + [other])

        def build(self) -> 'Process':
            p = Process()
            for f in self._stack:
                f(p)

            p.validate()

            return p

        @classmethod
        def _with_stack(cls, stack: List[Callable[['Process'], None]]) -> 'Process.Fluid':
            fluid: 'Process.Fluid' = Process.Fluid()
            fluid._stack = stack
            return fluid


    class WithParams(object):
        def __init__(self, process: 'Process', params: ParamSet) -> None:
            self._process: Process = process
            self._params: ParamSet = params

        @property
        def cmd(self) -> str:
            return resolve(self._process._cmd, self._params)

        @property
        def args(self) -> List[str]:
            return list(resolve_iterable(self._process._args, self._params))

        @property
        def working_directory(self) -> Optional[str]:
            return resolve(self._process._working_directory, self._params)

        @property
        def environment(self) -> Dict[str, str]:
            return self._process._environment.evaluate_with_params(self._params)

        @property
        def stdin(self) -> 'Process.Stdin':
            stdin = self._process._stdin
            return Process.Stdin(
                source = None if stdin.source is None else resolve_writable(stdin.source, self._params),
                path = None if stdin.path is None else resolve_path(stdin.path, self._params),
                must_not_exist = stdin.must_not_exist,
                mode = stdin.mode,
                cleanup_after_finish = stdin.cleanup_after_finish,
            )

        @property
        def stdout(self) -> 'Process.Stdout':
            stdout = self._process._stdout
            return Process.Stdout(
                capture = stdout.capture,
                path = None if stdout.path is None else resolve_path(stdout.path, self._params),
                must_not_exist = stdout.must_not_exist,
                mode = stdout.mode,
                cleanup_after_finish = stdout.cleanup_after_finish,
            )

        @property
        def stderr(self) -> 'Process.Stdout':
            stderr = self._process._stderr
            return Process.Stdout(
                capture = stderr.capture,
                path = None if stderr.path is None else resolve_path(stderr.path, self._params),
                must_not_exist = stderr.must_not_exist,
                mode = stderr.mode,
                cleanup_after_finish = stderr.cleanup_after_finish,
            )

        @property
        def input_files(self) -> 'List[Process.InputFile]':
            def r(file: 'Process._InputFile') -> 'Process.InputFile':
                return Process.InputFile(
                    name = file.name,
                    source = resolve_writable(file.source, self._params),
                    path = None if file.path is None else resolve_path(file.path, self._params),
                    must_not_exist = file.must_not_exist,
                    mode = file.mode,
                    cleanup_after_finish = file.cleanup_after_finish,
                )

            return list(map(r, self._process._input_files.values()))

        @property
        def output_files(self) -> 'List[Process.OutputFile]':
            def r(file: 'Process._OutputFile') -> 'Process.OutputFile':
                return Process.OutputFile(
                    name = file.name,
                    path = None if file.path is None else resolve_path(file.path, self._params),
                    must_not_exist = file.must_not_exist,
                    create = file.create,
                    mode = file.mode,
                    cleanup_after_finish = file.cleanup_after_finish,
                )

            return list(map(r, self._process._output_files.values()))

        @property
        def failure_mode(self) -> 'Process.FailureMode':
            return Process.FailureMode(
                interpret_exit_code = self._process._failure_mode.interpret_exit_code,
                interpret_stderr = self._process._failure_mode.interpret_stderr,
                interpret_stdout = self._process._failure_mode.interpret_stdout,
            )


    @dataclass
    class _OutputFile(object):
        name: str
        path: Optional[Resolvable[PathType]] = None
        must_not_exist: bool = True
        create: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass
    class _InputFile(object):
        name: str
        source: Resolvable[Union[Writeable, str, bytes]]
        path: Optional[Resolvable[PathType]] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass
    class _Stdin(object):
        source: Optional[Resolvable[Union[Writeable, str, bytes]]] = None
        path: Optional[Resolvable[PathType]] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass
    class _Stdout(object):
        capture: bool = False
        path: Optional[Resolvable[PathType]] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass
    class _FailureMode(object):
        interpret_exit_code: Optional[Callable[[int], bool]] = None
        interpret_stderr: Optional[Callable[['FileAccessor'], bool]] = None
        interpret_stdout: Optional[Callable[['FileAccessor'], bool]] = None


    @dataclass(frozen=True)
    class OutputFile(object):
        name: str
        path: Optional[PrimitivePathType] = None
        must_not_exist: bool = True
        create: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass(frozen=True)
    class InputFile(object):
        name: str
        source: Writeable
        path: Optional[PrimitivePathType] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass(frozen=True)
    class Stdin(object):
        source: Optional[Writeable] = None
        path: Optional[PrimitivePathType] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False

        @property
        def connected(self) -> bool:
            return self.source is not None


    @dataclass(frozen=True)
    class Stdout(object):
        capture: bool = False
        path: Optional[PrimitivePathType] = None
        must_not_exist: bool = True
        mode: int = 0o666
        cleanup_after_finish: bool = False


    @dataclass(frozen=True)
    class FailureMode(object):
        interpret_exit_code: Optional[Callable[[int], bool]] = None
        interpret_stderr: Optional[Callable[['FileAccessor'], bool]] = None
        interpret_stdout: Optional[Callable[['FileAccessor'], bool]] = None


    def __init__(self) -> None:
        self._cmd: Resolvable[str] = ''
        self._args: IterableResolvable[str] = []
        self._working_directory: Optional[Resolvable[str]] = None
        self._environment: Environment = Environment([])
        self._output_files: Dict[str, Process._OutputFile] = {}
        self._input_files: Dict[str, Process._InputFile] = {}
        self._stdin: Process._Stdin = Process._Stdin()
        self._stdout: Process._Stdout = Process._Stdout()
        self._stderr: Process._Stdout = Process._Stdout()
        self._failure_mode: Process._FailureMode = Process._FailureMode(
            interpret_exit_code = lambda code: code != 0,
        )

    def validate(self) -> None:
        """Raises if the ``Process`` instance is not complete or inconsistent.
        """

        if self._cmd == '':
            raise Exception('cmd must be set.')

        if not self._input_files.keys().isdisjoint(self._output_files.keys()):
            raise Exception('an input file and an output file must not have the same name.')

        if self._failure_mode.interpret_stderr is not None and not self._stderr.capture:
            raise Exception('cannot interpret stderr if not captured.')

        if self._failure_mode.interpret_stdout is not None and not self._stdout.capture:
            raise Exception('cannot interpret stdout if not captured.')

    def with_params(self, params: ParamSet) -> 'Process.WithParams':
        return Process.WithParams(self, params)


class FileAccessor(object):
    """Represents a file accessible for reading.
    """

    def __init__(self, name: str, open_path: PrimitivePathType, path: Optional[PrimitivePathType]=None) -> None:
        self._name: str = name
        self._open_path: PrimitivePathType = open_path
        self._path: Optional[PrimitivePathType] = path

    @property
    def name(self) -> str:
        return self._name

    @property
    def open_path(self) -> PrimitivePathType:
        return self._open_path

    @property
    def path(self) -> PrimitivePathType:
        if self._path is None:
            raise Exception(f'file {self._name} does not have a persistent path')
        return self._path

    def open_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> TextIOBase:
        return cast(TextIOBase, open(self._open_path, mode='rt', encoding=encoding, errors=errors, newline=newline))

    def open_bytes(self) -> BufferedIOBase:
        return cast(BufferedIOBase, open(self._open_path, mode='rb'))


class Result(object):
    def __init__(
        self,
        exit_code: int,
        stdin: Optional[FileAccessor] = None,
        stdout: Optional[FileAccessor] = None,
        stderr: Optional[FileAccessor] = None,
        input_files: Optional[Dict[str, FileAccessor]] = None,
        output_files: Optional[Dict[str, FileAccessor]] = None,
    ) -> None:
        self._exit_code: int = 0
        self._stdin: Optional[FileAccessor] = stdin
        self._stdout: Optional[FileAccessor] = stdout
        self._stderr: Optional[FileAccessor] = stderr
        self._input_files: Dict[str, FileAccessor] = input_files if input_files is not None else {}
        self._output_files: Dict[str, FileAccessor] = output_files if output_files is not None else {}

    @property
    def exit_code(self) -> int:
        return self._exit_code

    @property
    def stdin(self) -> FileAccessor:
        if self._stdin is None:
            raise Exception(f'No file was connected to stdin.')
        return self._stdin

    @property
    def stdout(self) -> FileAccessor:
        if self._stdout is None:
            raise Exception(f'Stdout was not captured.')
        return self._stdout

    @property
    def stderr(self) -> FileAccessor:
        if self._stderr is None:
            raise Exception(f'Stderr was not captured.')
        return self._stderr

    def input_file(self, name: str) -> FileAccessor:
        try:
            return self._input_files[name]
        except KeyError:
            raise Exception(f'There is no input file with the name {name}.')

    def output_file(self, name: str) -> FileAccessor:
        try:
            return self._output_files[name]
        except KeyError:
            raise Exception(f'There is no output file with the name {name}.')


Retrievable = Callable[[Result], _T]


class _DeferredResult(object):
    @property
    def exit_code(self) -> Retrievable[int]:
        def f(r: Result) -> int:
            return r.exit_code

        return f

    @property
    def stdin(self) -> Retrievable[FileAccessor]:
        def f(r: Result) -> FileAccessor:
            return r.stdin

        return f

    @property
    def stdout(self) -> Retrievable[FileAccessor]:
        def f(r: Result) -> FileAccessor:
            return r.stdout

        return f

    @property
    def stderr(self) -> Retrievable[FileAccessor]:
        def f(r: Result) -> FileAccessor:
            return r.stderr

        return f

    def input_file(self, name: str) -> Retrievable[FileAccessor]:
        def f(r: Result) -> FileAccessor:
            return r.input_file(name)

        return f

    def output_file(self, name: str) -> Retrievable[FileAccessor]:
        def f(r: Result) -> FileAccessor:
            return r.output_file(name)

        return f


DeferredResult = _DeferredResult()


def test() -> None:
    from .params import P, String
    from pathlib import PurePosixPath

    data_dir = PurePosixPath('/storage/paul/workspace/data')
    prefix = data_dir / 'physics_groups.two_years/638653110162/'

    p = Process.Fluid(). \
        environment(Environment.Fluid().
            inherit(whitelist=['HOME', 'PATH']).
            set(PIPENV_PIPFILE='/storage/paul/workspace/simulator/Pipfile').
            build()
        ). \
        add_input_file(
            name = 'access_seq',
            source = WriteableFromPath(prefix / 'accessseq.json'),
        ). \
        add_output_file(
            name = 'cache_info',
            path = String(str(prefix) + '/cache_info_{processor}_{storage_size}.json'),
        ). \
        add_output_file(
            name = 'stats_file',
            path = String(str(prefix) + '/stats_{processor}_{storage_size}.csv'),
        ). \
        cmd('/storage/paul/local/bin/pipenv'). \
        args(
            'run', 'python', '-m', 'simulator.cli',
            'replay',
            '-f', P('__file_access_seq'),
            '--cache-processor-count', P('processor_count').transform(str),
            '--cache-processor', P('processor'),
            '--storage-size', P('storage_size').transform(str),
            '--cache-info-file', P('__file_cache_info'),
            '--stats-file', P('__file_stats_file'),
        ). \
        capture_stdout(path=String(str(prefix) + '/out_{processor}_{storage_size}.txt')). \
        capture_stderr(path=String(str(prefix) + '/err_{processor}_{storage_size}.txt')). \
        build()
