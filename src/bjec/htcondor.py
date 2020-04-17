from collections import ChainMap
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from enum import IntEnum
import errno
import itertools
import logging
import os
from random import Random
from shutil import which
from tempfile import gettempdir, mkstemp, TMP_MAX
from time import sleep
from types import TracebackType
from typing import Any, AnyStr, Callable, cast, Dict, Iterable, Iterator, List, Mapping, Optional, SupportsInt, Tuple, Type, Union

from classad import ClassAd
from htcondor import JobAction, Schedd, Submit

from .config import config
from .io import PathType, PrimitivePathType, resolve_abs_path
from .params import ParamSet, resolve, Resolvable
from .process import FileAccessor, Process, Result as ProcessResult
from .processor import Processor
from .subprocessor import FileDescriptor, prepare_input_file, prepare_output_file
from .utils import CallbackOnException, HandlersCollector, HandlersList


"""

Sources:

* https://htcondor.readthedocs.io/en/latest/apis/python-bindings/users/Submitting-and-Managing-Jobs.html
* https://htcondor.readthedocs.io/en/latest/apis/python-bindings/advanced/Advanced-Schedd-Interactions.html

Transfer of Files:

    Jobs are executed in a job-specific working directory referred to as the
    scratch directory on the execute machine. The initial directory refers to
    the directory on the submit machine where ``condor_q`` was called. This
    can be overwritten with the ``initialdir`` command.

    Input files specified via ``transfer_input_files`` are transferred from
    the submit machine to the scratch directory before starting the job.
    Relative paths are evaluated from the initial directory. The basename of
    the file on the submit machine is used as the name in the scratch
    directory. There must be no duplicate basenames. Links are followed.

    If output files are specified via ``transfer_output_files``, these files
    are transferred back to the initial directory. Relative paths are
    evaluated from the scratch directory, absolute paths from the execute
    machine's root. If no output files are specified, all modified files in
    the scratch directory (no subdirectories) are transferred back. The
    basename of the file on the execute machine is used as the name in the
    initial directory. The ``transfer_output_remaps`` may specify other paths
    on the submit machine where files should be copied to.

    The files specified as the values of the ``executable``, ``input``,
    ``output`` and ``error`` commands are implicit input and output files. The
    ``transfer_input``, ... commands determine whether these files are
    transferred to the submit machine. They default to ``True``. ``input``,
    ``output`` and ``error`` can be streamed instead of being copied through
    the ``stream_input``, ... commands.

    The ``should_transfer_files`` command (values: ``YES``, ``NO``,
    ``IF_NEEDED``) controls what clauses are added to the ``requirements``
    attribute of the job's ClassAd. ``NO`` adds a check for
    ``FileSystemDomain`` being equal to the value of the submit machine.
    ``YES`` adds a check for the targeted execute machine to have file
    transfer mechanisms (``HasFileTransfer``). ``IF_NEEDED`` is the default
    and adds both checks combined with a logical or (`||`).

    ``when_to_transfer_output`` (values: ``ON_EXIT``, ``ON_EXIT_OR_EVICT``) is
    set to ``ON_EXIT`` (the default), as there is no clear desired behaviour
    on eviction.

    Sources:

    * https://htcondor.readthedocs.io/en/latest/users-manual/file-transfer.html
    * https://htcondor.readthedocs.io/en/latest/users-manual/submitting-a-job.html
    * https://htcondor.readthedocs.io/en/stable/man-pages/condor_submit.html


File Mapping Strategy:

    stdin, stdout, stderr: Temporary files (/tmp) are used if no paths are
    specified. Otherwise the file path is passed directly to the ``input``,
    ``output`` and ``error`` commands. The ``stream_input``, ... commands are
    never set.

    Input files: The challenge is to achieve unique naming. A temporary file
    (/tmp) is created if no path is specified. These temporary files already
    have unique names due to the temporary file mechanism. If a path is
    specified, a symbolic link using the temporary files mechanism (/tmp) is
    created to the file. The resulting path is specified in the
    ``transfer_input_files`` command. The presented path for both is the
    basename of the temporary file.

    Output files: Output files require unique naming analogously to input
    files. A temporary file (/tmp) is created if no path is specified (unique
    name). If a path is specified, a temporary file (/tmp) is still created to
    have a unique basename. The resulting basenames are specified in
    ``transfer_output_files``. ``transfer_output_remaps`` is used to specify
    the absolute path on the submit machine for each output file.

Job Status Codes:

    ::

        0   Unexpanded      U
        1   Idle            I
        2   Running         R
        3   Removed         X
        4   Completed       C
        5   Held            H
        6   Submission_err  E

    http://pages.cs.wisc.edu/~adesmet/status.html

    https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers


For Testing:

    https://github.com/andypohl/htcondor-docker

    ::

        cd repo-root
        mkdir htcondor_tests
        docker run -d -v `pwd`:/host -h htcondor --name htcondor andypohl/htcondor
        docker exec -it htcondor bash
        (in docker) yum install python3
        docker exec -it -u 1000:1000 htcondor bash
        (in docker) cd /host/htcondor_tests
        (in docker) python3 -m venv venv
        (in docker) . venv/bin/activate
        (in docker) pip install -U pip wheel
        (in docker) pip install -e .[htcondor]
        docker exec -it -u 1000:1000 htcondor bash
        (in docker) cd /host/htcondor_tests
        (in docker) . venv/bin/activate
        (in docker) bjec run test_simple

Todo:
    * Batching of jobs. Set via HTCondor constructor or configuration option.
        Batches are a concurrency mechanism. When submitting via clusters,
        only an entire cluster can be submitted at a time. A batch is a set of
        jobs with a fixed size. Is time-in-queue important for scheduling?
        Perhaps for rank expressions which change with age? In order to
        overlap batches, each cluster could have a size of 1/k * batch_size.
        This way there would be k cluster in the queue simultaneously. A new
        cluster is submitted as soon as there are less than batch_size -
        cluster_size jobs enqueued. The term batch size is not ideal for this
        scenario. Is there any disadvantage in submitting each job in its own
        cluster? That would be the smoothest concurrency control possible.
        Batch, limit jobs per cluster, limit clusters in the queue
    * Requeueing of failed processes. Could be done via the max_retries and
        retry_until commands. retry_until supports class ad expressions but
        would this would require translating Python functions into class ad
        expressions.
    * Recognising temporary errors when querying (or otherwise interacting)
        with HTCondor services. Retry (with backoff?) in that case.
    * Supporting Jobs without a Process?
    * Support for working directory via a wrapper script when running on a
        shared file system?
        When should_transfer_files = NO and transfer_output = False
        (and ...error, ...input, ...executable), initialdir is used as the
        working directory.
    * Setting temp_dir on a job basis?

"""

_Intable = Union[str, bytes, SupportsInt]

def _opt_int(value: Optional[_Intable]) -> Optional[int]:
    if value is None:
        return value
    else:
        return int(value)

def _contains_any(s: str, what: Iterable[str]) -> bool:
    for c in what:
        if c in s:
            return True

    return False

def _quote(token: str) -> str:
    quote_wrap = _contains_any(token, ' \t\'')

    quoted = token.replace('\'', '\'\'').replace('"', '""')

    if quote_wrap:
        return '\'' + quoted + '\''
    elif len(token) == 0:
        return '\'' + token + '\''
    else:
        return quoted

def _args_to_str(args: Iterable[str]) -> str:
    """

    Serialises a list of arguments to a str suitable for the ``arguments``
    command.

    From the ``condor_submit`` reference referring to the ``arguments``
    command as part of a definition file:

    1. The entire string representing the command line arguments is surrounded
        by double quote marks. This permits the white space characters of
        spaces and tabs to potentially be embedded within a single argument.
        Putting the double quote mark within the arguments is accomplished by
        escaping it with another double quote mark.
    2. The white space characters of spaces or tabs delimit arguments.
    3. To embed white space characters of spaces or tabs within a single
        argument, surround the entire argument with single quote marks.
    4. To insert a literal single quote mark, escape it within an argument
        already delimited by single quote marks by adding another single quote
        mark.

    https://htcondor.readthedocs.io/en/stable/man-pages/condor_submit.html
    """

    return '"' + ' '.join(_quote(arg) for arg in args) + '"'

def _environment_to_str(environment: Mapping[str, str]) -> str:
    """

    Serialises key value pairs of an environment to a str suitable for the
    ``environment`` command.

    From the ``condor_submit`` reference referring to ``environment`` command
    as part of a definition file:

    1. Put double quote marks around the entire argument string. This
        distinguishes the new syntax from the old. The old syntax does not
        have double quote marks around it. Any literal double quote marks
        within the string must be escaped by repeating the double quote mark.
    2. Each environment entry has the form <name>=<value>
    3. Use white space (space or tab characters) to separate environment
        entries.
    4. To put any white space in an environment entry, surround the space and
        as much of the surrounding entry as desired with single quote marks.
    5. To insert a literal single quote mark, repeat the single quote mark
        anywhere inside of a section surrounded by single quote marks.


    https://htcondor.readthedocs.io/en/stable/man-pages/condor_submit.html
    """

    return '"' + ' '.join(f'{key}={_quote(value)}' for key, value in environment.items()) + '"'

def _file_remaps_to_str(remaps: Mapping[str, str]) -> str:
    """

    Serialises remaps to a str suitable for the ``transfer_output_remaps``
    command.

    From the ``condor_submit`` reference referring to
    ``transfer_output_remaps`` command as part of a definition file: ::

        transfer_output_remaps = < " name = newname ; name2 = newname2 ... ">

    ``name`` describes an output file name produced by your job, and
    ``newname`` describes the file name it should be downloaded to. Multiple
    remaps can be specified by separating each with a semicolon. If you wish
    to remap file names that contain equals signs or semicolons, these special
    characters may be escaped with a backslash. You cannot specify directories
    to be remapped.

    https://htcondor.readthedocs.io/en/stable/man-pages/condor_submit.html

    Testing with HTCondor v8.9.4 has shown that equal signs only have to be
    escaped in ``name`` and semicolons only have to be escaped in
    ``newname``.
    """

    def esc_k(s: str) -> str:
        return s.replace('=', '\\=')
    def esc_v(s: str) -> str:
        return s.replace(';', '\\;')

    return '"' + ';'.join(f'{esc_k(key)}={esc_v(value)}' for key, value in remaps.items()) + '"'

def _files_to_str(files: Iterable[str]) -> str:
    """

    Serialises a list of files to a str suitable for the
    ``transfer_input_files`` and ``transfer_output_files`` commands.

    There are no documented rules for escaping commas in the file names.
    Testing with HTCondor v8.9.4 did not reveal any obvious escaping scheme
    (tried: backslash escape, single quotation marks).

    https://htcondor.readthedocs.io/en/stable/man-pages/condor_submit.html
    """

    return ','.join(files)

def _path_to_str(path: PrimitivePathType) -> str:
    if isinstance(path, str):
        return path
    else:
        return os.fsdecode(path)

def _get_poll_sleep_times(first: int=10, max: int=600) -> Iterator[int]:
    cur = first
    while cur < max:
        yield cur
        cur *= 2
    for val in itertools.repeat(max):
        yield val


class _ProcessFailedError(Exception):
    pass


class _Status(IntEnum):
    UNEXPANDED = 0
    IDLE = 1
    RUNNING = 2
    REMOVED = 3
    COMPLETED = 4
    HELD = 5
    SUBMISSION_ERR = 6

    @classmethod
    def from_value(cls, val: Union[int, _Intable]) -> '_Status':
        int_val = val if isinstance(val, int) else int(val)

        if int_val == _Status.UNEXPANDED:
            return _Status.UNEXPANDED
        elif int_val == _Status.IDLE:
            return _Status.IDLE
        elif int_val == _Status.RUNNING:
            return _Status.RUNNING
        elif int_val == _Status.REMOVED:
            return _Status.REMOVED
        elif int_val == _Status.COMPLETED:
            return _Status.COMPLETED
        elif int_val == _Status.HELD:
            return _Status.HELD
        elif int_val == _Status.SUBMISSION_ERR:
            return _Status.SUBMISSION_ERR
        else:
            raise ValueError(val)


@dataclass
class _JobState:
    """

    https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html
    """

    cluster_id: int
    proc_id: int
    job_status: _Status
    exit_code: Optional[int]
    exit_by_signal: bool
    exit_signal: Optional[int]

    @classmethod
    def from_class_ad(cls, class_ad: ClassAd) -> '_JobState':
        return cls(
            cluster_id = int(cast(_Intable, class_ad['ClusterId'])),
            proc_id = int(cast(_Intable, class_ad['ProcId'])),
            job_status = _Status.from_value(cast(_Intable, class_ad['JobStatus'])),
            exit_code = _opt_int(cast('Optional[_Intable]', class_ad.get('ExitCode'))),
            exit_by_signal = class_ad.get('ExitBySignal', 'false') == 'true',
            exit_signal = _opt_int(cast('Optional[_Intable]', class_ad.get('ExitSignal'))),
        )

    @staticmethod
    def projection() -> List[str]:
        return [
            'ClusterId', 'ProcId', 'JobStatus', 'ExitCode', 'ExitBySignal', 'ExitSignal',
        ]


@dataclass
class _StatusCounts(object):
    unexpanded: int = 0
    idle: int = 0
    running: int = 0
    removed: int = 0
    completed: int = 0
    held: int = 0
    submission_err: int = 0

    @property
    def total(self) -> int:
        return (
            self.unexpanded
            + self.idle
            + self.running
            + self.removed
            + self.completed
            + self.held
            + self.submission_err
        )

    def __getitem__(self, key: int) -> int:
        if key == _Status.UNEXPANDED:
            return self.unexpanded
        elif key == _Status.IDLE:
            return self.idle
        elif key == _Status.RUNNING:
            return self.running
        elif key == _Status.REMOVED:
            return self.removed
        elif key == _Status.COMPLETED:
            return self.completed
        elif key == _Status.HELD:
            return self.held
        elif key == _Status.SUBMISSION_ERR:
            return self.submission_err
        else:
            raise KeyError(key)

    def __setitem__(self, key: int, val: int) -> None:
        if key == _Status.UNEXPANDED:
            self.unexpanded = val
        elif key == _Status.IDLE:
            self.idle = val
        elif key == _Status.RUNNING:
            self.running = val
        elif key == _Status.REMOVED:
            self.removed = val
        elif key == _Status.COMPLETED:
            self.completed = val
        elif key == _Status.HELD:
            self.held = val
        elif key == _Status.SUBMISSION_ERR:
            self.submission_err = val
        else:
            raise KeyError(key)

    def add_job(self, job: Union[ClassAd, _JobState]) -> None:
        if isinstance(job, _JobState):
            self[job.job_status] += 1
        else:
            self[int(cast(int, job['JobStatus']))] += 1

    def add_jobs(self, jobs: Iterable[Union[ClassAd, _JobState]]) -> None:
        for job in jobs:
            self.add_job(job)


class Job(object):
    class Fluid(object):
        def __init__(self) -> None:
            self._stack: List[Callable[[Job], None]] = []

        def process(self, process: Process) -> 'Job.Fluid':
            """Sets the process to be executed.

            The command has to be set, a job cannot execute without setting
            this. If unset, :meth:`Job.validate()` will raise.
            """

            def f(j: Job) -> None:
                j._process = process

            return self + f

        def commands(self, commands: Mapping[str, Resolvable[str]]) -> 'Job.Fluid':
            """Sets additional commands to include in the job definition.

            Calling this method again will overwrite all additional commands
            previously passed via this method.

            Paths of input and output files are available during evaluation as
            ``P('__file_NAME')``.
            """

            def f(j: Job) -> None:
                j._commands = commands

            return self + f

        def transfer_files(self, transfer: bool=True) -> 'Job.Fluid':
            """Configures whether the file transfer mechanism should be used.
            """

            def f(j: Job) -> None:
                j._transfer_files = transfer

            return self + f

        def capture_log(
            self,
            capture: bool = True,
            path: Optional[Resolvable[PathType]] = None,
            must_not_exist: bool = False,
            mode: int = 0o666,
            cleanup_after_finish: bool = False,
        ) -> 'Job.Fluid':
            """Configure whether and how the job log is captured.

            Args:
                capture: If ``True`` the job log is captured and made available
                    in ``Result`` instances. Subsequent calls may disable
                    capturing by setting ``False``.
                path: If not ``None`` the job log is made available at this
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
                raise ValueError('A file path was passed but log is not supposed to be captured')

            def f(j: Job) -> None:
                j._log = Job._Log(
                    capture = capture,
                    path = path,
                    must_not_exist = must_not_exist,
                    mode = mode,
                    cleanup_after_finish = cleanup_after_finish,
                )

            return self + f

        def __add__(self, other: Callable[['Job'], None]) -> 'Job.Fluid':
            return Job.Fluid._with_stack(self._stack + [other])

        def build(self) -> 'Job':
            j = Job()
            for f in self._stack:
                f(j)

            j.validate()

            return j

        @classmethod
        def _with_stack(cls, stack: List[Callable[['Job'], None]]) -> 'Job.Fluid':
            fluid: 'Job.Fluid' = Job.Fluid()
            fluid._stack = stack
            return fluid


    @dataclass()
    class _Log(object):
        capture: bool = False
        path: Optional[Resolvable[PathType]] = None
        must_not_exist: bool = False
        mode: int = 0o666
        cleanup_after_finish: bool = False


    class WithParams(object):
        def __init__(self, job: 'Job', params: ParamSet) -> None:
            self._job: Job = job
            self._params: ParamSet = params

        @property
        def process(self) -> Process.WithParams:
            return self._job.process.with_params(self._params)

        @property
        def commands(self) -> Dict[str, str]:
            return {
                key: resolve(value, self._params) for key, value in self._job.commands.items()
            }

        @property
        def transfer_files(self) -> bool:
            return resolve(self._job.transfer_files, self._params)

        @property
        def log(self) -> Process.Stdout:
            log = self._job.log
            return Process.Stdout(
                capture = log.capture,
                path = None if log.path is None else resolve_abs_path(log.path, self._params),
                must_not_exist = log.must_not_exist,
                mode = log.mode,
                cleanup_after_finish = log.cleanup_after_finish,
            )


    def __init__(self) -> None:
        self._process: Process = Process()
        self._commands: Mapping[str, Resolvable[str]] = {}
        self._transfer_files: Resolvable[bool] = True
        self._log: Job._Log = Job._Log()

    def validate(self) -> None:
        self._process.validate()

    @property
    def process(self) -> Process:
        return self._process

    @property
    def commands(self) -> Mapping[str, Resolvable[str]]:
        return self._commands

    @property
    def transfer_files(self) -> Resolvable[bool]:
        return self._transfer_files

    @property
    def log(self) -> 'Job._Log':
        return self._log

    def with_params(self, params: ParamSet) -> 'Job.WithParams':
        return Job.WithParams(self, params)


class Result(ProcessResult):
    def __init__(
        self,
        exit_code: int,
        stdin: Optional[FileAccessor] = None,
        stdout: Optional[FileAccessor] = None,
        stderr: Optional[FileAccessor] = None,
        input_files: Optional[Dict[str, FileAccessor]] = None,
        output_files: Optional[Dict[str, FileAccessor]] = None,
        log: Optional[FileAccessor] = None,
    ) -> None:
        super(Result, self).__init__(
            exit_code = exit_code,
            stdin = stdin,
            stdout = stdout,
            stderr = stderr,
            input_files = input_files,
            output_files = output_files,
        )
        self._log: Optional[FileAccessor] = log

    @property
    def log(self) -> FileAccessor:
        if self._log is None:
            raise Exception(f'Log was not captured.')
        return self._log


class HTCondor(Processor[Result]):
    """HTCondor performs Process executions through HTCondor's schedd.

    Args:
        schedd: :obj:`Schedd` instance to use for submitting jobs and querying their state.
        temp_dir: Directory in which temporary files and links are created while processing. If
            unset, the configuration option of the same name is used. The system default as
            determined by :func:`gettempdir` (e.g. ``/tmp``) is used if the configuration option is
            not set.

    Configuration Options:

        * ``temp_dir``: Directory in which temporary files and links are created while processing.

    Todo:
        * Additional arguments (config, constructor):
            Default commands for jobs?
    """

    def __init__(
        self,
        schedd: Optional[Schedd] = None,
        temp_dir: Optional[str] = None,
    ) -> None:
        super(HTCondor, self).__init__()
        self._schedd: Schedd = schedd if schedd is not None else Schedd()
        self._temp_dir: str = self._temp_dir_value(temp_dir)

        self._cleanup_handlers: HandlersList = HandlersList()

    def _temp_dir_value(self, temp_dir: Optional[str]) -> str:
        if temp_dir is not None:
            return temp_dir
        if 'temp_dir' in config[HTCondor]:
            return cast(str, config[HTCondor]['temp_dir'])
        return gettempdir()

    def __exit__(self, *args: Any) -> Optional[bool]:
        self._cleanup_handlers()
        self._cleanup_handlers.clear()

        return None

    def process(
        self, runnable: Any, params_it: Iterable[ParamSet],
    ) -> Iterator[Tuple[ParamSet, Result]]:
        job: Job
        if isinstance(runnable, Process):
            job = Job.Fluid().process(runnable).build()
        elif isinstance(runnable, Job):
            job = runnable
        else:
            raise Exception(f'{self.__class__.__name__} only supports Job and Process runnables')

        return self._run_job(job, params_it)

    def _run_job(self, job: Job, params_it: Iterable[ParamSet]) -> Iterator[Tuple[ParamSet, Result]]:
        # TODO: introduce public API of Process to get working_directory
        if job.process._working_directory is not None:
            raise Exception('HTCondor does not support setting the working_directory on Process')

        with ExitStack() as stack:
            cluster_generator = _JobClusterGenerator(self, job, params_it)
            stack.enter_context(cluster_generator)

            submit = Submit()

            with self._schedd.transaction() as txn:
                submit_result = submit.queue_with_itemdata(txn, itemdata=iter(cluster_generator))

            stack.callback(
                self._schedd.act, JobAction.Remove, f'ClusterId == {submit_result.cluster()}',
            )

            job_states: Dict[int, _JobState] = {}

            for sleep_time in _get_poll_sleep_times():
                sleep(sleep_time)

                query_result = self._schedd.xquery(
                    requirements = f'ClusterId == {submit_result.cluster()}',
                    projection = _JobState.projection(),
                )

                job_states.clear()
                for job_state_ad in query_result:
                    job_state = _JobState.from_class_ad(job_state_ad)
                    job_states[job_state.proc_id] = job_state

                counts = _StatusCounts()
                counts.add_jobs(job_states.values())

                print(counts)

                if counts.completed == counts.total:
                    break

            results: List[Tuple[ParamSet, Result]] = []
            for proc_id, process in enumerate(cluster_generator.processes):
                job_state = job_states[proc_id]
                if job_state.exit_by_signal:
                    raise _ProcessFailedError(
                        f'Process exited due to receiving signal {job_state.exit_signal}',
                    )
                if job_state.exit_code is None:
                    raise Exception('Exit code received from HTCondor is None')

                result = process.result(job_state.exit_code)
                self._check_for_failure(job, result, process.params)
                results.append((process.params, result))

            self._cleanup_handlers += cluster_generator.cleanup_handlers

        return iter(results)

    def _check_for_failure(self, job: Job, result: Result, params: ParamSet) -> None:
        failure_mode = job.with_params(params).process.failure_mode

        if failure_mode.interpret_exit_code is not None:
            if failure_mode.interpret_exit_code(result.exit_code):
                raise _ProcessFailedError(f'Exit code {result.exit_code} interpreted as failure')
        if failure_mode.interpret_stderr is not None:
            if failure_mode.interpret_stderr(result.stderr):
                raise _ProcessFailedError(f'Stderr interpreted as failure')
        if failure_mode.interpret_stdout is not None:
            if failure_mode.interpret_stdout(result.stdout):
                raise _ProcessFailedError(f'Stdout interpreted as failure')


class _JobClusterGenerator(object):
    @dataclass
    class _ProcessInfo(object):
        params: ParamSet
        stdin: Optional[FileDescriptor] = None
        stdout: Optional[FileDescriptor] = None
        stderr: Optional[FileDescriptor] = None
        log: Optional[FileDescriptor] = None
        input_files: List[FileDescriptor] = field(default_factory=list)
        output_files: List[FileDescriptor] = field(default_factory=list)

        def result(self, exit_code: int) -> Result:
            return Result(
                exit_code,
                stdin = self.stdin.accessor() if self.stdin is not None else None,
                stdout = self.stdout.accessor() if self.stdout is not None else None,
                stderr = self.stderr.accessor() if self.stderr is not None else None,
                log = self.log.accessor() if self.log is not None else None,
                input_files = {
                    file.name: file.accessor() for file in self.input_files
                },
                output_files = {
                    file.name: file.accessor() for file in self.output_files
                },
            )

    def __init__(self, htcondor: HTCondor, job: Job, params_it: Iterable[ParamSet]) -> None:
        self._htcondor: HTCondor = htcondor
        self._job: Job = job
        self._params_it: Iterable[ParamSet] = params_it

        self._processes: List[_JobClusterGenerator._ProcessInfo] = []
        self._cleanup_handlers: HandlersList = HandlersList()
        self._stack: ExitStack = ExitStack()

    @property
    def cleanup_handlers(self) -> HandlersList:
        return self._cleanup_handlers

    @property
    def processes(self) -> 'List[_JobClusterGenerator._ProcessInfo]':
        return self._processes

    def __enter__(self) -> '_JobClusterGenerator':
        self._stack.__enter__()
        self._stack.enter_context(CallbackOnException(self._cleanup_handlers))
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ) -> Optional[bool]:
        return self._stack.__exit__(exc_type, exc_val, exc_tb)

    def __iter__(self) -> Iterator[Dict[str, str]]:
        for params in self._params_it:
            try:
                job = self._job.with_params(params)
                info = _JobClusterGenerator._ProcessInfo(params)

                data: Dict[str, str] = {}

                if job.transfer_files:
                    file_params = self._prepare_all_files_for_transfer(job, data, info)
                else:
                    file_params = self._prepare_all_files_for_no_transfer(job, data, info)

                job = self._job.with_params(ChainMap(file_params, params))

                data.update(
                    universe = 'vanilla',
                    executable = _lookup_cmd(job.process.cmd),
                    arguments = _args_to_str(job.process.args),
                    environment = _environment_to_str(job.process.environment),
                    leave_in_queue = 'JobStatus != 3',
                )

                data.update(job.commands)

            except BaseException as e:
                # Ensure that at least one element has been yielded as the
                # htcondor Python bindings seg fault otherwise. See #7609.
                # https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=7609
                yield {'executable': '/bin/ls'}
                raise e

            self._processes.append(info)

            print(data)

            yield data

    def _prepare_all_files_for_transfer(
        self,
        job: Job.WithParams,
        data: Dict[str, str],
        info: '_JobClusterGenerator._ProcessInfo',
    ) -> Dict[str, str]:
        self._prepare_std_files(job, data, info)

        info.input_files = [
            self._prepare_input_file_for_transfer(spec) for spec in job.process.input_files
        ]

        info.output_files = [
            self._prepare_output_file_for_transfer(spec) for spec in job.process.output_files
        ]

        data.update(
            should_transfer_files = 'YES',
            transfer_executable = 'True',
            transfer_input = 'True',
            transfer_output = 'True',
            transfer_error = 'True',
            transfer_input_files = _files_to_str(
                _path_to_str(file.process_path) for file in info.input_files
            ),
            transfer_output_files = _files_to_str(
                _path_to_str(os.path.basename(file.process_path)) for file in info.output_files
            ),
            transfer_output_remaps = _file_remaps_to_str({
                _path_to_str(os.path.basename(file.process_path)): _path_to_str(file.open_path)
                for file in info.output_files
            }),
            when_to_transfer_output = 'ON_EXIT',
        )

        all_file_descriptors = itertools.chain(info.input_files, info.output_files)
        return {
            f'__file_{file.name}': _path_to_str(os.path.basename(file.process_path))
            for file in all_file_descriptors
        }

    def _prepare_all_files_for_no_transfer(
        self,
        job: Job.WithParams,
        data: Dict[str, str],
        info: '_JobClusterGenerator._ProcessInfo',
    ) -> Dict[str, str]:
        self._prepare_std_files(job, data, info)

        temp_dir = self._htcondor._temp_dir

        info.input_files = [
            prepare_input_file(spec, self._stack, self._cleanup_handlers, temp_dir=temp_dir)
            for spec in job.process.input_files
        ]

        info.output_files = [
            prepare_output_file(spec, self._stack, self._cleanup_handlers, temp_dir=temp_dir)
            for spec in job.process.output_files
        ]

        data.update(
            should_transfer_files = 'NO',
            transfer_executable = 'False',
            transfer_input = 'False',
            transfer_output = 'False',
            transfer_error = 'False',
        )

        all_file_descriptors = itertools.chain(info.input_files, info.output_files)
        return {
            f'__file_{file.name}': _path_to_str(file.process_path) for file in all_file_descriptors
        }

    def _prepare_std_files(
        self,
        job: Job.WithParams,
        data: Dict[str, str],
        info: '_JobClusterGenerator._ProcessInfo',
    ) -> None:
        temp_dir = self._htcondor._temp_dir
        if job.process.stdin.connected:
            info.stdin = stdin = prepare_input_file(
                job.process.stdin, self._stack, self._cleanup_handlers, name='stdin',
                temp_dir=temp_dir,
            )
            data['input'] = _path_to_str(info.stdin.open_path)
        if job.process.stdout.capture:
            info.stdout = prepare_output_file(
                job.process.stdout, self._stack, self._cleanup_handlers, name='stdout',
                temp_dir=temp_dir,
            )
            data['output'] = _path_to_str(info.stdout.open_path)
        if job.process.stderr.capture:
            info.stderr = prepare_output_file(
                job.process.stderr, self._stack, self._cleanup_handlers, name='stderr',
                temp_dir=temp_dir,
            )
            data['error'] = _path_to_str(info.stderr.open_path)
        if job.log.capture:
            info.log = prepare_output_file(
                job.log, self._stack, self._cleanup_handlers, name='log',
                temp_dir=temp_dir,
            )
            data['log'] = _path_to_str(info.log.open_path)

    def _prepare_input_file_for_transfer(self, spec: Process.InputFile) -> FileDescriptor:
        temp_dir = self._htcondor._temp_dir
        desc = prepare_input_file(spec, self._stack, self._cleanup_handlers, temp_dir=temp_dir)
        if not desc.temporary:
            if isinstance(desc.open_path, str):
                link_path = _make_temp_link(desc.open_path, temp_dir)
            elif isinstance(desc.open_path, bytes):
                link_path = _make_temp_link(_path_to_str(desc.open_path), temp_dir)
            self._stack.callback(os.unlink, link_path)
            desc = replace(desc, process_path=link_path)
        return desc

    def _prepare_output_file_for_transfer(self, spec: Process.OutputFile) -> FileDescriptor:
        temp_dir = self._htcondor._temp_dir
        desc = prepare_output_file(spec, self._stack, self._cleanup_handlers, temp_dir=temp_dir)
        if not desc.temporary:
            fd, file_path = mkstemp(dir=temp_dir)
            os.close(fd)
            self._stack.callback(os.unlink, file_path)
            desc = replace(desc, process_path=file_path)
        return desc


def _lookup_cmd(cmd: str) -> str:
    cmd_path = cmd if os.path.isabs(cmd) else which(cmd)
    if cmd_path is None:
        raise Exception(f'Failed to locate ("which") command {cmd!r}')
    return cmd_path

def _rand_name_it(length: int=8) -> Iterator[str]:
    c = 'abcdefghijklmnopqrstuvwxyz0123456789_'
    random = Random()

    return (''.join(random.choice(c) for _ in range(length)) for _ in itertools.repeat(None))

def _make_temp_link(
    orig: AnyStr,
    dir: AnyStr,
    prefix: Optional[AnyStr] = None,
    suffix: Optional[AnyStr] = None,
) -> AnyStr:
    """Creates a symlink using a mechanism suitable for a temporary directory.

    The function behaves safely in concurrent scenarios: The ``symlink``
    syscall atomically creates a symlink or fails. Many random names are tried
    before raising an exception if an existing files with the randomised name
    is discovered.

    Adapted from the Python standard library, dropped Windows special case.
    https://github.com/python/cpython/blob/e65b3fa9f16537d20f5f37c25673ac899fcd7099/Lib/tempfile.py#L247

    Args:
        orig: Path which the symlink should point to.
        dir: Directory in which the symlink should be created.
        prefix: Prefix before the random part of the symlink.
        suffix: Suffix after the random part of the symlink.
    """

    names: Iterator[AnyStr]
    pre: AnyStr
    suf: AnyStr
    if isinstance(orig, bytes):
        names = map(os.fsencode, _rand_name_it())
        pre = prefix if prefix is not None else os.fsencode('')
        suf = suffix if suffix is not None else os.fsencode('')
    else:
        names = _rand_name_it()
        pre = prefix if prefix is not None else ''
        suf = suffix if suffix is not None else ''

    files = map(lambda n: os.path.join(dir, pre + n + suf), names)

    for file, _ in zip(files, range(TMP_MAX)):
        try:
            os.symlink(orig, file)
        except FileExistsError:
            continue

        return os.path.abspath(file)

    raise FileExistsError(errno.EEXIST, 'No usable temporary file name found')
