from bjec import csv, json
from bjec.params import Join, P, String
from bjec.job import job, Job
from bjec.io import WriteableFromPath
from bjec.htcondor import HTCondor, Job as HTCondorJob
from bjec.process import Environment, Process, DeferredResult
from bjec.generator import Literal, Matrix
from bjec.collector import Concatenate, Convert, Multi, Noop

@job()
def test_simple(j: Job.Constructor) -> None:
    j.generator = Matrix(i=range(3))

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('sleep').args('30')
        .build()
    )

    j.collector = Noop()

@job()
def test_echo(j: Job.Constructor) -> None:
    j.generator = Matrix(i=range(3))

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('echo').args(P('i'))
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_echo_output.txt', before=Join(P('i').transform(str), ':\n'))),
        Convert(DeferredResult.stderr, Concatenate(path='test_echo_errput.txt', before=Join(P('i').transform(str), ':\n'))),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_cat_file(j: Job.Constructor) -> None:
    j.generator = Matrix(i=[1, 2, 3])

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('cat').args(P('__file_inputstring'))
        .add_input_file('inputstring', String('i = {i}\n'))
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_cat_file_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_cat_file_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_tar(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('tar').args('-cf', P('__file_tararchive'), P('__file_inp'))
        .add_input_file('inp', source=WriteableFromPath('bjec.py'))
        .add_output_file('tararchive')
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_tar_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_tar_errput.txt')),
        Convert(DeferredResult.output_file('tararchive'), Concatenate(path='complete.tar')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_tar_path(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('tar').args('-cf', P('__file_tararchive'), P('__file_inp'))
        .add_input_file('inp', source=WriteableFromPath('bjec.py'))
        .add_output_file('tararchive', path='complete_path.tar', must_not_exist=False)
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_tar_path_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_tar_path_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_env_quote(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .environment(Environment.Fluid()
            .set(
                A = 'a and b',
                B = '"',
                C = '"..." said he',
                D = '\'',
                E = '\'...\' said she',
                K = '',
            )
            .set()
            .build()
        )
        .cmd('env')
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_env_quote_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_env_quote_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))


@job()
def test_arg_quote(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('bash').args('-c', 'echo $#; echo $1; echo $2; echo $3; echo $4; echo $5;', '--', 'a and b', '"', '"..." said he', '\'', '\'...\' said she', '')
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_arg_quote_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_arg_quote_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_remap_escape(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (Process.Fluid()
        .cmd('cp').args(P('__file_inp'), P('__file_out'))
        .add_input_file('inp', String('i = {i}\n'))
        # Test remap escaping via amending htcondor.py code:
        # mkstemp(prefix='p;=') in _prepare_output_file_for_transfer
        .add_output_file('out', path='test_remap_escape_;=output_file.txt')
        .capture_stdout()
        .capture_stderr()
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_remap_escape_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_remap_escape_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))

@job()
def test_exit_code(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    j.runnable = (HTCondorJob.Fluid()
        .process(Process.Fluid()
            .cmd('bash').args('-c', 'exit 13')
            .build(),
        )
        .transfer_files(transfer=False)
        .capture_log(
            path = 'test_exit_code_log.txt',
        )
        .build()
    )

    j.collector = Noop()

@job()
def test_abs_std_paths(j: Job.Constructor) -> None:
    j.generator = Literal(i=1)

    j.processor = HTCondor()

    import os
    import shutil
    import tempfile
    dir_path = tempfile.mkdtemp()
    iwd_path = tempfile.mkdtemp()

    j.runnable = (HTCondorJob.Fluid()
        .process(Process.Fluid()
            .cmd('echo').args('some_str')
            .capture_stdout(path=f'{dir_path}/output.txt')
            .capture_stderr(path=f'{dir_path}/errput.txt')
            .build()
        )
        .transfer_files(transfer=False)
        .commands({
            # YES leads to stdout and stderr being placed in the iwd if
            # transfer_error and transfer_output are False
            'should_transfer_files': 'YES',
            'transfer_error': 'True',
            'transfer_output': 'True',
            'initialdir': iwd_path,
        })
        .build()
    )

    coll = j.collector = Multi(
        Convert(DeferredResult.stdout, Concatenate(path='test_abs_std_paths_output.txt')),
        Convert(DeferredResult.stderr, Concatenate(path='test_abs_std_paths_errput.txt')),
    )

    j.after(lambda _: print([c.collector.path for c in coll.collectors]))
    j.after(lambda _: shutil.rmtree(dir_path))
    j.after(lambda _: print([os.path.join(iwd_path, f) for f in os.listdir(iwd_path)]))
    j.after(lambda _: shutil.rmtree(iwd_path))
