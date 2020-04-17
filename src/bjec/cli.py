import argparse
import runpy
import sys
from typing import Any, cast, Optional

from .config import config as config_obj


class RunArgs:
	config_path: Optional[str]
	file: str
	name: str


def run(args: RunArgs) -> None:
    if args.config_path is not None:
        config_obj.read_yaml(args.config_path)

    bjec_file_globals = runpy.run_path(args.file)

    bjec_file_globals[args.name]()

parser = argparse.ArgumentParser(description='batch job executor and collector.')
if sys.version_info >= (3, 7): # VERSION_RULE: Python 3.6
	subparsers = parser.add_subparsers(dest='command', required=True)
else:
	subparsers = parser.add_subparsers(dest='command')

parser_record = subparsers.add_parser('run', help='executes a runnable from a bjec definition file.')
parser_record.add_argument('-f', '--file', default='bjec.py', type=str, help='bjec definition file. Defaults to "bjec.py" in the working directory.')
parser_record.add_argument('-c', '--config', type=str, dest='config_path', help='config file in YAML format.')
parser_record.add_argument('name', type=str, help='name of the runnable to execute.')

def main() -> None:
	args = parser.parse_args(sys.argv[1:])

	if args.command == 'run':
		run(cast(RunArgs, args))
	else:
		raise NotImplementedError(f'No command {args.command!r}, see --help for usage info')

if __name__ == '__main__':
	main()
