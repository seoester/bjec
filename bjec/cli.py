import runpy
from argh import ArghParser

from .config import config as config_obj

def run(path, name, config=None):
	if config is not None:
		config_obj.read_yaml(config)

	gl = runpy.run_path(path)

	gl[name]()


parser = ArghParser()
parser.add_commands([run])


def main():
	parser.dispatch()
