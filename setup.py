import sys
from setuptools import setup, find_packages


with open('README.rst') as file:
	long_description = file.read()

install_requires = [
	'argh',
	'pyyaml',
	'gitpython',
]

if sys.version_info < (3, 5):
	install_requires.append("subprocess.run")

setup(
	name="bjec",
	version="0.2.1",
	description="Batch Job Executor & Collector",
	long_description=long_description,
	packages=find_packages(),
	install_requires=install_requires,
	entry_points={
		'console_scripts': ['bjec=bjec.cli:main'],
	},
)
