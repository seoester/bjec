from setuptools import setup, find_packages


with open('README.rst') as file:
	long_description = file.read()

setup(
	name="bjec",
	version="0.2.4",
	description="Batch Job Executor & Collector",
	long_description=long_description,
	packages=find_packages(),
	install_requires=[
		'argh',
		'pyyaml',
		'gitpython',
	],
	entry_points={
		'console_scripts': ['bjec=bjec.cli:main'],
	},
)
