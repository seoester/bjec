from setuptools import setup, find_packages


with open('README.rst') as file:
    long_description = file.read()

setup(
    name="bjec",
    version="0.2.8",
    description="Batch Job Executor & Collector",
    long_description=long_description,
    package_data = {'simulator': ['py.typed']},
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    zip_safe = False,
    install_requires=[
        'argh',
        'pyyaml',
        'gitpython',
    ],
    entry_points={
        'console_scripts': ['bjec=bjec.cli:main'],
    },
)
