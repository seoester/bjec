from setuptools import setup, find_packages


with open('README.md') as file:
    long_description = file.read()

setup(
    name = 'bjec',
    version = '0.3.dev1',
    description = 'Batch Job Executor & Collector',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    package_data = {'bjec': ['py.typed']},
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    zip_safe = False,
    python_requires = '>= 3.6, < 4',
    install_requires = [
        'PyYAML',
        'GitPython',
        'typing-extensions',
    ],
    extras_require = {
        'htcondor': ['htcondor'],
    },
    entry_points = {
        'console_scripts': ['bjec=bjec.cli:main'],
    },
)
