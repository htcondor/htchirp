from setuptools import setup
import os

wd = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(wd, 'README.md')) as f:
    long_description = f.read()

setup(
    name='htchirp',
    version='2.0',
    description='Pure Python Chirp client for HTCondor',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://htcondor.org',
    author='Jason Patton',
    author_email='jpatton@cs.wisc.edu',
    keywords='htcondor chirp',
    license='ASL 2.0',
    packages=['htchirp'],
    entry_points = {
        'console_scripts': ['condor_htchirp=htchirp.cli:main'],
    },
    project_urls={
        'Bug Reports': 'https://github.com/htcondor/htchirp/issues',
        'Source': 'https://github.com/htcondor/htchirp/',
    },
    zip_safe=False,
)
