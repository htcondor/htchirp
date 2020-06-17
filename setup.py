from setuptools import setup

setup(
    name='htchirp',
    version='1.0',
    description='Pure Python Chirp client for HTCondor',
    keywords='htcondor chirp',
    url='https://github.com/htcondor/htchirp',
    author='Jason Patton',
    author_email='jpatton@cs.wisc.edu',
    license='ASL 2.0',
    packages=['htchirp'],
    entry_points = {
        'console_scripts': ['condor_htchirp=htchirp.cli:main'],
    },
    zip_safe=False
)
