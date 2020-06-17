# HTChirp

Pure Python Chirp client for HTCondor

## Installation

The latest release is available on PyPI:

`pip install htchirp`

However, if HTCondor job sandbox space is a premium, most of HTChirp's
functionality can be accessed from [`htchirp.py`](htchirp/htchirp.py)
as a standalone script or module.

## Example Usage

There are multiple ways to invoke HTChirp inside a HTCondor job
environment.

### Using HTChirp inside Python
First, you can use an HTChirp object as part of a larger Python workflow
connect to the Chirp server and issue commands:

Using context management (**recommended**):
```python
>>> import htchirp
>>> with htchirp.HTChirp() as chirp:
>>>     chirp.ulog('Logging use of Chirp in Python')
>>>     me = chirp.whoami()
>>>     chirp.set_job_attr('UsingPythonChirp', 'True')
>>>     using_chirp = chirp.get_job_attr('UsingPythonChirp')
>>> me
'CONDOR'
>>> using_chirp
'true'
```

Using manual connection and disconnection (*not recommended*):
```python
>>> import htchirp
>>> chirp = htchirp.HTChirp()
>>> chirp.connect()
>>> chirp.write('Important output 1\n', '/tmp/my-job-output', 'cwa')
19
>>> chirp.write('Important output 2\n', '/tmp/my-job-output', 'cwa')
19
>>> chirp.read('/tmp/my-job-output', 1024)
'Important output 1\nImportant output 2\n'
>>> chirp.fetch('/tmp/my-job-output', './my-job-output')
38
>>> chirp.disconnect()
```

For more information on the available commands, see `help(htchirp.HTChirp)`.


### Using HTChirp on the command line
Second, you can use HTChirp on the command line with the same commands
and arguments supported by the
[`condor_chirp`](https://htcondor.readthedocs.io/en/latest/man-pages/condor_chirp.html)
utility, either by including
`htchirp.py` with your job or by installing the HTChirp package inside a
virtual environment inside your job.

Using `htchirp.py` from within the working directory:
```
$ python htchirp.py ulog "Logging use of Chirp in Python"
$ python htchirp.py whoami
CONDOR
$ python htchirp.py set_job_attr UsingPythonChirp True
$ python htchirp.py get_job_attr UsingPythonChirp
True
```

Using `condor_htchirp` after installing HTChirp in an active virtual
environment:
```
$ condor_htchirp ulog "Logging use of Chirp in Python"
$ condor_htchirp whoami
CONDOR
$ condor_htchirp set_job_attr UsingPythonChirp True
$ condor_htchirp get_job_attr UsingPythonChirp
True
```

Using `python -m htchirp` after installing HTChirp in an active
virtual environment:
```
$ python -m htchirp ulog "Logging use of Chirp in Python"
$ python -m htchirp whoami
CONDOR
$ python -m htchirp set_job_attr UsingPythonChirp True
$ python -m htchirp get_job_attr UsingPythonChirp
True
```

For a list of commands and arguments, pass `--help` to your preferred
command line invokation, or see the
[`condor_chirp` man page](https://htcondor.readthedocs.io/en/latest/man-pages/condor_chirp.html).

