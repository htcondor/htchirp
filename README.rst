htchirp
=======

Pure Python Chirp client for HTCondor

Installation
---------

The latest release is available on PyPI

``pip install htchirp``

Example Usage
------------

There are multiple ways to invoke HTChirp inside a HTCondor job
environment.

First, you can use an HTChirp object as part of a larger Python workflow
connect to the Chirp server and issue commands:

Using context management (**recommended**)::
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

Using manual connection and disconnection (**not recommended**)::
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
  
For more commands, see ``help(htchirp.HTChirp)``.


Second, you can use HTChirp on the command-line with the same commands
and arguments supported by ``condor_chirp``, either by including
``htchirp.py`` with your job or by installing the HTChirp package inside a
virtual environment inside your job.

Using ``htchirp.py`` from the working directory::
  $ python htchirp.py ulog "Logging use of Chirp in Python"
  $ python htchirp.py whoami
  CONDOR
  $ python htchirp.py set_job_attr UsingPythonChirp True
  $ python htchirp.py get_job_attr UsingPythonChirp
  True

Using the ``condor_htchirp`` entrypoint after installing HTChirp in an
active virtual environment::
  $ condor_htchirp ulog "Logging use of Chirp in Python"
  $ condor_htchirp whoami
  CONDOR
  $ condor_htchirp set_job_attr UsingPythonChirp True
  $ condor_htchirp get_job_attr UsingPythonChirp
  True

Using ``python -m htchirp`` after installing HTChirp in an active
virtual environment::
  $ python -m htchirp ulog "Logging use of Chirp in Python"
  $ python -m htchirp whoami
  CONDOR
  $ python -m htchirp set_job_attr UsingPythonChirp True
  $ python -m htchirp get_job_attr UsingPythonChirp
  True

For a list of commands and arguments, pass ``-h`` to your preferred
command-line invokation, or see https://htcondor.readthedocs.io/en/latest/man-pages/condor_chirp.html

