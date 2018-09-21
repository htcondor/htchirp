htchirp
=======

Pure Python Chirp client for HTCondor

Installation
---------

The latest release is available on PyPI

``pip install htchirp``

Example Usage
------------

Using `with` syntax (**recommended**)::
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
For a broader explanation of ``condor_chirp``, see 
http://research.cs.wisc.edu/htcondor/manual/current/condor_chirp.html
