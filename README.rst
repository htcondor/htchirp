htchirp
=======

Pure Python Chirp client for HTCondor

Example Usage
------------

From within an HTCondor ``+WantIOProxy = true`` job::
  
  >>> import htchirp
  >>> chirp = htchirp.HTChirp()
  >>> chirp.whoami()
  'CONDOR'
  >>> chirp.set_job_attr('UsingPythonChirp', 'True')
  >>> chirp.get_job_attr('UsingPythonChirp')
  'true'
  >>> chirp.write('Important output 1\n', '/tmp/my-job-output', 'cwa')
  19
  >>> chirp.write('Important output 2\n', '/tmp/my-job-output', 'cwa')
  19
  >>> chirp.read('/tmp/my-job-output', 1024)
  'Important output 1\nImportant output 2\n'
  >>> chirp.fetch('/tmp/my-job-output', './my-job-output')
  38
  >>> open('./my-job-output').read()
  'Important output 1\nImportant output 2\n'
  >>> chirp.ulog('Logging use of Chirp in Python')

For more commands, see ``help(htchirp.HTChirp)``.
For a broader explanation of ``condor_chirp``, see 
http://research.cs.wisc.edu/htcondor/manual/current/condor_chirp.html
