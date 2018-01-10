htchirp
=======

Pure Python Chirp client for HTCondor

Usage
-----

From within an HTCondor ``+WantIOProxy = true`` job::
  
  >>> import htchirp
  >>> chirp = htchirp.HTChirp()
  >>> job_status = chirp.get_job_attr('JobStatus')
  >>> job_status
  '2'

For more commands, see ``help(htchirp.HTChirp)`` and/or
http://research.cs.wisc.edu/htcondor/manual/current/condor_chirp.html
