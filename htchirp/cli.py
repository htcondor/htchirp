import sys
from htchirp import condor_chirp

def main():
    return condor_chirp(sys.argv[1:], True)
