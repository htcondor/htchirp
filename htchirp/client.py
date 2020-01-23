import sys
import argparse
import re
import htchirp

from inspect import getargspec
from datetime import datetime

# Every callable function not starting with "_" defined here will be a valid pychirp sub-command.
# When defining a new function, please refer to an existing one as a model.
# All functions should implement both interactive and non-interactive parameter input.

# Functions take arguments from the command line when True
interactive = False


def _interactive(custom={}):
    """Makes the function callable from a console.
    
    Args:
        custom (dict, optional): Custom ArgumentParser.add_argument parameters. Defaults to {}.
    
    Returns:
        func: Decorated function.
    """

    # This decorator expects the docstring format used above.
    # The docstring description (i.e., all text from the beginning to the first blank line) will be 
    # used as the console command help.
    # The parameter description under Args will be used as an argument help text. Their types and defaults
    # will not be considered.
    # Any other text below the last parameter description, or in a format different than the one described
    # here, will be ignored.

    def decorator(func):
        def wrapper(*args, **kwargs):
            if interactive:
                # Parser initialization
                parser = argparse.ArgumentParser()
                
                # Define command usage
                parser.prog = "%s %s" % (parser.prog, func.__name__)
                
                # Extract command help based on an available docstring
                if func.__doc__:
                    parser.description = re.split(r"\n\s*\n", func.__doc__)[0]
                
                # Retrieve function signature to build arguments
                args, _, _, defaults = getargspec(func)
                if defaults:
                    defaults = dict(zip(args[-len(defaults):], defaults))
                else:
                    defaults = {}

                # Add arguments to the parser and tries to extract help from an available docstring
                for arg in args:
                    arghelp = None
                    if func.__doc__:  # Extract argument help based on an available docstring
                        argdoc = re.findall(r"%s\s\(.*\)\:\s(.*)" % arg, func.__doc__)
                        if argdoc:
                            arghelp = re.sub(r"\sdefaults\sto\s.*", "", argdoc[0].lower()).strip(".")
                    argname = arg
                    argoptions = {"help": arghelp}
                    if arg in defaults:  # Additional settings for optional arguments
                        argname = "-" + arg
                        if defaults[arg] is False: # Detect flags
                            argoptions["action"] = "store_true"
                        if defaults[arg] is True:
                            argoptions["action"] = "store_false"
                    if arg in custom:  # Custom settings for arguments
                        argoptions.update(custom[arg])
                    parser.add_argument(argname, **argoptions)

                # Parse system args
                parsed_args = vars(parser.parse_args(sys.argv[2:]))
                parsed_args = dict(filter(lambda item: item[1] or item[0] not in defaults, parsed_args.items()))
                
                return func(**parsed_args)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def _print_out(out, level=0):
    """Prints an output formated to a console.
    
    Args:
        out (any): Output to print.
        level (int, optional): Tabulation level. Defaults to 0.
    """
    def to_str(value):
        if type(value) is datetime:
            return value.ctime()
        return str(value)

    prefix = level * "\t"

    if type(out) is list:
        for value in out:
            if type(value) in (list, dict):
                _print_out(value, level + 1)
            else:
                print(prefix + to_str(value))
        return
    
    if type(out) is dict:
        for key, value in out.items():
            if type(value) in (list, dict):
                print(prefix + to_str(key))
                _print_out(value, level + 1)
            else:
                print(prefix + "%s: %s" % (to_str(key), to_str(value)))
        return

    print(prefix + to_str(out))


@_interactive()
def fetch(remote_file, local_file):
    """Copy the remote_file from the submit machine to the execute machine, naming it local_file.
    
    Args:
        remote_file (str, optional): File on submit machine.
        local_file (str, optional): File on execute machine.
    
    Returns:
        int: Bytes written
    """

    with htchirp.HTChirp() as chirp:
        return chirp.fetch(remote_file, local_file)


@_interactive()
def put(remote_file, local_file, mode="wct", perm=None):
    """Copy the local_file from the execute machine to the submit machine, naming it remote_file.
       The optional perm argument describes the file access permissions in a Unix format.
       The optional mode argument is one or more of the following characters describing the remote_file file:
       w, open for writing; a, force all writes to append; t, truncate before use;
       c, create the file, if it does not exist; x, fail if c is given and the file already exists.
    
    Args:
        remote_file (str, optional): File on submit machine.
        local_file (str, optional): File on execute machine.
        mode (str, optional): File open modes (one or more of 'watcx'). Defaults to 'wct'.
            w, open for writing;
            a, force all writes to append;
            t, truncate before use;
            c, create the file, if it does not exist;
            x, fail if c is given and the file already exists. Defaults to None.
        perm (str, optional): Describes the file access permissions in a Unix format. Defaults to None.
    """
    opt_params = {}
    if mode:
        # Add "w" to along with the following characters to reproduce condor_chirp behavior
        for c in "act":
            if c in mode:
                mode += "w"
                break
        opt_params["flags"] = mode
    if perm:
        opt_params["mode"] = perm

    with htchirp.HTChirp() as chirp:
        chirp.put(remote_file, local_file, mode, perm)


@_interactive()
def remove(remote_file):
    """Remove the remote_file file from the submit machine.
    
    Args:
        remote_file (str, optional): File on submit machine.
    """

    with htchirp.HTChirp() as chirp:
        chirp.remove(remote_file)


@_interactive()
def get_job_attr(job_attribute):
    """Prints the named job ClassAd attribute to standard output.
    
    Args:
        job_attribute (str, optional): Job ClassAd attribute.
    
    Returns:
        str: The value of the job attribute as a string.
    """

    with htchirp.HTChirp() as chirp:
        return chirp.get_job_attr(job_attribute)


@_interactive()
def set_job_attr(job_attribute, attribute_value):
    """Sets the named job ClassAd attribute with the given attribute value.
    
    Args:
        job_attribute (str): Job ClassAd attribute.
        attribute_value (str): Job ClassAd value.
    """

    with htchirp.HTChirp() as chirp:
        chirp.set_job_attr(job_attribute, attribute_value)


@_interactive()
def get_job_attr_delayed(job_attribute):
    """Prints the named job ClassAd attribute to standard output, potentially reading the cached value
       from a recent set_job_attr_delayed.
    
    Args:
        job_attribute (str, optional): Job ClassAd attribute.
    
    Returns:
        str: The value of the job attribute as a string.
    """

    with htchirp.HTChirp() as chirp:
        return chirp.get_job_attr_delayed(job_attribute)


@_interactive()
def set_job_attr_delayed(job_attribute, attribute_value):
    """Sets the named job ClassAd attribute with the given attribute value, but does not immediately
       synchronize the value with the submit side. It can take 15 minutes before the synchronization occurs.
       This has much less overhead than the non delayed version. With this option, jobs do not need ClassAd
       attribute WantIOProxy set. With this option, job attribute names are restricted to begin with the case
       sensitive substring Chirp. 
    
    Args:
        job_attribute (str): Job ClassAd attribute.
        attribute_value (str): Job ClassAd value.
    """

    with htchirp.HTChirp() as chirp:
        chirp.set_job_attr_delayed(job_attribute, attribute_value)


@_interactive()
def ulog(text):
    """Appends Message to the job event log.
    
    Args:
        text (str): Message to log.
    """

    with htchirp.HTChirp() as chirp:
        chirp.ulog(text)


@_interactive()
def phase(phasestring):
    """Tell HTCondor that the job is changing phases.
    
    Args:
        phasestring (str): New phase.
    """

    with htchirp.HTChirp() as chirp:
        chirp.phase(phasestring)


@_interactive({"stride":{"nargs": 2, "metavar": ("LENGTH", "SKIP")}})
def read(remote_file, length, offset=None, stride=(None, None)):
    """Read length bytes from remote_file. Optionally, implement a stride by starting the read at offset
       and reading stride(length) bytes with a stride of stride(skip) bytes.
    
    Args:
        remote_file (str): File on the submit machine.
        length (int): Number of bytes to read.
        offset (int, optional): Number of bytes to offset from beginning of file. Defaults to None.
        stride (tuple, optional): Number of bytes to read followed by number of bytes to skip per stride. Defaults to (None, None).
    
    Returns:
        str: Data read from file
    """

    with htchirp.HTChirp() as chirp:
        return chirp.read(remote_file, length, offset, stride[0], stride[1])


@_interactive({"length": {"nargs": "?"}, "stride":{"nargs": 2, "metavar": ("LENGTH", "SKIP")}})
def write(remote_file, local_file, length, offset=None, stride=(None, None)):
    """Write the contents of local_file to remote_file. Optionally, start writing to the remote file at offset
       and write stride(length) bytes with a stride of stride(skip) bytes. If the optional length follows
       local_file, then the write will halt after length input bytes have been written. Otherwise, the entire
       contents of local_file will be written.
    
    Args:
        remote_file (str): File on the submit machine.
        local_file (str): File on execute machine.
        length (int): Number of bytes to write.
        offset (int, optional): Number of bytes to offset from beginning of file. Defaults to None.
        stride (tuple, optional): Number of bytes to read followed by number of bytes to skip per stride. Defaults to (None, None).
    """

    data = open(local_file).read()

    with htchirp.HTChirp() as chirp:
        chirp.write(data, remote_file, length=length, offset=offset, stride_length=stride[0], stride_skip=stride[1])


@_interactive()
def rmdir(remotepath, r=False):
    """Delete the directory specified by RemotePath. If the optional -r is specified, recursively delete the entire directory.
    
    Args:
        remotepath (str): Path to directory on the submit machine.
        r (bool, optional): Recursively delete remotepath. Defaults to False.
    """

    with htchirp.HTChirp() as chirp:
        chirp.rmdir(remotepath, r)


@_interactive()
def getdir(remotepath, l=False):
    """List the contents of the directory specified by RemotePath.
    
    Args:
        remotepath (str): Path to directory on the submit machine.
        l (bool, optional): Returns a dict of file metadata. Defaults to False.
    
    Returns:
        list: List of files, when l is False.
        dict: Dictionary of files with their metadata, when l is True.
    """

    with htchirp.HTChirp() as chirp:
        out = chirp.getdir(remotepath, l)

    for item in out:
        for key in ["atime", "mtime", "ctime"]:
            out[item][key] = datetime.fromtimestamp(out[item][key])

    return out


@_interactive()
def whoami():
    """Get the user's current identity.
    
    Returns:
        str: The user's identity.
    """

    with htchirp.HTChirp() as chirp:
        return chirp.whoami()


@_interactive()
def whoareyou(remotepath):
    """Get the identity of RemoteHost.
    
    Args:
        remotepath (str): Remote host
    
    Returns:
        str: The server's identity
    """

    with htchirp.HTChirp() as chirp:
        return chirp.whoareyou(remotepath)


@_interactive()
def link(oldpath, newpath, s=False):
    """Create a hard link from OldRemotePath to NewRemotePath.
    
    Args:
        oldpath (str): File path to link from on the submit machine.
        newpath (str): File path to link to on the submit machine.
        s (bool, optional): Create a symbolic link instead. Defaults to False.
    """

    with htchirp.HTChirp() as chirp:
        chirp.link(oldpath, newpath, s)


@_interactive({"remotepath": {"nargs": "+"}})
def readlink(remotepath):
    """Read the contents of the file defined by the symbolic link remotepath.
    
    Args:
        remotepath (str): File path to link on the submit machine.
    
    Returns:
        str: Contents of the link.
    """

    with htchirp.HTChirp() as chirp:
        return chirp.readlink(remotepath[0]).decode()


@_interactive()
def stat(remotepath):
    """Get metadata for remotepath. Examines the target, if it is a symbolic link.
    
    Args:
        remotepath (str): File path to link on the submit machine.
    
    Returns:
        dict: Dict of file metadata.
    """

    with htchirp.HTChirp() as chirp:
        out = chirp.stat(remotepath)
    
    for key in ["atime", "mtime", "ctime"]:
        out[key] = datetime.fromtimestamp(out[key])

    return out


@_interactive()
def lstat(remotepath):
    """Get metadata for remotepath. Examines the file, if it is a symbolic link.
    
    Args:
        remotepath (str): File path to link on the submit machine.
    
    Returns:
        dict: Dict of file metadata.
    """

    with htchirp.HTChirp() as chirp:
        out = chirp.lstat(remotepath)
    
    for key in ["atime", "mtime", "ctime"]:
        out[key] = datetime.fromtimestamp(out[key])

    return out


@_interactive()
def statfs(remotepath):
    """Get file system metadata for remotepath.
    
    Args:
        remotepath (str): File path to link on the submit machine.
    
    Returns:
        dict: Dict of filesystem metadata.
    """

    with htchirp.HTChirp() as chirp:
        return chirp.statfs(remotepath)


@_interactive()
def access(remotepath, mode):
    """Check access permissions for RemotePath. Mode is one or more of the characters
       r, w, x, or f, representing read, write, execute, and existence, respectively.
    
    Args:
        remotepath (str): Path to examine on the submit machine.
        mode (str): Mode to check (one or more of 'rwxf').
    """

    with htchirp.HTChirp() as chirp:
        chirp.access(remotepath, mode)


@_interactive()
def chmod(remotepath, mode):
    """Change the permissions of remotepath to mode. mode describes the file access
       permissions in a Unix format; 660 is an example Unix format.
    
    Args:
        remotepath (str): Target path on the submit machine.
        mode (str): Permission mode to set
    """

    with htchirp.HTChirp() as chirp:
        chirp.chmod(remotepath, int(mode, 8))


@_interactive()
def chown(remotepath, uid, gid):
    """Change the ownership of remotepath to uid and gid. Changes the target of remotepath, if it is a symbolic link.
    
    Args:
        remotepath (str): File on the submit machine.
        uid (int): User's UID.
        gid (int): User's GID.
    """

    with htchirp.HTChirp() as chirp:
        chirp.chown(remotepath, uid, gid)


@_interactive()
def lchown(remotepath, uid, gid):
    """Change the ownership of remotepath to uid and gid. Changes the link, if remotepath is a symbolic link.
    
    Args:
        remotepath (str): File on the submit machine.
        uid (int): User's UID.
        gid (int): User's GID.
    """

    with htchirp.HTChirp() as chirp:
        chirp.lchown(remotepath, uid, gid)


@_interactive()
def truncate(remotepath, length):
    """Truncates the file at remotepath to length bytes.
    
    Args:
        remotepath (str): File on the submit machine.
        length (int): Truncated length.
    """

    with htchirp.HTChirp() as chirp:
        chirp.truncate(remotepath, length)


@_interactive()
def utime(remotepath, actime, mtime):
    """Change the access to actime and modification time to mtime of remotepath.
    
    Args:
        remotepath (str): Target path on the submit machine.
        actime (int): Access time, in seconds (Unix epoch).
        mtime (int): Modification time, in seconds (Unix epoch).
    """

    with htchirp.HTChirp() as chirp:
        chirp.utime(remotepath, actime, mtime)


def main():
    # Help text
    description = "Drop-in replacement of condor_chirp in Pure Python"
    usage = "pychirp.py [-h] command [args]"
    epilog = ("commands:\n"
              "  fetch remote_file local_file\n"
              "  put [-mode mode] [-perm perm] local_file remote_file\n"
              "  remove remote_file\n"
              "  get_job_attr job_attribute\n"
              "  get_job_attr_delayed job_attribute\n"
              "  set_job_attr job_attribute attribute_value\n"
              "  set_job_attr_delayed job_attribute attribute_value\n"
              "  ulog text\n"
              "  phase phasestring\n"
              "  read [-offset offset] [-stride length skip] remote_file length\n"
              "  write [-offset remote_offset] [-stride length skip] remote_file local_file\n"
              "  rmdir [-r] remotepath\n"
              "  getdir [-l] remotepath\n"
              "  whoami\n"
              "  whoareyou remotepath\n"
              "  link [-s] oldpath newpath\n"
              "  readlink remotepath\n"
              "  stat remotepath\n"
              "  lstat remotepath\n"
              "  statfs remotepath\n"
              "  access remotepath mode(rwxf)\n"
              "  chmod remotepath mode\n"
              "  chown remotepath uid gid\n"
              "  lchown remotepath uid gid\n"
              "  truncate remotepath length\n"
              "  utime remotepath actime mtime")

    # Handle command line arguments
    parser = argparse.ArgumentParser()
    parser.description = description
    parser.usage = usage
    parser.epilog = epilog
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.add_argument("command", help="one of the commands listed below")
    args = parser.parse_args(sys.argv[1:2])

    # Call the command function
    if args.command in dir() and not args.command.startswith("_") and callable(eval(args.command)):
        interactive = True
        response = eval(args.command)()
        if response is not None:
            _print_out(response)
    else:
        print("error: command not implemented")


if __name__ == "__main__":
    main()