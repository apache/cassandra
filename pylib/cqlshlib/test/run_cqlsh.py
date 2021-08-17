# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTE: this testing tool is *nix specific

from __future__ import unicode_literals

import os
import sys
import re
import contextlib
import subprocess
import signal
import math
from time import time
from . import basecase
from os.path import join, normpath


def is_win():
    return sys.platform in ("cygwin", "win32")

if is_win():
    from .winpty import WinPty
    DEFAULT_PREFIX = ''
else:
    import pty
    DEFAULT_PREFIX = os.linesep

DEFAULT_CQLSH_PROMPT = DEFAULT_PREFIX + '(\S+@)?cqlsh(:\S+)?> '
DEFAULT_CQLSH_TERM = 'xterm'

try:
    Pattern = re._pattern_type
except AttributeError:
    # Python 3.7+
    Pattern = re.Pattern

def get_smm_sequence(term='xterm'):
    """
    Return the set meta mode (smm) sequence, if any.
    On more recent Linux systems, xterm emits the smm sequence
    before each prompt.
    """
    result = ''
    if not is_win():
        tput_proc = subprocess.Popen(['tput', '-T{}'.format(term), 'smm'], stdout=subprocess.PIPE)
        tput_stdout = tput_proc.communicate()[0]
        if (tput_stdout and (tput_stdout != b'')):
            result = tput_stdout
            if isinstance(result, bytes):
                result = result.decode("utf-8")
    return result

DEFAULT_SMM_SEQUENCE = get_smm_sequence()

cqlshlog = basecase.cqlshlog

def set_controlling_pty(master, slave):
    os.setsid()
    os.close(master)
    for i in range(3):
        os.dup2(slave, i)
    if slave > 2:
        os.close(slave)
    os.close(os.open(os.ttyname(1), os.O_RDWR))

@contextlib.contextmanager
def raising_signal(signum, exc):
    """
    Within the wrapped context, the given signal will interrupt signal
    calls and will raise the given exception class. The preexisting signal
    handling will be reinstated on context exit.
    """
    def raiser(signum, frames):
        raise exc()
    oldhandlr = signal.signal(signum, raiser)
    try:
        yield
    finally:
        signal.signal(signum, oldhandlr)

class TimeoutError(Exception):
    pass

@contextlib.contextmanager
def timing_out_itimer(seconds):
    if seconds is None:
        yield
        return
    with raising_signal(signal.SIGALRM, TimeoutError):
        oldval, oldint = signal.getitimer(signal.ITIMER_REAL)
        if oldval != 0.0:
            raise RuntimeError("ITIMER_REAL already in use")
        signal.setitimer(signal.ITIMER_REAL, seconds)
        try:
            yield
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)

@contextlib.contextmanager
def timing_out_alarm(seconds):
    if seconds is None:
        yield
        return
    with raising_signal(signal.SIGALRM, TimeoutError):
        oldval = signal.alarm(int(math.ceil(seconds)))
        if oldval != 0:
            signal.alarm(oldval)
            raise RuntimeError("SIGALRM already in use")
        try:
            yield
        finally:
            signal.alarm(0)

if is_win():
    try:
        import eventlet
    except ImportError as e:
        sys.exit("evenlet library required to run cqlshlib tests on Windows")

    def timing_out(seconds):
        return eventlet.Timeout(seconds, TimeoutError)
else:
    # setitimer is new in 2.6, but it's still worth supporting, for potentially
    # faster tests because of sub-second resolution on timeouts.
    if hasattr(signal, 'setitimer'):
        timing_out = timing_out_itimer
    else:
        timing_out = timing_out_alarm

def noop(*a):
    pass

class ProcRunner:
    def __init__(self, path, tty=True, env=None, args=()):
        self.exe_path = path
        self.args = args
        self.tty = bool(tty)
        self.realtty = self.tty and not is_win()
        if env is None:
            env = {}
        self.env = env
        self.readbuf = ''

        self.start_proc()

    def start_proc(self):
        preexec = noop
        stdin = stdout = stderr = None
        cqlshlog.info("Spawning %r subprocess with args: %r and env: %r"
                      % (self.exe_path, self.args, self.env))
        if self.realtty:
            masterfd, slavefd = pty.openpty()
            preexec = (lambda: set_controlling_pty(masterfd, slavefd))
            self.proc = subprocess.Popen((self.exe_path,) + tuple(self.args),
                                         env=self.env, preexec_fn=preexec,
                                         stdin=stdin, stdout=stdout, stderr=stderr,
                                         close_fds=False)
            os.close(slavefd)
            self.childpty = masterfd
            self.send = self.send_tty
            self.read = self.read_tty
        else:
            stdin = stdout = subprocess.PIPE
            stderr = subprocess.STDOUT
            self.proc = subprocess.Popen((self.exe_path,) + tuple(self.args),
                                         env=self.env, stdin=stdin, stdout=stdout,
                                         stderr=stderr, bufsize=0, close_fds=False)
            self.send = self.send_pipe
            if self.tty:
                self.winpty = WinPty(self.proc.stdout)
                self.read = self.read_winpty
            else:
                self.read = self.read_pipe

    def close(self):
        cqlshlog.info("Closing %r subprocess." % (self.exe_path,))
        if self.realtty:
            os.close(self.childpty)
        else:
            self.proc.stdin.close()
        cqlshlog.debug("Waiting for exit")
        return self.proc.wait()

    def send_tty(self, data):
        if not isinstance(data, bytes):
            data = data.encode("utf-8")
        os.write(self.childpty, data)

    def send_pipe(self, data):
        self.proc.stdin.write(data)

    def read_tty(self, blksize, timeout=None):
        buf = os.read(self.childpty, blksize)
        if isinstance(buf, bytes):
            buf = buf.decode("utf-8")
        return buf

    def read_pipe(self, blksize, timeout=None):
        buf = self.proc.stdout.read(blksize)
        if isinstance(buf, bytes):
            buf = buf.decode("utf-8")
        return buf

    def read_winpty(self, blksize, timeout=None):
        buf = self.winpty.read(blksize, timeout)
        if isinstance(buf, bytes):
            buf = buf.decode("utf-8")
        return buf

    def read_until(self, until, blksize=4096, timeout=None,
                   flags=0, ptty_timeout=None, replace=[]):
        if not isinstance(until, Pattern):
            until = re.compile(until, flags)

        cqlshlog.debug("Searching for %r" % (until.pattern,))
        got = self.readbuf
        self.readbuf = ''
        with timing_out(timeout):
            while True:
                val = self.read(blksize, ptty_timeout)
                for replace_target in replace:
                    if (replace_target != ''):
                        val = val.replace(replace_target, '')
                cqlshlog.debug("read %r from subproc" % (val,))
                if val == '':
                    raise EOFError("'until' pattern %r not found" % (until.pattern,))
                got += val
                m = until.search(got)
                if m is not None:
                    self.readbuf = got[m.end():]
                    got = got[:m.end()]
                    return got

    def read_lines(self, numlines, blksize=4096, timeout=None):
        lines = []
        with timing_out(timeout):
            for n in range(numlines):
                lines.append(self.read_until('\n', blksize=blksize))
        return lines

    def read_up_to_timeout(self, timeout, blksize=4096):
        got = self.readbuf
        self.readbuf = ''
        curtime = time()
        stoptime = curtime + timeout
        while curtime < stoptime:
            try:
                with timing_out(stoptime - curtime):
                    stuff = self.read(blksize)
            except TimeoutError:
                break
            cqlshlog.debug("read %r from subproc" % (stuff,))
            if stuff == '':
                break
            got += stuff
            curtime = time()
        return got

class CqlshRunner(ProcRunner):
    def __init__(self, path=None, host=None, port=None, keyspace=None, cqlver=None,
                 args=(), prompt=DEFAULT_CQLSH_PROMPT, env=None,
                 win_force_colors=True, tty=True, **kwargs):
        if path is None:
            path = join(basecase.cqlsh_dir, 'cqlsh')
        if host is None:
            host = basecase.TEST_HOST
        if port is None:
            port = basecase.TEST_PORT
        if env is None:
            env = {}
        if is_win():
            env['PYTHONUNBUFFERED'] = '1'
            env.update(os.environ.copy())
        env.setdefault('TERM', 'xterm')
        env.setdefault('CQLSH_NO_BUNDLED', os.environ.get('CQLSH_NO_BUNDLED', ''))
        env.setdefault('PYTHONPATH', os.environ.get('PYTHONPATH', ''))
        coverage = False
        if ('CQLSH_COVERAGE' in env.keys()):
            coverage = True
        args = tuple(args) + (host, str(port))
        if cqlver is not None:
            args += ('--cqlversion', str(cqlver))
        if keyspace is not None:
            args += ('--keyspace', keyspace.lower())
        if tty and is_win():
            args += ('--tty',)
            args += ('--encoding', 'utf-8')
            if win_force_colors:
                args += ('--color',)
        if coverage:
            args += ('--coverage',)
        self.keyspace = keyspace
        env.setdefault('CQLSH_PYTHON', sys.executable)  # run with the same interpreter as the test
        ProcRunner.__init__(self, path, tty=tty, args=args, env=env, **kwargs)
        self.prompt = prompt
        if self.prompt is None:
            self.output_header = ''
        else:
            self.output_header = self.read_to_next_prompt()

    def read_to_next_prompt(self, timeout=10.0):
        return self.read_until(self.prompt, timeout=timeout, ptty_timeout=3, replace=[DEFAULT_SMM_SEQUENCE,])

    def read_up_to_timeout(self, timeout, blksize=4096):
        output = ProcRunner.read_up_to_timeout(self, timeout, blksize=blksize)
        # readline trying to be friendly- remove these artifacts
        output = output.replace(' \r', '')
        output = output.replace('\r', '')
        return output

    def cmd_and_response(self, cmd):
        self.send(cmd + '\n')
        output = self.read_to_next_prompt()
        # readline trying to be friendly- remove these artifacts
        output = output.replace(' \r', '')
        output = output.replace('\r', '')
        output = output.replace(' \b', '')
        if self.realtty:
            echo, output = output.split('\n', 1)
            assert echo == cmd, "unexpected echo %r instead of %r" % (echo, cmd)
        try:
            output, promptline = output.rsplit('\n', 1)
        except ValueError:
            promptline = output
            output = ''
        assert re.match(self.prompt, DEFAULT_PREFIX + promptline), \
                'last line of output %r does not match %r?' % (promptline, self.prompt)
        return output + '\n'

def run_cqlsh(**kwargs):
    return contextlib.closing(CqlshRunner(**kwargs))

def call_cqlsh(**kwargs):
    kwargs.setdefault('prompt', None)
    proginput = kwargs.pop('input', '')
    kwargs['tty'] = False
    c = CqlshRunner(**kwargs)
    output, _ = c.proc.communicate(proginput)
    result = c.close()
    if isinstance(output, bytes):
        output = output.decode("utf-8")
    return output, result
