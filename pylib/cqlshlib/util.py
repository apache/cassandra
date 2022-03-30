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


import cProfile
import codecs
import pstats
import os
import errno
import stat

from datetime import timedelta, tzinfo
from io import StringIO

try:
    from line_profiler import LineProfiler
    HAS_LINE_PROFILER = True
except ImportError:
    HAS_LINE_PROFILER = False

ZERO = timedelta(0)


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


def split_list(items, pred):
    """
    Split up a list (or other iterable) on the elements which satisfy the
    given predicate 'pred'. Elements for which 'pred' returns true start a new
    sublist for subsequent elements, which will accumulate in the new sublist
    until the next satisfying element.

    >>> split_list([0, 1, 2, 5, 99, 8], lambda n: (n % 2) == 0)
    [[0], [1, 2], [5, 99, 8], []]
    """

    thisresult = []
    results = [thisresult]
    for i in items:
        thisresult.append(i)
        if pred(i):
            thisresult = []
            results.append(thisresult)
    return results


def find_common_prefix(strs):
    """
    Given a list (iterable) of strings, return the longest common prefix.

    >>> find_common_prefix(['abracadabra', 'abracadero', 'abranch'])
    'abra'
    >>> find_common_prefix(['abracadabra', 'abracadero', 'mt. fuji'])
    ''
    """

    common = []
    for cgroup in zip(*strs):
        if all(x == cgroup[0] for x in cgroup[1:]):
            common.append(cgroup[0])
        else:
            break
    return ''.join(common)


def list_bifilter(pred, iterable):
    """
    Filter an iterable into two output lists: the first containing all
    elements of the iterable for which 'pred' returns true, and the second
    containing all others. Order of the elements is otherwise retained.

    >>> list_bifilter(lambda x: isinstance(x, int), (4, 'bingo', 1.2, 6, True))
    ([4, 6], ['bingo', 1.2, True])
    """

    yes_s = []
    no_s = []
    for i in iterable:
        (yes_s if pred(i) else no_s).append(i)
    return yes_s, no_s


def identity(x):
    return x


def trim_if_present(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def is_file_secure(filename):
    try:
        st = os.stat(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        # the file doesn't exist, the security of it is irrelevant
        return True
    uid = os.getuid()

    # Skip enforcing the file owner and UID matching for the root user (uid == 0).
    # This is to allow "sudo cqlsh" to work with user owned credentials file.
    return (uid == 0 or st.st_uid == uid) and stat.S_IMODE(st.st_mode) & (stat.S_IRGRP | stat.S_IROTH) == 0


def get_file_encoding_bomsize(filename):
    """
    Checks the beginning of a file for a Unicode BOM.  Based on this check,
    the encoding that should be used to open the file and the number of
    bytes that should be skipped (to skip the BOM) are returned.
    """
    bom_encodings = ((codecs.BOM_UTF8, 'utf-8-sig'),
                     (codecs.BOM_UTF16_LE, 'utf-16le'),
                     (codecs.BOM_UTF16_BE, 'utf-16be'),
                     (codecs.BOM_UTF32_LE, 'utf-32be'),
                     (codecs.BOM_UTF32_BE, 'utf-32be'))

    firstbytes = open(filename, 'rb').read(4)
    for bom, encoding in bom_encodings:
        if firstbytes.startswith(bom):
            file_encoding, size = encoding, len(bom)
            break
    else:
        file_encoding, size = "utf-8", 0

    return file_encoding, size


def profile_on(fcn_names=None):
    if fcn_names and HAS_LINE_PROFILER:
        pr = LineProfiler()
        for fcn_name in fcn_names:
            pr.add_function(fcn_name)
        pr.enable()
        return pr

    pr = cProfile.Profile()
    pr.enable()
    return pr


def profile_off(pr, file_name):
    pr.disable()
    s = StringIO()

    if HAS_LINE_PROFILER and isinstance(pr, LineProfiler):
        pr.print_stats(s)
    else:
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats()

    ret = s.getvalue()
    if file_name:
        with open(file_name, 'w') as f:
            print("Writing to %s\n" % (f.name, ))
            f.write(ret)
    return ret
