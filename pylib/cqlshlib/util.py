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

import codecs
from itertools import izip


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
    for cgroup in izip(*strs):
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
        file_encoding, size = "ascii", 0

    return (file_encoding, size)
