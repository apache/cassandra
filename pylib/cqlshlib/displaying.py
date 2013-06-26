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

import re
from collections import defaultdict

RED = '\033[0;1;31m'
GREEN = '\033[0;1;32m'
YELLOW = '\033[0;1;33m'
BLUE = '\033[0;1;34m'
MAGENTA = '\033[0;1;35m'
CYAN = '\033[0;1;36m'
WHITE = '\033[0;1;37m'
DARK_MAGENTA = '\033[0;35m'
ANSI_RESET = '\033[0m'

def colorme(bval, colormap, colorkey):
    if colormap is None:
        colormap = DEFAULT_VALUE_COLORS
    return FormattedValue(bval, colormap[colorkey] + bval + colormap['reset'])

class FormattedValue:
    def __init__(self, strval, coloredval=None, displaywidth=None):
        self.strval = strval
        if coloredval is None:
            coloredval = strval
        self.coloredval = coloredval
        if displaywidth is None:
            displaywidth = len(strval)
        # displaywidth is useful for display of special unicode characters
        # with
        self.displaywidth = displaywidth

    def __len__(self):
        return len(self.strval)

    def _pad(self, width, fill=' '):
        if width > self.displaywidth:
            return fill * (width - self.displaywidth)
        else:
            return ''

    def ljust(self, width, fill=' ', color=False):
        """
        Similar to self.strval.ljust(width), but takes expected terminal
        display width into account for special characters, and does not
        take color escape codes into account.
        """
        if color:
            return self.color_ljust(width, fill=fill)
        return self.strval + self._pad(width, fill)

    def rjust(self, width, fill=' ', color=False):
        """
        Similar to self.strval.rjust(width), but takes expected terminal
        display width into account for special characters, and does not
        take color escape codes into account.
        """
        if color:
            return self.color_rjust(width, fill=fill)
        return self._pad(width, fill) + self.strval

    def color_rjust(self, width, fill=' '):
        """
        Similar to self.rjust(width), but uses this value's colored
        representation, and does not take color escape codes into account
        in determining width.
        """
        return self._pad(width, fill) + self.coloredval

    def color_ljust(self, width, fill=' '):
        """
        Similar to self.ljust(width), but uses this value's colored
        representation, and does not take color escape codes into account
        in determining width.
        """
        return self.coloredval + self._pad(width, fill)

DEFAULT_VALUE_COLORS = dict(
    default=YELLOW,
    text=YELLOW,
    error=RED,
    blob=DARK_MAGENTA,
    timestamp=GREEN,
    int=GREEN,
    float=GREEN,
    decimal=GREEN,
    inet=GREEN,
    boolean=GREEN,
    uuid=GREEN,
    collection=BLUE,
    reset=ANSI_RESET,
)

COLUMN_NAME_COLORS = defaultdict(lambda: MAGENTA,
    error=RED,
    blob=DARK_MAGENTA,
    reset=ANSI_RESET,
)
