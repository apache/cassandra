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
    if colormap is NO_COLOR_MAP:
        return bval
    if colormap is None:
        colormap = DEFAULT_VALUE_COLORS
    return FormattedValue(bval, colormap[colorkey] + bval + colormap['reset'])


def get_str(val):
    if isinstance(val, FormattedValue):
        return val.strval
    return val


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
    date=GREEN,
    time=GREEN,
    int=GREEN,
    float=GREEN,
    decimal=GREEN,
    inet=GREEN,
    boolean=GREEN,
    uuid=GREEN,
    duration=GREEN,
    collection=BLUE,
    reset=ANSI_RESET,
)

COLUMN_NAME_COLORS = defaultdict(lambda: MAGENTA,
                                 error=RED,
                                 blob=DARK_MAGENTA,
                                 reset=ANSI_RESET,
                                 )

NO_COLOR_MAP = dict()

class TablePrinter:
    def print_header(self, formatted_names):
        raise NotImplementedError

    def print_rows(self, formatted_names, formatted_values):
        raise NotImplementedError

    def finish(self):
        pass

    @staticmethod
    def factory(format_type, shell):
        format_map = {'csv': CsvTablePrinter, 'json': JsonTablePrinter, 'tabular': TabularTablePrinter}
        printer_cls = format_map.get(format_type.lower(), TabularTablePrinter)
        return printer_cls(shell) if format_type.lower() != 'tabular' else printer_cls(shell, shell.tty)

class TabularTablePrinter(TablePrinter):
    def __init__(self, shell, tty, row_count_offset=0):
        self._shell = shell
        self._tty = tty
        self._row_count_offset = row_count_offset
        self._pending_header = None

    def print_header(self, formatted_names):
        # Store only — cannot render yet because column widths depend on
        # data values. print_rows will render header+data together.
        # Empty-result case is handled in finish().
        self._pending_header = formatted_names

    def print_rows(self, formatted_names, formatted_values):
        # with_header=True only when print_header was called for this page.
        with_header = self._pending_header is not None
        self._pending_header = None
        if self._shell.expand_enabled:
            self._shell.print_formatted_result_vertically(
                formatted_names, formatted_values, self._row_count_offset)
        else:
            self._shell.print_formatted_result(
                formatted_names, formatted_values, with_header, self._tty)
        if formatted_values:
            self._row_count_offset += len(formatted_values)

    def finish(self):
        if self._pending_header is not None:
            self._shell.print_formatted_result(
                self._pending_header, None, with_header=True, tty=self._tty)
            self._pending_header = None

class CsvTablePrinter(TablePrinter):
    def __init__(self, shell):
        import csv
        self._writer = csv.writer(shell.query_out)
        self._header_written = False
        self._colnames = None

    def print_header(self, formatted_names):
        self._colnames = [n.strval for n in formatted_names]

    def print_rows(self, formatted_names, formatted_values):
        if not self._header_written:
            self._writer.writerow(self._colnames)
            self._header_written = True
        if formatted_values is None:
            return
        for row in formatted_values:
            self._writer.writerow([col.strval for col in row])

    def finish(self):
        if self._colnames is not None and not self._header_written:
            self._writer.writerow(self._colnames)
            self._header_written = True

class JsonTablePrinter(TablePrinter):
    def __init__(self, shell):
        self._shell = shell
        self._colnames = None
        self._first_row = True

    def print_header(self, formatted_names):
        self._colnames = [n.strval for n in formatted_names]
        self._shell.writeresult('[')

    def print_rows(self, formatted_names, formatted_values):
        import json
        if formatted_values is None:
            return
        for row in formatted_values:
            row_dict = {self._colnames[i]: col.strval for i, col in enumerate(row)}
            serialized = json.dumps(row_dict)
            if self._first_row:
                self._shell.writeresult('  ' + serialized, newline=False)
                self._first_row = False
            else:
                self._shell.writeresult(',\n  ' + serialized, newline=False)

    def finish(self):
        self._shell.writeresult('\n]')
