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

import sys
import re
import time
import calendar
import math
from collections import defaultdict
from . import wcwidth
from .displaying import colorme, FormattedValue, DEFAULT_VALUE_COLORS
from cassandra.cqltypes import EMPTY

unicode_controlchars_re = re.compile(r'[\x00-\x31\x7f-\xa0]')
controlchars_re = re.compile(r'[\x00-\x31\x7f-\xff]')

def _show_control_chars(match):
    txt = repr(match.group(0))
    if txt.startswith('u'):
        txt = txt[2:-1]
    else:
        txt = txt[1:-1]
    return txt

bits_to_turn_red_re = re.compile(r'\\([^uUx]|u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|U[0-9a-fA-F]{8})')

def _make_turn_bits_red_f(color1, color2):
    def _turn_bits_red(match):
        txt = match.group(0)
        if txt == '\\\\':
            return '\\'
        return color1 + txt + color2
    return _turn_bits_red

default_null_placeholder = 'null'
default_time_format = ''
default_float_precision = 3
default_colormap = DEFAULT_VALUE_COLORS
empty_colormap = defaultdict(lambda: '')

def format_by_type(cqltype, val, encoding, colormap=None, addcolor=False,
                   nullval=None, time_format=None, float_precision=None):
    if nullval is None:
        nullval = default_null_placeholder
    if val is None:
        return colorme(nullval, colormap, 'error')
    if addcolor is False:
        colormap = empty_colormap
    elif colormap is None:
        colormap = default_colormap
    if time_format is None:
        time_format = default_time_format
    if float_precision is None:
        float_precision = default_float_precision
    return format_value(cqltype, val, encoding=encoding, colormap=colormap,
                        time_format=time_format, float_precision=float_precision,
                        nullval=nullval)

def color_text(bval, colormap, displaywidth=None):
    # note that here, we render natural backslashes as just backslashes,
    # in the same color as surrounding text, when using color. When not
    # using color, we need to double up the backslashes so it's not
    # ambiguous. This introduces the unique difficulty of having different
    # display widths for the colored and non-colored versions. To avoid
    # adding the smarts to handle that in to FormattedValue, we just
    # make an explicit check to see if a null colormap is being used or
    # not.

    if displaywidth is None:
        displaywidth = len(bval)
    tbr = _make_turn_bits_red_f(colormap['blob'], colormap['text'])
    coloredval = colormap['text'] + bits_to_turn_red_re.sub(tbr, bval) + colormap['reset']
    if colormap['text']:
        displaywidth -= bval.count(r'\\')
    return FormattedValue(bval, coloredval, displaywidth)

def format_value_default(val, colormap, **_):
    val = str(val)
    escapedval = val.replace('\\', '\\\\')
    bval = controlchars_re.sub(_show_control_chars, escapedval)
    return color_text(bval, colormap)

# Mapping cql type base names ("int", "map", etc) to formatter functions,
# making format_value a generic function
_formatters = {}

def format_value(type, val, **kwargs):
    if val == EMPTY:
        return format_value_default('', **kwargs)
    formatter = _formatters.get(type.__name__, format_value_default)
    return formatter(val, **kwargs)

def formatter_for(typname):
    def registrator(f):
        _formatters[typname] = f
        return f
    return registrator

@formatter_for('bytearray')
def format_value_blob(val, colormap, **_):
    bval = '0x' + ''.join('%02x' % c for c in val)
    return colorme(bval, colormap, 'blob')
formatter_for('buffer')(format_value_blob)


def format_python_formatted_type(val, colormap, color, quote=False):
    bval = str(val)
    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, color)

@formatter_for('Decimal')
def format_value_decimal(val, colormap, **_):
    return format_python_formatted_type(val, colormap, 'decimal')

@formatter_for('UUID')
def format_value_uuid(val, colormap, **_):
    return format_python_formatted_type(val, colormap, 'uuid')


@formatter_for('inet')
def formatter_value_inet(val, colormap, quote=False, **_):
    return format_python_formatted_type(val, colormap, 'inet', quote=quote)

@formatter_for('bool')
def format_value_boolean(val, colormap, **_):
    return format_python_formatted_type(val, colormap, 'boolean')

def format_floating_point_type(val, colormap, float_precision, **_):
    if math.isnan(val):
        bval = 'NaN'
    elif math.isinf(val):
        bval = 'Infinity'
    else:
        exponent = int(math.log10(abs(val))) if abs(val) > sys.float_info.epsilon else -sys.maxint -1
        if -4 <= exponent < float_precision:
            # when this is true %g will not use scientific notation,
            # increasing precision should not change this decision
            # so we increase the precision to take into account the
            # digits to the left of the decimal point
            float_precision = float_precision + exponent + 1
        bval = '%.*g' % (float_precision, val)
    return colorme(bval, colormap, 'float')

formatter_for('float')(format_floating_point_type)

def format_integer_type(val, colormap, **_):
    # base-10 only for now; support others?
    bval = str(val)
    return colorme(bval, colormap, 'int')

formatter_for('long')(format_integer_type)
formatter_for('int')(format_integer_type)

@formatter_for('date')
def format_value_timestamp(val, colormap, time_format, quote=False, **_):
    bval = strftime(time_format, calendar.timegm(val.utctimetuple()))
    if quote:
        bval = "'%s'" % bval
    return colorme(bval, colormap, 'timestamp')

formatter_for('datetime')(format_value_timestamp)

def strftime(time_format, seconds):
    local = time.localtime(seconds)
    formatted = time.strftime(time_format, local)
    if local.tm_isdst != 0:
        offset = -time.altzone
    else:
        offset = -time.timezone
    if formatted[-4:] != '0000' or time_format[-2:] != '%z' or offset == 0:
        return formatted
    # deal with %z on platforms where it isn't supported. see CASSANDRA-4746.
    if offset < 0:
        sign = '-'
    else:
        sign = '+'
    hours, minutes = divmod(abs(offset) / 60, 60)
    return formatted[:-5] + sign + '{0:0=2}{1:0=2}'.format(hours, minutes)

@formatter_for('str')
def format_value_text(val, encoding, colormap, quote=False, **_):
    escapedval = val.replace(u'\\', u'\\\\')
    if quote:
        escapedval = escapedval.replace("'", "''")
    escapedval = unicode_controlchars_re.sub(_show_control_chars, escapedval)
    bval = escapedval.encode(encoding, 'backslashreplace')
    if quote:
        bval = "'%s'" % bval
    displaywidth = wcwidth.wcswidth(bval.decode(encoding))
    return color_text(bval, colormap, displaywidth)

# name alias
formatter_for('unicode')(format_value_text)

def format_simple_collection(val, lbracket, rbracket, encoding,
                             colormap, time_format, float_precision, nullval):
    subs = [format_value(type(sval), sval, encoding=encoding, colormap=colormap,
                         time_format=time_format, float_precision=float_precision,
                         nullval=nullval, quote=True)
            for sval in val]
    bval = lbracket + ', '.join(sval.strval for sval in subs) + rbracket
    lb, sep, rb = [colormap['collection'] + s + colormap['reset']
                   for s in (lbracket, ', ', rbracket)]
    coloredval = lb + sep.join(sval.coloredval for sval in subs) + rb
    displaywidth = 2 * len(subs) + sum(sval.displaywidth for sval in subs)
    return FormattedValue(bval, coloredval, displaywidth)

@formatter_for('list')
def format_value_list(val, encoding, colormap, time_format, float_precision, nullval, **_):
    return format_simple_collection(val, '[', ']', encoding, colormap,
                                    time_format, float_precision, nullval)

@formatter_for('tuple')
def format_value_tuple(val, encoding, colormap, time_format, float_precision, nullval, **_):
    return format_simple_collection(val, '(', ')', encoding, colormap,
                                    time_format, float_precision, nullval)

@formatter_for('set')
def format_value_set(val, encoding, colormap, time_format, float_precision, nullval, **_):
    return format_simple_collection(sorted(val), '{', '}', encoding, colormap,
                                    time_format, float_precision, nullval)
formatter_for('frozenset')(format_value_set)
formatter_for('sortedset')(format_value_set)


@formatter_for('dict')
def format_value_map(val, encoding, colormap, time_format, float_precision, nullval, **_):
    def subformat(v):
        return format_value(type(v), v, encoding=encoding, colormap=colormap,
                            time_format=time_format, float_precision=float_precision,
                            nullval=nullval, quote=True)

    subs = [(subformat(k), subformat(v)) for (k, v) in sorted(val.items())]
    bval = '{' + ', '.join(k.strval + ': ' + v.strval for (k, v) in subs) + '}'
    lb, comma, colon, rb = [colormap['collection'] + s + colormap['reset']
                            for s in ('{', ', ', ': ', '}')]
    coloredval = lb \
               + comma.join(k.coloredval + colon + v.coloredval for (k, v) in subs) \
               + rb
    displaywidth = 4 * len(subs) + sum(k.displaywidth + v.displaywidth for (k, v) in subs)
    return FormattedValue(bval, coloredval, displaywidth)
formatter_for('OrderedDict')(format_value_map)
formatter_for('OrderedMap')(format_value_map)
formatter_for('OrderedMapSerializedKey')(format_value_map)


def format_value_utype(val, encoding, colormap, time_format, float_precision, nullval, **_):
    def format_field_value(v):
        if v is None:
            return colorme(nullval, colormap, 'error')
        return format_value(type(v), v, encoding=encoding, colormap=colormap,
                            time_format=time_format, float_precision=float_precision,
                            nullval=nullval, quote=True)

    def format_field_name(name):
        return format_value_text(name, encoding=encoding, colormap=colormap, quote=False)

    subs = [(format_field_name(k), format_field_value(v)) for (k, v) in val._asdict().items()]
    bval = '{' + ', '.join(k.strval + ': ' + v.strval for (k, v) in subs) + '}'
    lb, comma, colon, rb = [colormap['collection'] + s + colormap['reset']
                            for s in ('{', ', ', ': ', '}')]
    coloredval = lb \
                 + comma.join(k.coloredval + colon + v.coloredval for (k, v) in subs) \
                 + rb
    displaywidth = 4 * len(subs) + sum(k.displaywidth + v.displaywidth for (k, v) in subs)
    return FormattedValue(bval, coloredval, displaywidth)
