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

from datetime import datetime
from cassandra.util import DateRange, DateRangeBound, DateRangePrecision, OPEN_BOUND, ms_timestamp_from_datetime


def _raise_parse_error(msg):
    """
    We need to throw ParseError which is defined in copyutil
    since copyutil imports this file as part of it's initialization though,
    importing it at the top of this file would create an import loop, so
    we just import it locally when/if we need it
    """
    from cqlshlib import copyutil
    raise copyutil.ParseError(msg)


def _new_date_range_bound(datetime, precision):
    millis = ms_timestamp_from_datetime(datetime)
    return DateRangeBound(value=millis, precision=precision)


def _parse_date_range_bound(val):
    if val == '*':
        return OPEN_BOUND
    else:
        year = 1
        month = 1
        day = 1
        hour = 0
        minute = 0
        second = 0
        millisecond = 0
        precision = DateRangePrecision.MILLISECOND

        date_split = val.replace('Z', '').split('-')
        if len(date_split) > 0:
            year = int(date_split[0])
            precision = DateRangePrecision.YEAR
        if len(date_split) > 1:
            month = int(date_split[1])
            precision = DateRangePrecision.MONTH
        if len(date_split) > 2:
            day_split = date_split[2].split('T')
            day = int(day_split[0])
            precision = DateRangePrecision.DAY
            if len(day_split) > 1:
                time_split = day_split[1].split(':')
                if len(time_split) > 0:
                    hour = int(time_split[0])
                    precision = DateRangePrecision.HOUR
                if len(time_split) > 1:
                    minute = int(time_split[1])
                    precision = DateRangePrecision.MINUTE
                if len(time_split) > 2:
                    second_split = time_split[2].split('.')
                    second = int(second_split[0])
                    precision = DateRangePrecision.SECOND
                    if len(second_split) > 1:
                        millisecond = int(second_split[1])
                        precision = DateRangePrecision.MILLISECOND

    return _new_date_range_bound(
        datetime=datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second,
                          microsecond=millisecond * 1000),
        precision=precision)


def _convert_daterange(val):
    if val.startswith('['):
        if val.endswith(']'):
            bounds = val[1:-1].split(' TO ')
            if len(bounds) == 2:
                lower_bound = _parse_date_range_bound(bounds[0].strip()).round_down()
                upper_bound = _parse_date_range_bound(bounds[1].strip()).round_up()
                return DateRange(lower_bound=lower_bound, upper_bound=upper_bound)
            else:
                _raise_parse_error("If date range starts with [ must contain ' TO '; got {}".format(val))
        else:
            _raise_parse_error("If date range starts with [ must end with ]; got {}".format(val))
    else:
        bound = _parse_date_range_bound(val).round_down()
        return DateRange(value=bound)


def _patch_get_converters(klass):
    original_method = klass._get_converter

    def new_method(self, cql_type):
        if cql_type.typename == 'daterange':
            return _convert_daterange
        else:
            return original_method(self, cql_type)

    klass._get_converter = new_method


def patch_daterange_import_conversion(klass):
    """
    monkey patches cqlshlib.copyutil.ImportConversion to support DateRangeType
    """
    _patch_get_converters(klass)
