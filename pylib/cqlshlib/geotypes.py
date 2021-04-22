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

from numbers import Number
import re

from cassandra.metadata import protect_value
from cassandra.util import Point, LineString, Polygon
from geomet import wkt


def _raise_parse_error(msg):
    """
    We need to throw ParseError which is defined in copyutil
    since copyutil imports this file as part of it's initialization though,
    importing it at the top of this file would create an import loop, so
    we just import it locally when/if we need it
    """
    from cqlshlib import copyutil
    raise copyutil.ParseError(msg)


def _validate_point(val, raw):
    if not all([isinstance(v, Number) for v in val]):
        _raise_parse_error("Got non-numeric value in {}".format(raw))

    if len(val) != 2:
        _raise_parse_error("Got point with {} coordinates in: {}".format(len(val), raw))


def _get_coords(val, expected_type):
    geojson = wkt.loads(val)
    gjtype = geojson.get('type')
    if gjtype != expected_type:
        _raise_parse_error("Expected {} type, but got {} type for {}".format(expected_type, gjtype, val))

    return geojson.get("coordinates", [])


def _convert_point(val):
    coords = _get_coords(val, 'Point')
    _validate_point(coords, val)
    point = Point(*coords)
    return point


def _convert_linestring(val):
    points = _get_coords(val, 'LineString')
    for xy in points:
        _validate_point(xy, val)
    linestring = LineString(points)
    return linestring


def _convert_polygon(val):
    rings = _get_coords(val, 'Polygon')
    if len(rings) == 0:
        return Polygon([])

    for ring in rings:
        for point in ring:
            _validate_point(point, val)

    polygon = Polygon(exterior=rings[0], interiors=rings[1:])
    return polygon


def _patch_get_converters(klass):
    """
    patches the get converters method to convert WKT to dse.util.{Point, LineString, Polygon}
    when using prepared statements to batch load
    """
    original_method = klass._get_converter

    def new_method(self, cql_type):
        if cql_type.typename == 'PointType':
            return _convert_point
        elif cql_type.typename == 'LineStringType':
            return _convert_linestring
        elif cql_type.typename == 'PolygonType':
            return _convert_polygon
        else:
            return original_method(self, cql_type)
    klass._get_converter = new_method


def _patch_init(klass):
    """
    patches the constructor method to also protect (quote) geotype values
    when making queries with string literal values
    """
    original_method = klass.__init__

    def new_method(self, *args, **kwargs):
        original_method(self, *args, **kwargs)
        ptypes = zip(self.protectors, self.coltypes)
        clean = lambda t: re.sub("[\\W]", "", t.split('.')[-1])  # discard java package names and ' characters
        gtypes = {'PointType', 'LineStringType', 'PolygonType'}
        self.protectors = [protect_value if clean(t) in gtypes else p for p, t in ptypes]
    klass.__init__ = new_method


def patch_geotypes_import_conversion(klass):
    """
    monkey patches cqlshlib.copyutil.ImportConversion to support geotypes
    """
    _patch_get_converters(klass)
    _patch_init(klass)
