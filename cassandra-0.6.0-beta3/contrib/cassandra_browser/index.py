#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
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
#

## Cassandra Web Browser

import sys
try:
    from mod_python import util     # FieldStorage form processing
except:
    pass
try:                                # Choice of json parser
  import json
except:
  import simplejson as json
import pprint                       # JSON printer
import cutejson                     # My own JSON printer - cute!
try:
    sys.path.append('../../interface/gen-py')
except:
    pass
from cassandra import Cassandra     # Cassandra thrift API
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from cgi import escape, FieldStorage
from datetime import datetime       # Most timestamp handling
import time                         # clock(), time()
from copy import copy               # Cloning objects
import urllib                       # URL-encoding form parameters


### Action handlers - Cassandra ###########################################################################################

def _cassandraConnect(params):
    # Make socket
    transport = TSocket.TSocket(params['host'], params['port'])

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    #lient = Calculator.Client(protocol)
    client = Cassandra.Client(protocol)

    # Connect!
    transport.open()

    return {'client':client, 'transport':transport }

def _cassandraGetSliceFrom(params):
  try:
    cassandra = _cassandraConnect(params)
    s = ""

    range = Cassandra.SliceRange(params['search'],params['searchend'],params['descending'],params['count']+params['offset'])
    predicate = Cassandra.SlicePredicate(None, range)
    parent = Cassandra.ColumnParent(params['cf'], None) # No supercolumn, we are not using them ATM

    result = cassandra['client'].get_slice(params['table'], params['row'], parent, predicate, 1)

    ## Close!
    cassandra['transport'].close()

    n = len(result)-params['offset']
    if (n<0):
      n = 0
    s += "Obtained: "+str(n)+" columns (click on column data to open)"
    s += ' <span style="color:gray">- query predicate: '+str(predicate) + "</span><br>\n"
    s += "%s<table class=\"main\">" % _navigationButtonsHtml(params)
    strcol = """\
<tr class=\"%s\">
  <td class="rownum">%d</td>
  <td><tt>%s</tt></td>
  <td class=\"link\"><a href=\"%s\"><tt>%s</tt></a></td>
  <td>%s<br/><span class="parsedts">%s</span></td>
  <td><a title="Delete column" href=\"#\" onclick=\"javascript:if(confirm('Are you sure you want to delete this column?'))window.open('%s')\">&times;</a></td>
</tr>
"""
    i=0
    deleteParams = {\
        "host":params["host"],\
        "port":params["port"],\
        "table":params["table"],\
        "row":params["row"],\
        "cf":params["cf"]\
    }
    clazz = "even"
    for csp in result:
      col = csp.column # Assuming we are only querying columns - need to change this for supercolumns?
      s+="<!-- a column -- >\n"
      if i>=params['offset']:
        targetmap = copy(params)
        targetmap['open'] = col.name
        target = "view?%s" % urllib.urlencode(targetmap)
        deleteParams['columnName'] = col.name
        svalue = col.value
        if len(svalue)>120:
          svalue = escape(svalue[0:120])+_htmlEllipsis()
        else:
          svalue = escape(svalue)
        s += strcol % (clazz, i, col.name, target, svalue, str(col.timestamp), _cassandraTimestampToDatetime(col.timestamp), "delete?"+urllib.urlencode(deleteParams))
        if clazz=="odd":
           clazz = "even"
        else:
           clazz = "odd"
      i=i+1

    s += "</table>\n"

    s += _navigationButtonsHtml(params)

    return s

  except Thrift.TException, tx:
    print '%s' % (tx.message)
    return 'TException: %s' % (tx.message)

  except Cassandra.InvalidRequestException, ire:
    print '%s' % (ire.why)
    return 'InvalidRequestException: %s' % (ire.why)

def _cassandraGetColumn(params):
  try:
    cassandra = _cassandraConnect(params)
    pp = pprint.PrettyPrinter(indent = 2)

    result = cassandra['client'].get(params['table'], params['row'], Cassandra.ColumnPath(params["cf"], None, params['open']), 1)
    col = result.column

    # Close!
    cassandra['transport'].close()
    s = ""

    # Headers
    s += "Column: %s<br/>\nTimestamp: %s - <span class=\"parsedts\">%s</span>" % \
        (col.name, col.timestamp, _cassandraTimestampToDatetime(col.timestamp))

    jsonObj = None
    error = ""
    try:
      jsonObj = json.loads(col.value)
    except ValueError, exc:
      error = str(exc)

    # Colorful pretty printed
    if jsonObj!=None:
      s += "<h3>Data is a json object!</h3>\n<div class=\"columnData\"><tt>\n"+cutejson.cutejson(jsonObj)+"\n</tt></div>"
    else:
      s += "<br/><br/>(Not a valid json string) - "+str(error)

    # Plain data
    s += "<h3>Data</h3><div class=\"columnData\">%s</div><br/>" % escape(col.value)

    if jsonObj!=None:
      s += "<h3>By Python prettyprinter:</h3><div class=\"columnData\"><pre>%s</pre></div>" % json.dumps(json.loads(col.value), indent=4, sort_keys=True)
#    s += "Formatted json:<div class=\"columnData\"><pre>%s</pre></div>" % pp.pformat(json.loads(result.value))

    return s

  except Thrift.TException, tx:
    print '%s' % (tx.message)
    return 'Thrift Error: %s' % (tx.message)

  except Cassandra.InvalidRequestException, ire:
    print '%s' % (ire.message)
    return 'Invalid Request Error: %s' % (ire.message)

def _cassandraDeleteColumn(params):
  try:
    cassandra = _cassandraConnect(params)
    cfcol =  Cassandra.ColumnPath(params["cf"],None,params['columnName'])
    cassandra['client'].remove(params['table'], params['row'], cfcol, _nowAsCassandraTimestamp(), 1)
    cassandra['transport'].close()

    s = "Column deleted. (You can delete this window, and refresh the launcher window contents)<ul><li>table: %s</li><li>Row: %s</li><li>CF:Col: %s</li>" %\
        (params['table'], params['row'], cfcol)

    return s
  except Thrift.TException, tx:
    print '%s' % (tx.message)
    return 'Thrift Error: %s' % (tx.message)

def _cassandraInsert(params, colName, columnValue):
  try:

    cassandra = _cassandraConnect(params)
    cassandra['client'].insert(params['table'], params['row'], Cassandra.ColumnPath(params["cf"],None,colName), columnValue, _nowAsCassandraTimestamp(), 1)
    cassandra['transport'].close()

    return "Column '%s' inserted" % escape(colName)

  except Thrift.TException, tx:
    print '%s' % (tx.message)
    return 'Error inserting column - Thrift Error: %s' % (tx.message)

  except Cassandra.InvalidRequestException, ire:
    print '%s' % (ire.message)
    return 'Error inserting column - Invalid Request Error: %s' % (ire.message)


### Request handlers ###########################################################################################

def index(req):
  # Defaults to "list":
  return list(req)

def list(req):
    params = _processForm(req)
    return _list(params)

def _list(params):
  t0 = time.clock()
  if params['action']=="insert":
    if (params['newname']!=None and params['value']!=None):
      message="<div class=\"message\">%s</div>" % _cassandraInsert(params,params['newname'],params['value'])
      ## Change params so that we browse on the inserted column
      params['search'] = params['newname']
      params['searchend'] = ''
      params['descending'] = 0
      params['offset'] = 0
    else:
      message="<div class=\"message\">%s</div>" % "Please specify a column name and a column value! (Press your browser's Back button)"
  else:
    message=""
  paramsTable = _formatParams(params)
  cassandraData = _cassandraGetSliceFrom(params);
  t1 = time.clock()
  return _mainHtml() % (params['host'], css(), paramsTable+message+cassandraData+_htmlFooter(t1-t0))

def view(req):
    params = _processForm(req)
    return _view(params)

def _view(params):
  t0 = time.clock()
  paramsTable = _formatParams(params)
  if params['open']==None:
    colstr = "<b>No column specified!</b>"
  else:
    colstr = _cassandraGetColumn(params);
  t1 = time.clock()
  return _mainHtml() % (params['host'], css(), paramsTable+colstr+_htmlFooter(t1-t0))

def delete(req):
    params = _processForm(req)
    return _delete(params)

def _delete(params):
  t0 = time.clock()
  paramsTable = _formatParams(params)
  if params['columnName']==None:
    colstr = "<b>No column specified!</b>"
  else:
    colstr = _cassandraDeleteColumn(params)
  t1 = time.clock()
  return _mainHtml() % (params['host'], css(), paramsTable+colstr+_htmlFooter(t1-t0))


### HTML formatting functions ###########################################################################################

def _formatParams(params):
    s = "<form action=\"list\">"
    s += "<div>"
    s += "Host: <input size=\"30\" name=\"host\" value=\"%s\" title=\"%s\"></input>:\n"             % (params['host']  , "Server address")
    s += "<input size=\"4\" name=\"port\" value=\"%s\" title=\"%s\"></input>, \n"                   % (params['port']  , "Thrift port")
    s += "Keyspace: <input size=\"10\" name=\"table\" value=\"%s\" title=\"%s\"></input>, \n"       % (params['table'] , "")
    s += "Row: <input size=\"60\" name=\"row\" value=\"%s\" title=\"%s\"></input>, \n"              % (params['row']   , "")
    s += "CF: <input size=\"30\" name=\"cf\" value=\"%s\" title=\"%s\"></input>\n"                  % (params['cf']    , "")
    s += "</div><div class=\"onNotInserting\">"
    s += "start value: <input size=\"60\" name=\"search\" value=\"%s\" title=\"%s\"></input>, \n"   % (params['search']   , "Start value for the search (optional)")
    s += "end value: <input size=\"60\" name=\"searchend\" value=\"%s\" title=\"%s\"></input>, \n"  % (params['searchend'], "End value for the search (optional)")
    s += "offset: <input size=\"4\" name=\"offset\" value=\"%s\" title=\"%s\"></input>, \n"         % \
                   (params['offset'] , "Offset - This is used to allow paging in the web UI. Note that offset+count elements will be queried to cassandra; a big number impacts performance!")
    s += "count: <input size=\"4\" name=\"count\" value=\"%s\" title=\"%s\"></input>\n"             % (params['count'] , "")
    if params['descending']==1:
      checked = "checked=\"true\""
    else:
      checked = ""
    s += " - <input type=\"checkbox\" name=\"descending\" value=\"1\" %s>Reversed</input><br/>\n" % checked
    s += "</div>"
    s += "<input id=\"querybutton\" class=\"submit onNotInserting\" type=\"submit\"/>\n"
    s += """\
&nbsp;&nbsp;<a class="onNotInserting inserthelp" onclick="javascript:document.getElementsByTagName('body')[0].className = 'inserting';" href="#">Insert column</a>
<div class="onInserting">
<div>
    Name: <input size=\"60\" name=\"newname\" value=\"\" title=\"Name for the new column\"></input>
</div>
<div>
    Value:
    <textarea rows="20" cols="60" name="value" title="Value of the new column"></textarea>
</div>
<input id="insertbutton" class="submit" type="submit" value="insert" name="action" title="Insert the column on the keyspace, row and columnfamily specified above."/>
&nbsp;&nbsp;<a class="" onclick="javascript:document.getElementsByTagName('body')[0].className = '';" href="#">Cancel</a>
</div>
"""
    s += "</form>"
    return s


def _htmlEllipsis():
    return " <span style=\"color:red\">(&hellip;)</span>"

def _htmlFooter(ts=0):
    return "<center style=\"color:gray\">Rendered in %fms - (c)Copyright Apache Software Foundation, 2009</center>" \
    % ts

def css():
    return cutejson.css()+"""\
body { font-family: Sans-serif; font-size: 8pt; }
td { font-size: 8pt; }
pre { margin: 0; }
table { border: 1px solid gray; }
table.main td.link { cursor: pointer; }
table.main tr.odd { background: #f8f8f8; }
table.main td.link:hover { background: #ffffcc; }
table.main td { padding: 1px 5px; }
table.main td a, table.maintd a:visited { text-decoration: none; color: black; }
a, a:visited { color: #0000df; }
div.columnData { overflow: auto; margin: 10px 0px; width: 90%; padding: 5px; border: 1px solid gray; font-family: monospace; }
.rownum, .parsedts { color: gray; }
.navbar { margin: 6px 0; font-size: 110%; }
.navbar a { text-decoration: none; padding: 1px; }
.navbar a:hover { color: white; background: #0000df;}
input, textarea { background: #f8f8f8; border: 1px solid gray; padding: 2px; }
input.submit { font-weight: bold; background: #FFFFBB; cursor: pointer; }
/*#querybutton { vertical-align: top; margin-top: 4px; }
#insertbutton, .inserthelp { vertical-align: bottom; margin-bottom: 4px; }*/
textarea { vertical-align: baseline; }
div.message { padding: 4px; border: 1px solid blue; background: #f8f8f8; width: 90%; color: blue; margin: 8px 0px; }

.onInserting { display:none }
body.inserting div.onInserting { display: block; }
body.inserting span.onInserting { display: inline; }
body.inserting .onNotInserting { display: none; }
"""

def _navigationButtonsHtml(params):
    s = "<div class=\"navbar\">"
    paramsFst = copy(params)
    paramsFst['offset'] = 0

    s += "<a href=\"list?%s\">&lArr; &lArr; First</a> " % urllib.urlencode(paramsFst)
    s += "&nbsp;&nbsp;&bull;&nbsp;&nbsp;"

    if params['offset']>0:
      paramsPrev = copy(params)
      paramsPrev['offset'] = paramsPrev['offset'] - paramsPrev['count']
      if paramsPrev['offset']<0:
         paramsPrev['offset'] = 0
      s += "<a href=list?%s>&lArr; Previous</a> " % urllib.urlencode(paramsPrev)
    else:
      s += "&lArr; Previous"
    s += "&nbsp;&nbsp;&bull;&nbsp;&nbsp;"

    s += "<a href=list?%s>Refresh</a> " % urllib.urlencode(params)
    s += "&nbsp;&nbsp;&bull;&nbsp;&nbsp;"

    paramsNext = copy(params)
    paramsNext['offset'] = paramsNext['offset'] + paramsNext['count']
    s += "<a href=list?%s>&rArr; Next</a> " % urllib.urlencode(paramsNext)
    s += "</div>"
    return s

def _mainHtml():
  return """\
<html>
<title>Cassandra on %s</title>
<head><style>%s</style></head>
<body>
<h2>Cassandra browser</h2>
%s
</body></html>
"""

### Misc functions ###########################################################################################

def _processForm(req):
  form = util.FieldStorage(req)
  pyopts = req.get_options()

  defhost  = "localhost"
  defport  = 9160
  deftable = "Keyspace"
  defrow   = ""
  defcf    = ""
  # ugly conditionals, 2.5 compatible
  if "CassandraKeyspace" in pyopts:
    deftable = pyopts["CassandraKeyspace"]
  if "CassandraHost" in pyopts:
    defhost  = pyopts["CassandraHost"]
  if "CassandraPort" in pyopts:
    defport  = pyopts["CassandraPort"]
  if "CassandraRow" in pyopts:
    defrow   = pyopts["CassandraRow"]
  if "CassandraColumnFamily" in pyopts:
    defcf    = pyopts["CassandraColumnFamily"]

  return getRequiredParameters(form, defhost, defport, deftable, defrow, defcf)

def getRequiredParameters(form, defhost, defport, deftable, defrow, defcf):
  params = {}

  # Generic params, used in most actions
  params['table'] = str(form.getfirst('table', deftable))
  params['host']  = str(form.getfirst('host', defhost))
  params['port']  = int(form.getfirst('port', defport))

  params['row']   = str(form.getfirst('row', defrow))
  params['cf']    = str(form.getfirst('cf', defcf))

  params['search'] = str(form.getfirst('search', ""))
  params['searchend'] = str(form.getfirst('searchend', ""))

  params['offset'] = int(form.getfirst('offset', 0))
  params['count'] = int(form.getfirst('count', 20))
  params['descending'] = int(form.getfirst('descending', 0))

  # For column inserter
  params['value'] = form.getfirst('value', None)
  params['action'] = form.getfirst('action', None)
  params['newname'] = form.getfirst('newname', None)
  # For column viewer
  params['open'] = form.getfirst('open', None)
  params['columnName'] = form.getfirst('columnName', None) # For deletion -- TODO: unify this with other names!
  return params

def _cassandraTimestampToDatetime(colTimestamp):
  return datetime.fromtimestamp(colTimestamp/1000)

def _nowAsCassandraTimestamp():
  return long(time.time()*1000)

def _index_wsgi(environ, start_response):
    path = environ.get('PATH_INFO', '')
    found_path = None
    for path_option in ['list', 'view', 'delete']:
        if path_option in path:
            found_path = path_option
            break
    if not found_path:
        start_response('404 NOT FOUND', [('Content-Type', 'text/html')])
        return 'Path must be list, view, or delete'
    start_response('200 OK', [('Content-Type', 'text/html')])
    form = FieldStorage(fp=environ['wsgi.input'], environ=environ)
    params = getRequiredParameters(form, 'localhost', 9160, 'Keyspace1', '', 'Standard1')
    try:
        retval = globals()['_' + found_path](params)
    except KeyError, e:
        return ['Request missing parameter(s):' + str(e)]
    return [retval]

if __name__ == "__main__":
    from wsgiref.simple_server import make_server
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-a", "--address", dest="host",
                      help="Host address on which to listen", default="localhost")
    parser.add_option("-p", "--port", dest="port",
                      help="Port on which to listen", default="8111")
    (options, args) = parser.parse_args()

    srv = make_server(options.host, int(options.port), _index_wsgi)
    srv.serve_forever()
