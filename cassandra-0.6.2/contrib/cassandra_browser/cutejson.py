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

## Cute, HTML, smart json-interpreted pretty-printer! ;)

## usage: call cutejson(object) on any object, string or whatever; print the resulting HTML code.

try:
    import json
except:
    import simplejson as json
from datetime import datetime

def _rep(v,nest):
  s = ""
  for i in range(0,nest):
    s += v
  return s

def cutejson(v, nest=0, metanest=0):
  t = type(v).__name__;
  comment = ""
  prefix = ""
  postfix = ""
  s = ""
  if (t=='unicode' or t=='str'):
     try:
        v = json.loads(v)
        # Cute! Parsed a json string
        #s = "{<br/>\n%s}" % _mypp(j, nest+2)
        comment = "&nbsp;&nbsp;## Parsed contents"
        metanest += 1
        prefix = "<span class=\"interpreted metanest%d\">" % metanest
        postfix = "</span>"
     except ValueError:
        # Do nothing
        comment = ""

  t = type(v).__name__;
  if (t=='dict'):
     s = "%s{%s<br/>\n" % (prefix, comment)
     for key,val in v.iteritems():
       s += "%s<span class=\"key\">%s</span>: %s<br/>\n" % (_rep("&nbsp;",(nest+1)*3), key, cutejson(val,nest+1,metanest))
     s += "%s}%s" % (_rep("&nbsp;",nest*3), postfix)
  elif (t=='list'):
     s = "[<br/>\n"
     sep = "&nbsp;"
     for val in v:
       spc = _rep("&nbsp;",nest*3) + sep + _rep("&nbsp;",2)
       s += "%s%s<br/>\n" % (spc, cutejson(val,nest+1,metanest))
       sep = ","
     s += "%s]" % _rep("&nbsp;",nest*3)
       #"%s<span class=\"key\">%s</span>: %s<br/>\n" % (_rep("&nbsp;",(nest+1)*3), key, cutejson(val,nest+1,metanest))
  else:
    if (t=='str' or t=='unicode'):
      prefix = postfix = "\""
    elif (t=='int' and v>900000000 and v<1300000000):
      postfix = "&nbsp;&nbsp;<span class=\"interpreted metanest%d\">## Seconds: %s</span>" % (metanest+1, str(datetime.fromtimestamp(v)))
    elif (t=='long' and v>900000000000 and v<1300000000000):
      postfix = "&nbsp;&nbsp;<span class=\"interpreted metanest%d\">## Millis: %s</span>" % (metanest+1, str(datetime.fromtimestamp(v/1000)))
    s = "%s<span class=\"%s\">%s%s%s</span>" % (comment, t, prefix, str(v), postfix)
  return s

#    s = _rep("&nbsp;",nest)
#    for k,v in object.iteritems():
#       s += "<span class=\"key\">%s</span>: %s<br/>\n" % (k, _mypp_val(v,nest))
#    return s

def css():
    return """
.hint { background-color: #FFFFBB; }
.key { font-weight: bold; }
.int, .long { color: green; }
.float { color: magenta; }
.bool { color: brown; }
.unicode, .string { color: navy; }
.interpreted { background-color: #BBBBBB; }
.interpreted.metanest1 { background-color: #FFFFBB; }
.interpreted.metanest2 { background-color: #BBFFBB; }
.interpreted.metanest3 { background-color: #BBBBFF; }
"""


# Test as main()
if __name__ == "__main__":
  __jsonstr = """ {"test":"{\\"json\\":true,\\"str\\":\\"{\\\\\\"a\\\\\\":0}\\"}",
"id":"00071BEB5F143D30832575D800773A8A"
,"timestamp":1245275000
,"last_revision":1245275000
,"other_timestamp":1245274937123
,"has_attachments":false,"from":{"email":"some@email.com","lname":"Someone Else"}
,"to":[{"email":"john@doe.com","name":"John Doe"}]
,"collections":[{"type":"SystemView","name":"all"},{"type":"SystemView","name":"sent"}]
,"text":"This is a test string"
,"type":0
,"length":123.2
}
"""
  print "<html><head><style>%s</style></head><body><tt>\n%s\n</tt></body>" % (css(), cutejson(json.loads(__jsonstr)))
