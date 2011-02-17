
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

from uuid import UUID
from StringIO import StringIO
from errors import InvalidQueryFormat

__all__ = ['prepare']

def prepare(query, *args):
    result = StringIO()
    index = query.find('?')
    oldindex = 0
    count = 0
    
    while (index >= 0):
        result.write(query[oldindex:index])
        try:
            result.write(marshal(args[count]))
        except IndexError:
            raise InvalidQueryFormat("not enough arguments in substitution")
        
        oldindex = index + 1
        index = query.find('?', index + 1)
        count += 1
    result.write(query[oldindex:])
    
    if count < len(args):
        raise InvalidQueryFormat("too many arguments in substitution")
    
    return result.getvalue()

def marshal(term):
    if isinstance(term, long):
        return "%dL" % term
    elif isinstance(term, int):
        return "%d" % term
    elif isinstance(term, unicode):
        return "u\"%s\"" % term
    elif isinstance(term, str):
        return "\"%s\"" % term
    elif isinstance(term, UUID):
        if term.version == 1:
            return "timeuuid(\"%s\")" % str(term)
        else:
            return "uuid(\"%s\")" % str(term)
    else:
        return str(term)
