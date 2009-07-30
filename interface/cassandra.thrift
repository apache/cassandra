#!/usr/local/bin/thrift --java --php --py
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
# Interface definition for Cassandra Service
#

namespace java org.apache.cassandra.service
namespace cpp org.apache.cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra

#
# structures
#

struct Column {
   1: binary                        name,
   2: binary                        value,
   3: i64                           timestamp,
}

typedef map<string, list<Column>>   column_family_map

struct BatchMutation {
   1: string                        key,
   2: column_family_map             cfmap,
}

struct SuperColumn {
   1: binary                        name,
   2: list<Column>                  columns,
}

typedef map<string, list<SuperColumn>> SuperColumnFamilyMap

struct BatchMutationSuper {
   1: string                        key,
   2: SuperColumnFamilyMap          cfmap,
}


typedef list<map<string, string>>   ResultSet

struct CqlResult {
   1: i32                           error_code, // 0 - success
   2: string                        error_txt,
   3: ResultSet                     result_set,
}

#
# Exceptions
#

# a specific column was requested that does not exist
exception NotFoundException {
}

# invalid request (keyspace / CF does not exist, etc.)
exception InvalidRequestException {
    1: string why
}

# not all the replicas required could be created / read
exception UnavailableException {
}

# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)


#
# service api
#

enum ConsistencyLevel { 
    ZERO = 0, 
    ONE = 1, 
    QUORUM = 2, 
    ALL = 3, 
}

struct ColumnParent {
    3: string          column_family,
    4: optional binary super_column,
}

struct ColumnPath {
    3: string          column_family,
    4: optional binary super_column,
    5: binary          column,
}

struct SuperColumnPath {
    3: string          column_family,
    4: binary          super_column,
}

struct ColumnPathOrParent {
    3: string          column_family,
    4: optional binary super_column,
    5: optional binary column,
}


service Cassandra {
  list<Column> get_slice_by_names(1:string keyspace, 2:string key, 3:ColumnParent column_parent, 4:list<binary> column_names, 5:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  
  list<Column> get_slice(1:string keyspace, 2:string key, 3:ColumnParent column_parent, 4:binary start, 5:binary finish, 6:bool is_ascending, 7:i32 count=100, 8:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  Column       get_column(1:string keyspace, 2:string key, 3:ColumnPath column_path, 4:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  i32            get_column_count(1:string keyspace, 2:string key, 3:ColumnParent column_parent, 5:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire),

  void     insert(1:string keyspace, 2:string key, 3:ColumnPath column_path, 4:binary value, 5:i64 timestamp, 6:ConsistencyLevel consistency_level=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  void     batch_insert(1:string keyspace, 2:BatchMutation batch_mutation, 3:ConsistencyLevel consistency_level=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  void           remove(1:string keyspace, 2:string key, 3:ColumnPathOrParent column_path_or_parent, 4:i64 timestamp, 5:ConsistencyLevel consistency_level=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  list<SuperColumn> get_slice_super(1:string keyspace, 2:string key, 3:string column_family, 4:binary start, 5:binary finish, 6:bool is_ascending, 7:i32 count=100, 8:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire),

  list<SuperColumn> get_slice_super_by_names(1:string keyspace, 2:string key, 3:string column_family, 4:list<binary> super_column_names, 5:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire),

  SuperColumn  get_super_column(1:string keyspace, 2:string key, 3:SuperColumnPath super_column_path, 4:ConsistencyLevel consistency_level=1)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  void     batch_insert_super_column(1:string keyspace, 2:BatchMutationSuper batch_mutation_super, 3:ConsistencyLevel consistency_level=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  # range query: returns matching keys
  list<string>   get_key_range(1:string keyspace, 2:string column_family, 3:string start="", 4:string finish="", 5:i32 count=100) 
  throws (1: InvalidRequestException ire),

  /////////////////////////////////////////////////////////////////////////////////////
  // The following are beta APIs being introduced for CLI and/or CQL support.        //
  // These are still experimental, and subject to change.                            //
  /////////////////////////////////////////////////////////////////////////////////////

  // get property whose value is of type "string"
  string         get_string_property(1:string property),

  // get property whose value is list of "strings"
  list<string>   get_string_list_property(1:string property),

  // describe specified keyspace
  map<string, map<string, string>>  describe_keyspace(1:string keyspace)
  throws (1: NotFoundException nfe),

  // execute a CQL query
  CqlResult    execute_query(1:string query)
}

