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


#
# structures
#

struct column_t {
   1: string                        columnName,
   2: binary                        value,
   3: i64                           timestamp,
}

typedef map< string, list<column_t>  > column_family_map

struct batch_mutation_t {
   1: string                        table,
   2: string                        key,
   3: column_family_map             cfmap,
}

struct superColumn_t {
   1: string                        name,
   2: list<column_t>                columns,
}

typedef map< string, list<superColumn_t>  > superColumn_family_map

struct batch_mutation_super_t {
   1: string                        table,
   2: string                        key,
   3: superColumn_family_map        cfmap,
}


typedef list<map<string, string>> resultSet_t

struct CqlResult_t {
   1: i32                           errorCode, // 0 - success
   2: string                        errorTxt,
   3: resultSet_t                   resultSet,
}


#
# Exceptions
#

# a specific column was requested that does not exist
exception NotFoundException {
}

# invalid request (table / CF does not exist, etc.)
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

# CF = ColumnFamily name
# SC = SuperColumn name
# C = Column name
# columnParent: the parent of the columns you are specifying.  "CF" or "CF:SC".
# columnPath: full path to a column.  "CF:C" or "CF:SC:C".
# superColumnPath: full path to a supercolumn.  "CF:SC" only.
# columnPathOrParent: remove will wipe out any layer.  "CF" or "CF:C" or "CF:SC" or "CF:SC:C".


service Cassandra {
  list<column_t> get_slice_by_name_range(1:string tablename, 2:string key, 3:string columnParent, 4:string start, 5:string finish, 6:i32 count=100)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  
  list<column_t> get_slice_by_names(1:string tablename, 2:string key, 3:string columnParent, 4:list<string> columnNames)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  
  list<column_t> get_slice(1:string tablename, 2:string key, 3:string columnParent, 4:bool isAscending, 5:i32 offset, 6:i32 count=100)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  column_t       get_column(1:string tablename, 2:string key, 3:string columnPath)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  i32            get_column_count(1:string tablename, 2:string key, 3:string columnParent)
  throws (1: InvalidRequestException ire),

  void     insert(1:string tablename, 2:string key, 3:string columnPath, 4:binary cellData, 5:i64 timestamp, 6:i32 block_for=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  void     batch_insert(1: batch_mutation_t batchMutation, 2:i32 block_for=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  void           remove(1:string tablename, 2:string key, 3:string columnPathOrParent, 4:i64 timestamp, 5:i32 block_for=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  list<column_t> get_columns_since(1:string tablename, 2:string key, 3:string columnParent, 4:i64 timeStamp)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  list<superColumn_t> get_slice_super(1:string tablename, 2:string key, 3:string columnFamily, 4:bool isAscending, 5:i32 offset, 6:i32 count=100)
  throws (1: InvalidRequestException ire),

  list<superColumn_t> get_slice_super_by_names(1:string tablename, 2:string key, 3:string columnFamily, 4:list<string> superColumnNames)
  throws (1: InvalidRequestException ire),

  superColumn_t  get_superColumn(1:string tablename, 2:string key, 3:string superColumnPath)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  void     batch_insert_superColumn(1:batch_mutation_super_t batchMutationSuper, 2:i32 block_for=0)
  throws (1: InvalidRequestException ire, 2: UnavailableException ue),

  # range query: returns matching keys
  list<string>   get_key_range(1:string tablename, 2:list<string> columnFamilies=[], 3:string startWith="", 4:string stopAt="", 5:i32 maxResults=100) throws (1: InvalidRequestException ire),

  /////////////////////////////////////////////////////////////////////////////////////
  // The following are beta APIs being introduced for CLI and/or CQL support.        //
  // These are still experimental, and subject to change.                            //
  /////////////////////////////////////////////////////////////////////////////////////

  // get property whose value is of type "string"
  string         getStringProperty(1:string propertyName),

  // get property whose value is list of "strings"
  list<string>   getStringListProperty(1:string propertyName),

  // describe specified table
  map<string, map<string, string>>  describeTable(1:string tableName)
  throws (1: NotFoundException nfe),

  // execute a CQL query
  CqlResult_t    executeQuery(1:string query)
}

