#!/usr/local/bin/thrift --java --php --py

#
# Interface definition for Cassandra Service
#

namespace java org.apache.cassandra.service
namespace py org.apache.cassandra
namespace cpp org.apache.cassandra
namespace php org.apache.cassandra


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


#
# service api
#

service Cassandra {
  list<column_t> get_slice(1:string tablename, 2:string key, 3:string columnFamily_column, 4:i32 start=-1, 5:i32 count=-1)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  
  list<column_t> get_slice_by_names(1:string tablename, 2:string key, 3:string columnFamily, 4:list<string> columnNames)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  
  column_t       get_column(1:string tablename, 2:string key, 3:string columnFamily_column)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  i32            get_column_count(1:string tablename, 2:string key, 3:string columnFamily_column)
  throws (1: InvalidRequestException ire),

  async void     insert(1:string tablename, 2:string key, 3:string columnFamily_column, 4:binary cellData, 5:i64 timestamp),

  async void     batch_insert(batch_mutation_t batchMutation),

  bool           insert_blocking(1:string tablename, 2:string key, 3:string columnFamily_column, 4:binary cellData, 5:i64 timestamp)
  throws (1: InvalidRequestException ire),

  bool           batch_insert_blocking(batch_mutation_t batchMutation)
  throws (1: InvalidRequestException ire),

  bool           remove(1:string tablename, 2:string key, 3:string columnFamily_column, 4:i64 timestamp, 5:bool block)
  throws (1: InvalidRequestException ire),

  list<column_t> get_columns_since(1:string tablename, 2:string key, 3:string columnFamily_column, 4:i64 timeStamp)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  list<superColumn_t> get_slice_super(1:string tablename, 2:string key, 3:string columnFamily_superColumnName, 4:i32 start=-1, 5:i32 count=-1)
  throws (1: InvalidRequestException ire),

  list<superColumn_t> get_slice_super_by_names(1:string tablename, 2:string key, 3:string columnFamily, 4:list<string> superColumnNames)
  throws (1: InvalidRequestException ire),

  superColumn_t  get_superColumn(1:string tablename, 2:string key, 3:string columnFamily)
  throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  async void     batch_insert_superColumn(batch_mutation_super_t batchMutationSuper),

  bool           batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper)
  throws (1: InvalidRequestException ire),

  async void     touch(1:string key, 2:bool fData),

  # range query: returns matching keys
  list<string>   get_key_range(1:string tablename, 2:string startWith="", 3:string stopAt="", 4:i32 maxResults=1000) throws (1: InvalidRequestException ire),

  /////////////////////////////////////////////////////////////////////////////////////
  // The following are beta APIs being introduced for CLI and/or CQL support.        //
  // These are still experimental, and subject to change.                            //
  /////////////////////////////////////////////////////////////////////////////////////

  // get property whose value is of type "string"
  string         getStringProperty(string propertyName),

  // get property whose value is list of "strings"
  list<string>   getStringListProperty(string propertyName),

  // describe specified table
  string         describeTable(string tableName),

  // execute a CQL query
  CqlResult_t    executeQuery(string query)
}

