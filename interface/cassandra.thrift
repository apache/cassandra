#!/usr/local/bin/thrift --java --php

#
# Interface definition for Cassandra Service
#


namespace java  org.apache.cassandra.service
namespace cpp   org.apache.cassandra
php_namespace   cassandra

struct column_t {
   1: string                        columnName,
   2: string                        value,
   3: i64                           timestamp,
}

typedef map< string, list<column_t>  > column_family_map

struct batch_mutation_t {
   1: string                        table,
   2: string                        key,
   3: column_family_map             cfmap,
   4: column_family_map             cfmapdel,
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
   4: superColumn_family_map        cfmapdel,
}


typedef list<map<string, string>> resultSet_t

struct CqlResult_t {
   1: i32                           errorCode, // 0 - success
   2: string                        errorTxt,
   3: resultSet_t                   resultSet,
}

exception CassandraException {
  1: string error
 }

service Cassandra  {

  list<column_t> get_slice(string tablename,string key,string columnFamily_column, i32 start = -1 , i32 count = -1) throws (1:CassandraException e),
  list<column_t> get_slice_by_names(string tablename,string key,string columnFamily, list<string> columnNames) throws (1:CassandraException e),
  column_t       get_column(string tablename,string key,string columnFamily_column) throws (1:CassandraException e),
  i32            get_column_count(string tablename,string key,string columnFamily_column) throws (1:CassandraException e),
  async void     insert(string tablename,string key,string columnFamily_column, string cellData,i64 timestamp),
  async void     batch_insert(batch_mutation_t batchMutation),
  bool           batch_insert_blocking(batch_mutation_t batchMutation) throws (1:CassandraException e),
  bool           remove(string tablename,string key,string columnFamily_column, i64 timestamp, bool block),
  list<column_t> get_columns_since(string tablename, string key, string columnFamily_column, i64 timeStamp) throws (1:CassandraException e),

  list<superColumn_t> get_slice_super(string tablename, string key, string columnFamily_superColumnName, i32 start = -1 , i32 count = -1) throws (1:CassandraException e),
  list<superColumn_t> get_slice_super_by_names(string tablename, string key, string columnFamily, list<string> superColumnNames) throws (1:CassandraException e),
  superColumn_t       get_superColumn(string tablename,string key,string columnFamily) throws (1:CassandraException e),
  async void          batch_insert_superColumn(batch_mutation_super_t batchMutationSuper),
  bool                batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper) throws (1:CassandraException e),

  // Get the specified key into buffercache fData = true means get the data also for the key in buffercache.
  async void          touch(string key, bool fData),

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

