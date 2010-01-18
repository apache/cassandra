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

# Thrift.rb has a bug where top-level modules that include modules 
# with the same name are not properly referenced, so we can't do
# Cassandra::Cassandra::Client.
namespace rb CassandraThrift


#
# constants
#

# for clients checking that server and it have same thrift definitions.
# no promises are made other than "if both are equal, you're good."
# in particular, don't try to parse numeric information out and assume
# that a "greater" version is a superset of a "smaller" one.
const string VERSION = "0.5.0"


#
# data structures
#

/** Basic unit of data within a ColumnFamily.
 * @param name. A column name can act both as structure (a label) or as data (like value). Regardless, the name of the column
 *        is used as a key to its value.
 * @param value. Some data
 * @param timestamp. Used to record when data was sent to be written.
 */
struct Column {
   1: required binary name,
   2: required binary value,
   3: required i64 timestamp,
}

/** A named list of columns.
 * @param name. see Column.name.
 * @param columns. A collection of standard Columns.  The columns within a super column are defined in an adhoc manner.
 *                 Columns within a super column do not have to have matching structures (similarly named child columns).
 */
struct SuperColumn {
   1: required binary name,
   2: required list<Column> columns,
}

/**
    Methods for fetching rows/records from Cassandra will return either a single instance of ColumnOrSuperColumn or a list
    of ColumnOrSuperColumns (get_slice()). If you're looking up a SuperColumn (or list of SuperColumns) then the resulting
    instances of ColumnOrSuperColumn will have the requested SuperColumn in the attribute super_column. For queries resulting
    in Columns, those values will be in the attribute column. This change was made between 0.3 and 0.4 to standardize on
    single query methods that may return either a SuperColumn or Column.

    @param column. The Column returned by get() or get_slice().
    @param super_column. The SuperColumn returned by get() or get_slice().
 */
struct ColumnOrSuperColumn {
    1: optional Column column,
    2: optional SuperColumn super_column,
}


#
# Exceptions
# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)
#

/** A specific column was requested that does not exist. */
exception NotFoundException {
}

/** Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter is malformed. 
    why contains an associated error message.
*/
exception InvalidRequestException {
    1: required string why
}

/** Not all the replicas required could be created and/or read. */
exception UnavailableException {
}

/** RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large. */
exception TimedOutException {
}


#
# service api
#
/** The ConsistencyLevel is an enum that controls both read and write behavior based on <ReplicationFactor> in your
 * storage-conf.xml. The different consistency levels have different meanings, depending on if you're doing a write or read
 * operation. Note that if W + R > ReplicationFactor, where W is the number of nodes to block for on write, and R
 * the number to block for on reads, you will have strongly consistent behavior; that is, readers will always see the most
 * recent write. Of these, the most interesting is to do QUORUM reads and writes, which gives you consistency while still
 * allowing availability in the face of node failures up to half of <ReplicationFactor>. Of course if latency is more
 * important than consistency then you can use lower values for either or both.
 *
 * Write:
 *      ZERO    Ensure nothing. A write happens asynchronously in background
 *      ONE     Ensure that the write has been written to at least 1 node's commit log and memory table before responding to the client.
 *      QUORUM  Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes before responding to the client.
 *      ALL     Ensure that the write is written to <code>&lt;ReplicationFactor&gt;</code> nodes before responding to the client.
 *
 * Read:
 *      ZERO    Not supported, because it doesn't make sense.
 *      ONE     Will return the record returned by the first node to respond. A consistency check is always done in a
 *              background thread to fix any consistency issues when ConsistencyLevel.ONE is used. This means subsequent
 *              calls will have correct data even if the initial read gets an older value. (This is called 'read repair'.)
 *      QUORUM  Will query all storage nodes and return the record with the most recent timestamp once it has at least a
 *              majority of replicas reported. Again, the remaining replicas will be checked in the background.
 *      ALL     Not yet supported, but we plan to eventually.
*/
enum ConsistencyLevel {
    ZERO = 0,
    ONE = 1,
    QUORUM = 2,
    DCQUORUM = 3,
    DCQUORUMSYNC = 4,
    ALL = 5,
}

/**
    ColumnParent is used when selecting groups of columns from the same ColumnFamily. In directory structure terms, imagine
    ColumnParent as ColumnPath + '/../'.

    See also <a href="cassandra.html#Struct_ColumnPath">ColumnPath</a>
 */
struct ColumnParent {
    3: required string column_family,
    4: optional binary super_column,
}

/** The ColumnPath is the path to a single column in Cassandra. It might make sense to think of ColumnPath and
 * ColumnParent in terms of a directory structure.
 *
 * ColumnPath is used to looking up a single column.
 *
 * @param column_family. The name of the CF of the column being looked up.
 * @param super_column. The super column name.
 * @param column. The column name.
 */
struct ColumnPath {
    3: required string column_family,
    4: optional binary super_column,
    5: optional binary column,
}

/**
    A slice range is a structure that stores basic range, ordering and limit information for a query that will return
    multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY

    @param start. The column name to start the slice with. This attribute is not required, though there is no default value,
                  and can be safely set to '', i.e., an empty byte array, to start with the first column name. Otherwise, it
                  must a valid value under the rules of the Comparator defined for the given ColumnFamily.
    @param finish. The column name to stop the slice at. This attribute is not required, though there is no default value,
                   and can be safely set to an empty byte array to not stop until 'count' results are seen. Otherwise, it
                   must also be a value value to the ColumnFamily Comparator.
    @param reversed. Whether the results should be ordered in reversed order. Similar to ORDER BY blah DESC in SQL.
    @param count. How many keys to return. Similar to LIMIT 100 in SQL. May be arbitrarily large, but Thrift will
                  materialize the whole result into memory before returning it to the client, so be aware that you may
                  be better served by iterating through slices by passing the last value of one call in as the 'start'
                  of the next instead of increasing 'count' arbitrarily large.
 */
struct SliceRange {
    1: required binary start,
    2: required binary finish,
    3: required bool reversed=0,
    4: required i32 count=100,
}

/**
    A SlicePredicate is similar to a mathematic predicate (see http://en.wikipedia.org/wiki/Predicate_(mathematical_logic)),
    which is described as "a property that the elements of a set have in common."

    SlicePredicate's in Cassandra are described with either a list of column_names or a SliceRange.  If column_names is
    specified, slice_range is ignored.

    @param column_name. A list of column names to retrieve. This can be used similar to Memcached's "multi-get" feature
                        to fetch N known column names. For instance, if you know you wish to fetch columns 'Joe', 'Jack',
                        and 'Jim' you can pass those column names as a list to fetch all three at once.
    @param slice_range. A SliceRange describing how to range, order, and/or limit the slice.
 */
struct SlicePredicate {
    1: optional list<binary> column_names,
    2: optional SliceRange   slice_range,
}

/**
    A KeySlice is key followed by the data it maps to. A collection of KeySlice is returned by the get_range_slice operation.

    @param key. a row key
    @param columns. List of data represented by the key. Typically, the list is pared down to only the columns specified by
                    a SlicePredicate.
 */
struct KeySlice {
    1: required string key,
    2: required list<ColumnOrSuperColumn> columns,
}

service Cassandra {
  # retrieval methods

  /**
    Get the Column or SuperColumn at the given column_path. If no value is present, NotFoundException is thrown. (This is
    the only method that can throw an exception under non-failure conditions.)
   */
  ColumnOrSuperColumn get(1:required string keyspace,
                          2:required string key,
                          3:required ColumnPath column_path,
                          4:required ConsistencyLevel consistency_level=1)
                      throws (1:InvalidRequestException ire, 2:NotFoundException nfe, 3:UnavailableException ue, 4:TimedOutException te),

  /**
    Get the group of columns contained by column_parent (either a ColumnFamily name or a ColumnFamily/SuperColumn name
    pair) specified by the given SlicePredicate. If no matching values are found, an empty list is returned.
   */
  list<ColumnOrSuperColumn> get_slice(1:required string keyspace, 
                                      2:required string key, 
                                      3:required ColumnParent column_parent, 
                                      4:required SlicePredicate predicate, 
                                      5:required ConsistencyLevel consistency_level=1)
                            throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Perform a get for column_path in parallel on the given list<string> keys. The return value maps keys to the
    ColumnOrSuperColumn found. If no value corresponding to a key is present, the key will still be in the map, but both
    the column and super_column references of the ColumnOrSuperColumn object it maps to will be null.  
  */
  map<string,ColumnOrSuperColumn> multiget(1:required string keyspace, 
                                           2:required list<string> keys, 
                                           3:required ColumnPath column_path, 
                                           4:required ConsistencyLevel consistency_level=1)
                                  throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Performs a get_slice for column_parent and predicate for the given keys in parallel.
  */
  map<string,list<ColumnOrSuperColumn>> multiget_slice(1:required string keyspace, 
                                                       2:required list<string> keys, 
                                                       3:required ColumnParent column_parent, 
                                                       4:required SlicePredicate predicate, 
                                                       5:required ConsistencyLevel consistency_level=1)
                                        throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    returns the number of columns for a particular <code>key</code> and <code>ColumnFamily</code> or <code>SuperColumn</code>.
  */
  i32 get_count(1:required string keyspace, 
                2:required string key, 
                3:required ColumnParent column_parent, 
                4:required ConsistencyLevel consistency_level=1)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /** @deprecated; use get_range_slice instead */
  list<string> get_key_range(1:required string keyspace, 
                             2:required string column_family, 
                             3:required string start="", 
                             4:required string finish="", 
                             5:required i32 count=100,
                             6:required ConsistencyLevel consistency_level=1)
               throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   returns a subset of columns for a range of keys.
  */
  list<KeySlice> get_range_slice(1:required string keyspace, 
                                 2:required ColumnParent column_parent, 
                                 3:required SlicePredicate predicate,
                                 4:required string start_key="", 
                                 5:required string finish_key="", 
                                 6:required i32 row_count=100, 
                                 7:required ConsistencyLevel consistency_level=1)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  # modification methods

  /**
    Insert a Column consisting of (column_path.column, value, timestamp) at the given column_path.column_family and optional
    column_path.super_column. Note that column_path.column is here required, since a SuperColumn cannot directly contain binary
    values -- it can only contain sub-Columns. 
   */
  void insert(1:required string keyspace, 
              2:required string key, 
              3:required ColumnPath column_path, 
              4:required binary value, 
              5:required i64 timestamp, 
              6:required ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Insert Columns or SuperColumns across different Column Families for the same row key. batch_mutation is a
    map<string, list<ColumnOrSuperColumn>> -- a map which pairs column family names with the relevant ColumnOrSuperColumn
    objects to insert.
   */
  void batch_insert(1:required string keyspace, 
                    2:required string key, 
                    3:required map<string, list<ColumnOrSuperColumn>> cfmap, 
                    4:required ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp. Note
    that all the values in column_path besides column_path.column_family are truly optional: you can remove the entire
    row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column by specifying those levels too.
   */
  void remove(1:required string keyspace,
              2:required string key, 
              3:required ColumnPath column_path,
              4:required i64 timestamp,
              5:ConsistencyLevel consistency_level=0)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),


  // Meta-APIs -- APIs to get information about the node or cluster,
  // rather than user data.  The nodeprobe program provides usage examples.

  /** get property whose value is of type string. */
  string get_string_property(1:required string property),

  /** get property whose value is list of strings. */
  list<string> get_string_list_property(1:required string property),

  /** describe specified keyspace */
  map<string, map<string, string>> describe_keyspace(1:required string keyspace)
                                   throws (1:NotFoundException nfe),
}

