/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

class ReservedKeywords
{
    private static final String[] reservedKeywords = new String[]
                                                     {
                                                     "SELECT",
                                                     "FROM",
                                                     "WHERE",
                                                     "AND",
                                                     "KEY",
                                                     "ENTRIES",
                                                     "FULL",
                                                     "INSERT",
                                                     "UPDATE",
                                                     "WITH",
                                                     "LIMIT",
                                                     "USING",
                                                     "USE",
                                                     "COUNT",
                                                     "SET",
                                                     "BEGIN",
                                                     "UNLOGGED",
                                                     "BATCH",
                                                     "APPLY",
                                                     "TRUNCATE",
                                                     "DELETE",
                                                     "IN",
                                                     "CREATE",
                                                     "KEYSPACE",
                                                     "SCHEMA",
                                                     "COLUMNFAMILY",
                                                     "TABLE",
                                                     "MATERIALIZED",
                                                     "VIEW",
                                                     "INDEX",
                                                     "ON",
                                                     "TO",
                                                     "DROP",
                                                     "PRIMARY",
                                                     "INTO",
                                                     "TIMESTAMP",
                                                     "TTL",
                                                     "ALTER",
                                                     "RENAME",
                                                     "ADD",
                                                     "ORDER",
                                                     "BY",
                                                     "ASC",
                                                     "DESC",
                                                     "ALLOW",
                                                     "IF",
                                                     "IS",
                                                     "GRANT",
                                                     "OF",
                                                     "REVOKE",
                                                     "MODIFY",
                                                     "AUTHORIZE",
                                                     "DESCRIBE",
                                                     "EXECUTE",
                                                     "NORECURSIVE",
                                                     "ASCII",
                                                     "BIGINT",
                                                     "BLOB",
                                                     "BOOLEAN",
                                                     "COUNTER",
                                                     "DECIMAL",
                                                     "DOUBLE",
                                                     "FLOAT",
                                                     "INET",
                                                     "INT",
                                                     "SMALLINT",
                                                     "TINYINT",
                                                     "TEXT",
                                                     "UUID",
                                                     "VARCHAR",
                                                     "VARINT",
                                                     "TIMEUUID",
                                                     "TOKEN",
                                                     "WRITETIME",
                                                     "DATE",
                                                     "TIME",
                                                     "NULL",
                                                     "NOT",
                                                     "NAN",
                                                     "INFINITY",
                                                     "OR",
                                                     "REPLACE" };

    private static final Set<String> reservedSet = ImmutableSet.copyOf(reservedKeywords);

    static boolean isReserved(String text)
    {
        return reservedSet.contains(text.toUpperCase());
    }
}
