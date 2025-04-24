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

import java.util.EnumSet;

/**
 * In order to have a clean CI some known issues must be excluded from some tests until those issues are addressed.
 *
 * This type exists to make it easier to descover known issues and places in the code that account for them
 */
public enum KnownIssue
{
    BETWEEN_START_LARGER_THAN_END("https://issues.apache.org/jira/browse/CASSANDRA-20154",
                                  "BETWEEN is matching values when start > end, which should never return anything"),
    SAI_INET_MIXED("https://issues.apache.org/jira/browse/CASSANDRA-19492",
                   "SAI converts ipv4 to ipv6 to simplify the index, this causes issues with range search as it starts to mix the values, which isn't always desirable or intuative"),
    CUSTOM_INDEX_MAX_COLUMN_48("https://issues.apache.org/jira/browse/CASSANDRA-19897",
                               "Columns can be up to 50 chars, but CREATE CUSTOM INDEX only allows up to 48"),
    SHORT_AND_VARINT_GET_INT_FUNCTIONS("https://issues.apache.org/jira/browse/CASSANDRA-19874",
                                       "Function inference maybe unable to infer the correct function or chooses one for a smaller type"),
    SAI_EMPTY_TYPE("ML: Meaningless emptiness and filtering",
                   "Some types allow empty bytes, but define them as meaningless.  AF can be used to query them using <, <=, and =; but SAI can not"),
    AF_MULTI_NODE_MULTI_COLUMN_AND_NODE_LOCAL_WRITES("https://issues.apache.org/jira/browse/CASSANDRA-19007",
                                                     "When doing multi node/multi column queries, AF can miss data when the nodes are not in-sync"),
    SAI_AND_VECTOR_COLUMNS("https://issues.apache.org/jira/browse/CASSANDRA-20464",
                           "When doing an SAI query, if the where clause also contains a vector column bad results can be produced"),
    CAS_CONDITION_ON_UDT_W_EMPTY_BYTES("https://issues.apache.org/jira/browse/CASSANDRA-20479",
                                       "WHERE clause blocks operations on UDTs but CAS allows in IF clause.  During this path empty can be confused with null which allows non-existing rows to match empty bytes"),
    CAS_ON_STATIC_ROW("",
                      "When you do a CAS to the partition level the read is SELECT statics LIMIT 1, if the CAS doesn't apply the response includes the first row in the partition with its values redacted... this statement is partition level and not row level, would expect just the applied column like the other cases where the static row isn't present"),
    STATIC_LIST_APPEND_WITH_CLUSTERING_IN("",
                                          "When an 'UPDATE SET s += [0] WHERE pk = ? AND ck IN (?, ?)' happens the static operation happens twice, so the list append adds 2 elements!"),
    ACCORD_JOURNAL_SUPPORT_DROP_TABLE("",
                                      "When DROP TABLE is done, this is currently not plumbed through to Journals snapshot logic, which leads to unknown keyspace/table errors"),
    ;

    KnownIssue(String url, String description)
    {
        // don't actually care to save the values, just there to force documentation
    }

    public static EnumSet<KnownIssue> ignoreAll()
    {
        return EnumSet.allOf(KnownIssue.class);
    }
}
