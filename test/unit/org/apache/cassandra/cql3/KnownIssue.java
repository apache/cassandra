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
    AF_MULTI_NODE_AND_NODE_LOCAL_WRITES("https://issues.apache.org/jira/browse/CASSANDRA-20243",
                                        "When writes are done at NODE_LOCAL and the select is ALL, AF should be able to return the correct data but it doesn't"),
    SHORT_AND_VARINT_GET_INT_FUNCTIONS("https://issues.apache.org/jira/browse/CASSANDRA-19874",
                                       "Function inference maybe unable to infer the correct function or chooses one for a smaller type"),
    SAI_EMPTY_TYPE("ML: Meaningless emptiness and filtering",
                   "Some types allow empty bytes, but define them as meaningless.  AF can be used to query them using <, <=, and =; but SAI can not")
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
