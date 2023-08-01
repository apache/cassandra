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
package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableList;

public class SchemaKeyspaceTables
{
    public static final String KEYSPACES = "keyspaces";
    public static final String TABLES = "tables";
    public static final String COLUMNS = "columns";
    public static final String COLUMN_MASKS = "column_masks";
    public static final String DROPPED_COLUMNS = "dropped_columns";
    public static final String TRIGGERS = "triggers";
    public static final String VIEWS = "views";
    public static final String TYPES = "types";
    public static final String FUNCTIONS = "functions";
    public static final String AGGREGATES = "aggregates";
    public static final String INDEXES = "indexes";
 
    /**
     * The order in this list matters.
     *
     * When flushing schema tables, we want to flush them in a way that mitigates the effects of an abrupt shutdown whilst
     * the tables are being flushed. On startup, we load the schema from disk before replaying the CL, so we need to
     * try to avoid problems like reading a table without columns or types, for example. So columns and types should be
     * flushed before tables, which should be flushed before keyspaces.
     *
     * When truncating, the order should be reversed. For immutable lists this is an efficient operation that simply
     * iterates in reverse order.
     *
     * See CASSANDRA-12213 for more details.
     */
    public static final ImmutableList<String> ALL = ImmutableList.of(COLUMN_MASKS,
                                                                     COLUMNS,
                                                                     DROPPED_COLUMNS,
                                                                     TRIGGERS,
                                                                     TYPES,
                                                                     FUNCTIONS,
                                                                     AGGREGATES,
                                                                     INDEXES,
                                                                     TABLES,
                                                                     VIEWS,
                                                                     KEYSPACES);

}
