/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.stress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StressYaml
{
    public String keyspace;
    public String keyspace_definition;
    public String table;
    public String table_definition;

    public List<String> extra_definitions;

    public List<Map<String, Object>> columnspec;
    public Map<String, QueryDef> queries;
    public Map<String, String> insert;
    public Map<String, TokenRangeQueryDef> token_range_queries = new HashMap<>();

    public static class QueryDef
    {
        public String cql;
        public String fields;
        public String getConfigAsString()
        {
            return String.format("CQL:%s;Fields:%s;", cql, fields);
        }
    }

    public static class TokenRangeQueryDef
    {
        public String columns;
        public int page_size = 5000;
        public String getConfigAsString()
        {
            return String.format("Columns:%s;", columns);
        }
    }

}
