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
package org.apache.cassandra.index.sai.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.SchemaConstants;

import static org.junit.Assert.assertEquals;

public class AnalyzerViewTest extends SAITester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new AnalyzerView(SchemaConstants.VIRTUAL_VIEWS))));

        CQLTester.setUpClass();
    }

    @Test
    public void test() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM system_views.analyzer WHERE text = 'johnny apples seedlings'"+
                                           " AND json_analyzer = '[\n" +
                                           "\t{\"tokenizer\":\"whitespace\"},\n" +
                                           "\t{\"filter\":\"porterstem\"}\n" +
                                           "]' ALLOW FILTERING");
        UntypedResultSet.Row row = results.one();
        String tokenized = row.getString("tokens");

        assertEquals("[johnni, appl, seedl]", tokenized);
    }
}
