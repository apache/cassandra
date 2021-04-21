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

package org.apache.cassandra.cql3.statements;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class CreateIndexStatementTest extends CQLTester
{
    @Parameterized.Parameters(name = "index = {0}")
    public static Set<String> dseIndexes()
    {
        return CreateIndexStatement.DSE_INDEXES;
    }

    @Parameterized.Parameter()
    public String indexClass;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c))");
    }

    private void assertNoIndex(String indexName) throws Throwable
    {
        try
        {
            executeNet("DESCRIBE INDEX ks." + indexName);
            fail("Expected InvalidQueryException caused by a missing index");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage().contains(indexName + "' not found"));
        }
    }

    @Test
    public void dseIndexCreationShouldBeIgonerWithWarning() throws Throwable
    {
        // should not throw
        ResultSet rows = executeNet(String.format("CREATE CUSTOM INDEX index_name ON ks.tbl (v) USING '%s'", indexClass));

        assertTrue(rows.wasApplied()); // the command is ignored

        String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
        assertTrue("Custom DSE index creation should cause a warning", warning.contains("DSE custom index"));

        assertNoIndex("index_name");
    }
}
