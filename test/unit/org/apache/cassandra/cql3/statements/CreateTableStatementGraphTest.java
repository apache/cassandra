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

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateTableStatementGraphTest extends CQLTester
{
    @Parameterized.Parameters(name = "tableOptions = {0}")
    public static Set<String> tableOptions()
    {
        return ImmutableSet.of(
            "VERTEX LABEL",
            "vertex label",
            "VERTEX LABEL person_label",
            "VERTEX LABEL personlabel",
            "VERTEX LABEL \"personlabel\"",
            "VERTEX LABEL personlabel AND CLUSTERING ORDER BY (v DESC)",
            "CLUSTERING ORDER BY (v DESC) AND VERTEX LABEL",
            "EDGE LABEL person_authored_book FROM person(name,person_id) TO book(name, book_id,  cover)",
            "EDGE LABEL person_authored_book FROM person(name) TO book(cover)",
            "VERTEX LABEL AND EDGE LABEL person_authored_book FROM person(name) TO book(cover)"
        );
    }

    @Parameterized.Parameter()
    public String tableOptions;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
    }

    @Test
    public void dseGraphShouldBeIgnoredWithWarning() throws Throwable
    {
        String tableName = createTableName();

        // should not throw
        ResultSet rows = executeNet(String.format("CREATE TABLE ks.%s (k int, v int, PRIMARY KEY (k, v)) WITH %s", tableName, tableOptions));

        assertTrue(rows.wasApplied());

        String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
        assertThat(warning, containsString("The unsupported graph table property was ignored"));

        assertNoGraphLabels(tableName);
    }

    private void assertNoGraphLabels(String tableName) throws Throwable
    {
        ResultSet result = executeNet("DESCRIBE TABLE ks." + tableName);

        String createStatement = result.one().getString("create_statement");
        assertThat(createStatement.toUpperCase(), not(containsString("LABEL")));
    }
}
