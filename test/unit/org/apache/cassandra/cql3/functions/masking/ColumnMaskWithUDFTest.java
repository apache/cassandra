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

package org.apache.cassandra.cql3.functions.masking;

import org.junit.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertNull;

/**
 * {@link ColumnMaskTester} for column masks using a UDF.
 */
public class ColumnMaskWithUDFTest extends ColumnMaskTester
{
    @Test
    public void testUDF() throws Throwable
    {
        // create a table masked with a UDF and with a materialized view
        String function = createAddFunction();
        createTable(format("CREATE TABLE %%s (k int, c int, v int MASKED WITH %s(7), PRIMARY KEY (k, c))", function));
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL " +
                                 "PRIMARY KEY (v, k, c)");

        // add some data
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 10)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 20)");
        Object[][] clearRows = rows(row(0, 0, 10), row(0, 1, 20));
        Object[][] maskedRows = rows(row(0, 0, 17), row(0, 1, 27));
        waitForViewMutations();

        Session session = sessionNet();

        // query the table and verify that the column mask is applied
        String tableQuery = "SELECT k, c, v FROM %s";
        assertRowsNet(executeNet(tableQuery), maskedRows);
        BoundStatement tablePrepared = session.prepare(formatQuery(tableQuery)).bind();
        assertRowsNet(session.execute(tablePrepared), maskedRows);

        // query the view and verify that the column mask is applied
        String viewQuery = format("SELECT k, c, v FROM %s.%s", KEYSPACE, view);
        BoundStatement viewPrepared = session.prepare(formatQuery(tableQuery)).bind();
        assertRowsNet(executeNet(viewQuery), maskedRows);
        assertRowsNet(session.execute(viewPrepared), maskedRows);

        // drop the column mask and verify that the column is not masked anymore
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
        assertRowsNet(executeNet(tableQuery), clearRows);
        assertRowsNet(executeNet(viewQuery), clearRows);
        assertRowsNet(session.execute(tablePrepared), clearRows);
        assertRowsNet(session.execute(viewPrepared), clearRows);

        // mask the column again, but this time with a different argument
        alterTable(format("ALTER TABLE %%s ALTER v MASKED WITH %s(8)", function));
        maskedRows = rows(row(0, 0, 18), row(0, 1, 28));
        assertRowsNet(executeNet(tableQuery), maskedRows);
        assertRowsNet(executeNet(viewQuery), maskedRows);
        assertRowsNet(session.execute(tablePrepared), maskedRows);
        assertRowsNet(session.execute(viewPrepared), maskedRows);
    }

    @Test
    public void testUDFWithReversed() throws Throwable
    {
        // create a table masked with a UDF and with a materialized view
        String function = createAddFunction();
        createTable(format("CREATE TABLE %%s (k int, c int MASKED WITH %s(100), v int, PRIMARY KEY (k, c)) " +
                           "WITH CLUSTERING ORDER BY (c DESC)", function));
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL " +
                                 "PRIMARY KEY (v, c, K) " +
                                 "WITH CLUSTERING ORDER BY (c DESC, k ASC)");

        // add some data
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 0)");
        Object[][] clearRows = rows(row(0, 2, 0), row(0, 1, 0));
        Object[][] maskedRows = rows(row(0, 102, 0), row(0, 101, 0));
        waitForViewMutations();

        // query the table and verify that the column mask is applied
        String tableQuery = "SELECT k, c, v FROM %s";
        assertRowsNet(executeNet(tableQuery), maskedRows);

        // query the view and verify that the column mask is applied
        String viewQuery = format("SELECT k, c, v FROM %s.%s", KEYSPACE, view);
        assertRowsNet(executeNet(viewQuery), maskedRows);

        // drop the column mask and verify that the column is not masked anymore
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        assertRowsNet(executeNet(tableQuery), clearRows);
        assertRowsNet(executeNet(viewQuery), clearRows);

        // mask the column again, but this time with a different argument
        alterTable(format("ALTER TABLE %%s ALTER c MASKED WITH %s(1000)", function));
        maskedRows = rows(row(0, 1002, 0), row(0, 1001, 0));
        assertRowsNet(executeNet(tableQuery), maskedRows);
        assertRowsNet(executeNet(viewQuery), maskedRows);
    }

    /**
     * Verifies that queries dropping a UDF that is used for masking columns are rejected.
     */
    @Test
    public void testDropUDFWithDependentMasks() throws Throwable
    {
        String function = createAddFunction();
        String mask = format("MASKED WITH %s(7)", function);
        String table1 = createTable(format("CREATE TABLE %%s (k int %s PRIMARY KEY, v int %<s)", mask));
        String table2 = createTable(format("CREATE TABLE %%s (k int %s, c int %<s, v int %<s, s int static %<s, PRIMARY KEY (k, c))", mask));

        String dropFunctionQuery = format("DROP FUNCTION %s", function);

        String message = format("Function '%s' is still referenced by column masks in tables %s, %s", function, table1, table2);
        assertInvalidThrowMessage(message, InvalidRequestException.class, dropFunctionQuery);

        alterTable(format("ALTER TABLE %s.%s ALTER k DROP MASKED", KEYSPACE, table1));
        alterTable(format("ALTER TABLE %s.%s ALTER k DROP MASKED", KEYSPACE, table2));
        assertInvalidThrowMessage(message, InvalidRequestException.class, dropFunctionQuery);

        alterTable(format("ALTER TABLE %s.%s ALTER v DROP MASKED", KEYSPACE, table1));
        alterTable(format("ALTER TABLE %s.%s ALTER v DROP MASKED", KEYSPACE, table2));
        message = format("Function '%s' is still referenced by column masks in tables %s", function, table2);
        assertInvalidThrowMessage(message, InvalidRequestException.class, dropFunctionQuery);

        alterTable(format("ALTER TABLE %s.%s ALTER c DROP MASKED", KEYSPACE, table2));
        assertInvalidThrowMessage(message, InvalidRequestException.class, dropFunctionQuery);

        alterTable(format("ALTER TABLE %s.%s ALTER s DROP MASKED", KEYSPACE, table2));
        schemaChange(dropFunctionQuery);
    }

    @Test
    public void testMissingUDF() throws Throwable
    {
        assertMaskingFails("Unable to find masking function for v, no declared function matches the signature %s()",
                           "missing_udf");
    }

    @Test
    public void testUDFWithNoArguments() throws Throwable
    {
        assertMaskingFails("Invalid number of arguments in call to function %s: 0 required but 1 provided",
                           createFunction(KEYSPACE,
                                          "",
                                          "CREATE FUNCTION %s () " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS int " +
                                          "LANGUAGE java " +
                                          "AS 'return 42;'"));
    }

    @Test
    public void testUDFWithWrongArgumentCount() throws Throwable
    {
        assertMaskingFails("Invalid number of arguments in call to function %s: 2 required but 3 provided",
                           createAddFunction(),
                           "(1, 3)");
    }

    @Test
    public void testUDFWithWrongArgumentType() throws Throwable
    {
        assertMaskingFails("Type error: org.apache.cassandra.db.marshal.Int32Type " +
                           "cannot be passed as argument 0 of function %s of type text",
                           createFunction(KEYSPACE,
                                          "text",
                                          "CREATE FUNCTION %s (a text) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return \"redacted\";'"));
    }

    @Test
    public void testUDFWithWrongReturnType() throws Throwable
    {
        assertMaskingFails("Masking function %s() return type is text",
                           createFunction(KEYSPACE,
                                          "int",
                                          "CREATE FUNCTION %s (a int) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return \"redacted\";'"));
    }

    @Test
    public void testUDFInOtherKeyspace() throws Throwable
    {
        assertMaskingFails("Masking function %s() doesn't belong to the same keyspace as the table",
                           createFunction(KEYSPACE_PER_TEST,
                                          "int",
                                          "CREATE FUNCTION %s (input int) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS int " +
                                          "LANGUAGE java " +
                                          "AS 'return Integer.valueOf(input);'"));
    }

    @Test
    public void testUDA() throws Throwable
    {
        String fState = createAddFunction();
        String fFinal = createIdentityFunction();
        assertMaskingFails("Aggregate function %s() cannot be used for masking table columns",
                           createAggregate(KEYSPACE,
                                           "int",
                                           "CREATE AGGREGATE %s(int) " +
                                           "SFUNC " + shortFunctionName(fState) + " " +
                                           "STYPE int " +
                                           "FINALFUNC " + shortFunctionName(fFinal) + " " +
                                           "INITCOND 42"));
    }

    private void assertMaskingFails(String message, String function) throws Throwable
    {
        assertMaskingFails(message, function, "()");
    }

    private void assertMaskingFails(String message, String function, String arguments) throws Throwable
    {
        message = format(message, function);

        // create table should fail
        Assertions.assertThatThrownBy(() -> execute(format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int MASKED WITH %s%s)",
                                                           KEYSPACE, createTableName(), function, arguments)))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(message);
        assertNull(currentTableMetadata());

        // alter table should fail
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        Assertions.assertThatThrownBy(() -> execute(format("ALTER TABLE %s.%s ALTER v MASKED WITH %s%s",
                                                           KEYSPACE, table, function, arguments)))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(message);
        assertColumnIsNotMasked(table, "v");
    }

    private String createAddFunction() throws Throwable
    {
        return createFunction(KEYSPACE,
                              "int, int",
                              "CREATE FUNCTION %s (a int, b int) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java " +
                              "AS 'return Integer.valueOf(a) + Integer.valueOf(b);'");
    }

    private String createIdentityFunction() throws Throwable
    {
        return createFunction(KEYSPACE,
                              "int, int",
                              "CREATE FUNCTION %s (a int) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java " +
                              "AS 'return a;'");
    }
}
