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
package org.apache.cassandra.cql3.validation.entities;

import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertTrue;

/**
 * Checks the collection of Function objects returned by CQLStatement.getFunction
 * matches expectations. This is intended to verify the various subcomponents of
 * the statement (Operations, Terms, Restrictions, RestrictionSet, Selection,
 * Selector, SelectorFactories etc) properly report any constituent functions.
 * Some purely terminal functions are resolved at preparation, so those are not
 * included in the reported list. They still need to be surveyed, to verify the
 * calling client has the necessary permissions. UFAuthTest includes tests which
 * verify this more thoroughly than we can here.
 */
public class UFIdentificationTest extends CQLTester
{
    private com.google.common.base.Function<Function, String> toFunctionNames = new com.google.common.base.Function<Function, String>()
    {
        public String apply(Function f)
        {
            return f.name().keyspace + "." + f.name().name;
        }
    };

    String tFunc;
    String iFunc;
    String lFunc;
    String sFunc;
    String mFunc;
    String uFunc;
    String udtFunc;

    String userType;

    @Before
    public void setup() throws Throwable
    {
        userType = KEYSPACE + "." + createType("CREATE TYPE %s (t text, i int)");

        createTable("CREATE TABLE %s (" +
                    "   key int, " +
                    "   t_sc text STATIC," +
                    "   i_cc int, " +
                    "   t_cc text, " +
                    "   i_val int," +
                    "   l_val list<int>," +
                    "   s_val set<int>," +
                    "   m_val map<int, int>," +
                    "   u_val timeuuid," +
                    "   udt_val frozen<" + userType + ">," +
                    "   PRIMARY KEY (key, i_cc, t_cc)" +
                    ")");

        tFunc = createEchoFunction("text");
        iFunc = createEchoFunction("int");
        lFunc = createEchoFunction("list<int>");
        sFunc = createEchoFunction("set<int>");
        mFunc = createEchoFunction("map<int, int>");
        uFunc = createEchoFunction("timeuuid");
        udtFunc = createEchoFunction(userType);
    }

    @Test
    public void testSimpleModificationStatement() throws Throwable
    {
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, t_sc) VALUES (0, 0, 'A', %s)", functionCall(tFunc, "'foo'")), tFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc) VALUES (0, %s, 'A')", functionCall(iFunc, "1")), iFunc);
        assertFunctions(cql("INSERT INTO %s (key, t_cc, i_cc) VALUES (0, %s, 1)", functionCall(tFunc, "'foo'")), tFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'A', %s)", functionCall(iFunc, "1")), iFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, l_val) VALUES (0, 0, 'A', %s)", functionCall(lFunc, "[1]")), lFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, s_val) VALUES (0, 0, 'A', %s)", functionCall(sFunc, "{1}")), sFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, m_val) VALUES (0, 0, 'A', %s)", functionCall(mFunc, "{1:1}")), mFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, udt_val) VALUES (0, 0, 'A', %s)", functionCall(udtFunc, "{i : 1, t : 'foo'}")), udtFunc);
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, u_val) VALUES (0, 0, 'A', %s)", functionCall(uFunc, "now()")), uFunc, "system.now");
    }

    @Test
    public void testNonTerminalCollectionLiterals() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        String mapValue = String.format("{%s:%s}", functionCall(iFunc, "1"), functionCall(iFunc2, "1"));
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, m_val) VALUES (0, 0, 'A', %s)", mapValue), iFunc, iFunc2);

        String listValue = String.format("[%s]", functionCall(iFunc, "1"));
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, l_val) VALUES (0, 0, 'A',  %s)", listValue), iFunc);

        String setValue = String.format("{%s}", functionCall(iFunc, "1"));
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, s_val) VALUES (0, 0, 'A', %s)", setValue), iFunc);
    }

    @Test
    public void testNonTerminalUDTLiterals() throws Throwable
    {
        String udtValue = String.format("{ i: %s, t : %s } ", functionCall(iFunc, "1"), functionCall(tFunc, "'foo'"));
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, udt_val) VALUES (0, 0, 'A', %s)", udtValue), iFunc, tFunc);
    }

    @Test
    public void testModificationStatementWithConditions() throws Throwable
    {
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF t_sc=%s", functionCall(tFunc, "'foo'")), tFunc);
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF i_val=%s", functionCall(iFunc, "1")), iFunc);
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF l_val=%s", functionCall(lFunc, "[1]")), lFunc);
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF s_val=%s", functionCall(sFunc, "{1}")), sFunc);
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF m_val=%s", functionCall(mFunc, "{1:1}")), mFunc);


        String iFunc2 = createEchoFunction("int");
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF i_val IN (%s, %S)",
                            functionCall(iFunc, "1"),
                            functionCall(iFunc2, "2")),
                        iFunc, iFunc2);

        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF u_val=%s",
                            functionCall(uFunc, "now()")),
                        uFunc, "system.now");

        // conditions on collection elements
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF l_val[%s] = %s",
                            functionCall(iFunc, "1"),
                            functionCall(iFunc2, "1")),
                        iFunc, iFunc2);
        assertFunctions(cql("UPDATE %s SET i_val=0 WHERE key=0 AND i_cc = 0 AND t_cc = 'A' IF m_val[%s] = %s",
                            functionCall(iFunc, "1"),
                            functionCall(iFunc2, "1")),
                        iFunc, iFunc2);
    }

    @Test @Ignore
    // Technically, attributes like timestamp and ttl are Terms so could potentially
    // resolve to function calls (& so you can call getFunctions on them)
    // However, this is currently disallowed by CQL syntax
    public void testModificationStatementWithAttributesFromFunction() throws Throwable
    {
        String longFunc = createEchoFunction("bigint");
        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'foo', 0) USING TIMESTAMP %s",
                            functionCall(longFunc, "9999")),
                        longFunc);

        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'foo', 0) USING TTL %s",
                            functionCall(iFunc, "8888")),
                        iFunc);

        assertFunctions(cql("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'foo', 0) USING TIMESTAMP %s AND TTL %s",
                            functionCall(longFunc, "9999"), functionCall(iFunc, "8888")),
                        longFunc, iFunc);
    }

    @Test
    public void testModificationStatementWithNestedFunctions() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        String iFunc3 = createEchoFunction("int");
        String iFunc4 = createEchoFunction("int");
        String iFunc5 = createEchoFunction("int");
        String iFunc6 = createEchoFunction("int");
        String nestedFunctionCall = nestedFunctionCall(iFunc6, iFunc5,
                                                       nestedFunctionCall(iFunc4, iFunc3,
                                                                          nestedFunctionCall(iFunc2, iFunc, "1")));

        assertFunctions(cql("DELETE FROM %s WHERE key=%s", nestedFunctionCall),
                        iFunc, iFunc2, iFunc3, iFunc4, iFunc5, iFunc6);
    }

    @Test
    public void testSelectStatementSimpleRestrictions() throws Throwable
    {
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=%s", functionCall(iFunc, "1")), iFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND t_sc=%s ALLOW FILTERING", functionCall(tFunc, "'foo'")), tFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND i_cc=%s AND t_cc='foo' ALLOW FILTERING", functionCall(iFunc, "1")), iFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND i_cc=0 AND t_cc=%s ALLOW FILTERING", functionCall(tFunc, "'foo'")), tFunc);

        String iFunc2 = createEchoFunction("int");
        String tFunc2 = createEchoFunction("text");
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=%s AND t_sc=%s AND i_cc=%s AND t_cc=%s ALLOW FILTERING",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'"),
                            functionCall(iFunc2, "1"),
                            functionCall(tFunc2, "'foo'")),
                        iFunc, tFunc, iFunc2, tFunc2);
    }

    @Test
    public void testSelectStatementRestrictionsWithNestedFunctions() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        String iFunc3 = createEchoFunction("int");
        String iFunc4 = createEchoFunction("int");
        String iFunc5 = createEchoFunction("int");
        String iFunc6 = createEchoFunction("int");
        String nestedFunctionCall = nestedFunctionCall(iFunc6, iFunc5,
                                                       nestedFunctionCall(iFunc3, iFunc4,
                                                                          nestedFunctionCall(iFunc, iFunc2, "1")));

        assertFunctions(cql("SELECT i_val FROM %s WHERE key=%s", nestedFunctionCall),
                        iFunc, iFunc2, iFunc3, iFunc4, iFunc5, iFunc6);
    }

    @Test
    public void testNonTerminalTupleInSelectRestrictions() throws Throwable
    {
        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND (i_cc, t_cc) IN ((%s, %s))",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'")),
                        iFunc, tFunc);

        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND (i_cc, t_cc) = (%s, %s)",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'")),
                        iFunc, tFunc);

        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND (i_cc, t_cc) > (%s, %s)",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'")),
                        iFunc, tFunc);

        assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND (i_cc, t_cc) < (%s, %s)",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'")),
                        iFunc, tFunc);

         assertFunctions(cql("SELECT i_val FROM %s WHERE key=0 AND (i_cc, t_cc) > (%s, %s) AND (i_cc, t_cc) < (%s, %s)",
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'"),
                            functionCall(iFunc, "1"),
                            functionCall(tFunc, "'foo'")),
                         iFunc, tFunc);
    }

    @Test
    public void testNestedFunctionInTokenRestriction() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        assertFunctions(cql("SELECT i_val FROM %s WHERE token(key) = token(%s)", functionCall(iFunc, "1")),
                        "system.token", iFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE token(key) > token(%s)", functionCall(iFunc, "1")),
                        "system.token", iFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE token(key) < token(%s)", functionCall(iFunc, "1")),
                        "system.token", iFunc);
        assertFunctions(cql("SELECT i_val FROM %s WHERE token(key) > token(%s) AND token(key) < token(%s)",
                            functionCall(iFunc, "1"),
                            functionCall(iFunc2, "1")),
                        "system.token", iFunc, iFunc2);
    }

    @Test
    public void testSelectStatementSimpleSelections() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        execute("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'foo', 0)");
        assertFunctions(cql2("SELECT i_val, %s FROM %s WHERE key=0", functionCall(iFunc, "i_val")), iFunc);
        assertFunctions(cql2("SELECT i_val, %s FROM %s WHERE key=0", nestedFunctionCall(iFunc, iFunc2, "i_val")), iFunc, iFunc2);
    }

    @Test
    public void testSelectStatementNestedSelections() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        execute("INSERT INTO %s (key, i_cc, t_cc, i_val) VALUES (0, 0, 'foo', 0)");
        assertFunctions(cql2("SELECT i_val, %s FROM %s WHERE key=0", functionCall(iFunc, "i_val")), iFunc);
        assertFunctions(cql2("SELECT i_val, %s FROM %s WHERE key=0", nestedFunctionCall(iFunc, iFunc2, "i_val")), iFunc, iFunc2);
    }

    @Test
    public void testBatchStatement() throws Throwable
    {
        String iFunc2 = createEchoFunction("int");
        List<ModificationStatement> statements = new ArrayList<>();
        statements.add(modificationStatement(cql("INSERT INTO %s (key, i_cc, t_cc) VALUES (%s, 0, 'foo')",
                                                 functionCall(iFunc, "0"))));
        statements.add(modificationStatement(cql("INSERT INTO %s (key, i_cc, t_cc) VALUES (1, %s, 'foo')",
                                                 functionCall(iFunc2, "1"))));
        statements.add(modificationStatement(cql("INSERT INTO %s (key, i_cc, t_cc) VALUES (2, 2, %s)",
                                                 functionCall(tFunc, "'foo'"))));

        BatchStatement batch = new BatchStatement(-1, BatchStatement.Type.LOGGED, statements, Attributes.none());
        assertFunctions(batch, iFunc, iFunc2, tFunc);
    }

    @Test
    public void testBatchStatementWithConditions() throws Throwable
    {
        List<ModificationStatement> statements = new ArrayList<>();
        statements.add(modificationStatement(cql("UPDATE %s SET i_val = %s WHERE key=0 AND i_cc=0 and t_cc='foo' IF l_val = %s",
                                                 functionCall(iFunc, "0"), functionCall(lFunc, "[1]"))));
        statements.add(modificationStatement(cql("UPDATE %s SET i_val = %s WHERE key=0 AND i_cc=1 and t_cc='foo' IF s_val = %s",
                                                 functionCall(iFunc, "0"), functionCall(sFunc, "{1}"))));

        BatchStatement batch = new BatchStatement(-1, BatchStatement.Type.LOGGED, statements, Attributes.none());
        assertFunctions(batch, iFunc, lFunc, sFunc);
    }

    private ModificationStatement modificationStatement(String cql)
    {
        return (ModificationStatement) QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement;
    }

    private void assertFunctions(String cql, String... function)
    {
        CQLStatement stmt = QueryProcessor.getStatement(cql, ClientState.forInternalCalls()).statement;
        assertFunctions(stmt, function);
    }

    private void assertFunctions(CQLStatement stmt, String... function)
    {
        Set<String> expected = com.google.common.collect.Sets.newHashSet(function);
        Set<String> actual = com.google.common.collect.Sets.newHashSet(Iterables.transform(stmt.getFunctions(),
                                                                                           toFunctionNames));
        assertTrue(com.google.common.collect.Sets.symmetricDifference(expected, actual).isEmpty());
    }

    private String cql(String template, String... params)
    {
        String tableName = KEYSPACE + "." + currentTable();
        return String.format(template, com.google.common.collect.Lists.asList(tableName, params).toArray());
    }

    // Alternative query builder - appends the table name to the supplied params,
    // for stmts of the form "SELECT x, %s FROM %s WHERE y=0"
    private String cql2(String template, String... params)
    {
        Object[] args = Arrays.copyOf(params, params.length + 1);
        args[params.length] = KEYSPACE + "." + currentTable();
        return String.format(template, args);
    }

    private String functionCall(String fName, String... args)
    {
        return String.format("%s(%s)", fName, Joiner.on(",").join(args));
    }

    private String nestedFunctionCall(String outer, String inner, String innerArgs)
    {
        return functionCall(outer, functionCall(inner, innerArgs));
    }

    private String createEchoFunction(String type) throws Throwable
    {
        return createFunction(KEYSPACE, type,
           "CREATE FUNCTION %s(input " + type + ")" +
           " CALLED ON NULL INPUT" +
           " RETURNS " + type +
           " LANGUAGE java" +
           " AS ' return input;'");
    }
}
