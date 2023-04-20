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

import java.lang.reflect.Field;
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UFAuthTest extends CQLTester
{
    String roleName = "test_role";
    AuthenticatedUser user;
    RoleResource role;
    ClientState clientState;

    @BeforeClass
    public static void setupAuthorizer()
    {
        try
        {
            IAuthorizer authorizer = new StubAuthorizer();
            Field authorizerField = DatabaseDescriptor.class.getDeclaredField("authorizer");
            authorizerField.setAccessible(true);
            authorizerField.set(null, authorizer);
            DatabaseDescriptor.setPermissionsValidity(0);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setup() throws Throwable
    {
        ((StubAuthorizer) DatabaseDescriptor.getAuthorizer()).clear();
        setupClientState();
        setupTable("CREATE TABLE %s (k int, v1 int, v2 int, PRIMARY KEY (k, v1))");
    }

    @Test
    public void functionInSelection() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT k, %s FROM %s WHERE k = 1;",
                                   functionCall(functionName),
                                   KEYSPACE + "." + currentTable());
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectPKRestriction() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectClusteringRestriction() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k = 0 AND v1 = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectInRestriction() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k IN (%s, %s)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectMultiColumnInRestriction() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))");
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k=0 AND (v1, v2) IN ((%s, %s))",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectMultiColumnEQRestriction() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))");
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k=0 AND (v1, v2) = (%s, %s)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectMultiColumnSliceRestriction() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2))");
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE k=0 AND (v1, v2) < (%s, %s)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectTokenEQRestriction() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE token(k) = token(%s)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInSelectTokenSliceRestriction() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT * FROM %s WHERE token(k) < token(%s)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInPKForInsert() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("INSERT INTO %s (k, v1, v2) VALUES (%s, 0, 0)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInClusteringValuesForInsert() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("INSERT INTO %s (k, v1, v2) VALUES (0, %s, 0)",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInPKForDelete() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("DELETE FROM %s WHERE k = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInClusteringValuesForDelete() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("DELETE FROM %s WHERE k = 0 AND v1 = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void testBatchStatement() throws Throwable
    {
        List<ModificationStatement> statements = new ArrayList<>();
        List<String> functions = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            String functionName = createSimpleFunction();
            ModificationStatement stmt =
            (ModificationStatement) getStatement(String.format("INSERT INTO %s (k, v1, v2) " +
                                                               "VALUES (%s, %s, %s)",
                                                               KEYSPACE + "." + currentTable(),
                                                               i, i, functionCall(functionName)));
            functions.add(functionName);
            statements.add(stmt);
        }
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED, VariableSpecifications.empty(), statements, Attributes.none());
        assertUnauthorized(batch, functions);

        grantExecuteOnFunction(functions.get(0));
        assertUnauthorized(batch, functions.subList(1, functions.size()));

        grantExecuteOnFunction(functions.get(1));
        assertUnauthorized(batch, functions.subList(2, functions.size()));

        grantExecuteOnFunction(functions.get(2));
        batch.authorize(clientState);
    }

    @Test
    public void testNestedFunctions() throws Throwable
    {
        String innerFunctionName = createSimpleFunction();
        String outerFunctionName = createFunction("int",
                                                  "CREATE FUNCTION %s(input int) " +
                                                  " CALLED ON NULL INPUT" +
                                                  " RETURNS int" +
                                                  " LANGUAGE java" +
                                                  " AS 'return Integer.valueOf(0);'");
        assertPermissionsOnNestedFunctions(innerFunctionName, outerFunctionName);
    }

    @Test
    public void functionInStaticColumnRestrictionInSelect() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, s int STATIC, v1 int, v2 int, PRIMARY KEY(k, v1))");
        String functionName = createSimpleFunction();
        String cql = String.format("SELECT k FROM %s WHERE k = 0 AND s = %s ALLOW FILTERING",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInRegularCondition() throws Throwable
    {
        String functionName = createSimpleFunction();
        String cql = String.format("UPDATE %s SET v2 = 0 WHERE k = 0 AND v1 = 0 IF v2 = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }
    @Test
    public void functionInStaticColumnCondition() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, s int STATIC, v1 int, v2 int, PRIMARY KEY(k, v1))");
        String functionName = createSimpleFunction();
        String cql = String.format("UPDATE %s SET v2 = 0 WHERE k = 0 AND v1 = 0 IF s = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInCollectionLiteralCondition() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, v1 int, m_val map<int, int>, PRIMARY KEY(k))");
        String functionName = createSimpleFunction();
        String cql = String.format("UPDATE %s SET v1 = 0 WHERE k = 0 IF m_val = {%s : %s}",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void functionInCollectionElementCondition() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, v1 int, m_val map<int, int>, PRIMARY KEY(k))");
        String functionName = createSimpleFunction();
        String cql = String.format("UPDATE %s SET v1 = 0 WHERE k = 0 IF m_val[%s] = %s",
                                   KEYSPACE + "." + currentTable(),
                                   functionCall(functionName),
                                   functionCall(functionName));
        assertPermissionsOnFunction(cql, functionName);
    }

    @Test
    public void systemFunctionsRequireNoExplicitPrivileges() throws Throwable
    {
        // with terminal arguments, so evaluated at prepare time
        String cql = String.format("UPDATE %s SET v2 = 0 WHERE k = blob_as_int(int_as_blob(0)) and v1 = 0",
                                   KEYSPACE + "." + currentTable());
        getStatement(cql).authorize(clientState);

        // with non-terminal arguments, so evaluated at execution
        String functionName = createSimpleFunction();
        grantExecuteOnFunction(functionName);
        cql = String.format("UPDATE %s SET v2 = 0 WHERE k = blob_as_int(int_as_blob(%s)) and v1 = 0",
                            KEYSPACE + "." + currentTable(),
                            functionCall(functionName));
        getStatement(cql).authorize(clientState);
    }

    @Test
    public void requireExecutePermissionOnComponentFunctionsWhenDefiningAggregate() throws Throwable
    {
        String sFunc = createSimpleStateFunction();
        String fFunc = createSimpleFinalFunction();
        // aside from the component functions, we need CREATE on the keyspace's functions
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                 ImmutableSet.of(Permission.CREATE),
                                                 FunctionResource.keyspace(KEYSPACE),
                                                 role);
        String aggDef = String.format(aggregateCql(sFunc, fFunc),
                                      KEYSPACE + ".aggregate_for_permissions_test");

        assertUnauthorized(aggDef, sFunc, "int, int");
        grantExecuteOnFunction(sFunc);

        assertUnauthorized(aggDef, fFunc, "int");
        grantExecuteOnFunction(fFunc);

        getStatement(aggDef).authorize(clientState);
    }

    @Test
    public void revokeExecutePermissionsOnAggregateComponents() throws Throwable
    {
        String sFunc = createSimpleStateFunction();
        String fFunc = createSimpleFinalFunction();
        String aggDef = aggregateCql(sFunc, fFunc);
        grantExecuteOnFunction(sFunc);
        grantExecuteOnFunction(fFunc);

        String aggregate = createAggregate(KEYSPACE, "int", aggDef);
        grantExecuteOnFunction(aggregate);

        String cql = String.format("SELECT %s(v1) FROM %s",
                                   aggregate,
                                   KEYSPACE + "." + currentTable());
        getStatement(cql).authorize(clientState);

        // check that revoking EXECUTE permission on any one of the
        // component functions means we lose the ability to execute it
        revokeExecuteOnFunction(aggregate);
        assertUnauthorized(cql, aggregate, "int");
        grantExecuteOnFunction(aggregate);
        getStatement(cql).authorize(clientState);

        revokeExecuteOnFunction(sFunc);
        assertUnauthorized(cql, sFunc, "int, int");
        grantExecuteOnFunction(sFunc);
        getStatement(cql).authorize(clientState);

        revokeExecuteOnFunction(fFunc);
        assertUnauthorized(cql, fFunc, "int");
        grantExecuteOnFunction(fFunc);
        getStatement(cql).authorize(clientState);
    }

    @Test
    public void functionWrappingAggregate() throws Throwable
    {
        String outerFunc = createFunction("int",
                                          "CREATE FUNCTION %s(input int) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS int " +
                                          "LANGUAGE java " +
                                          "AS 'return input;'");

        String sFunc = createSimpleStateFunction();
        String fFunc = createSimpleFinalFunction();
        String aggDef = aggregateCql(sFunc, fFunc);
        grantExecuteOnFunction(sFunc);
        grantExecuteOnFunction(fFunc);

        String aggregate = createAggregate(KEYSPACE, "int", aggDef);

        String cql = String.format("SELECT %s(%s(v1)) FROM %s",
                                   outerFunc,
                                   aggregate,
                                   KEYSPACE + "." + currentTable());

        assertUnauthorized(cql, outerFunc, "int");
        grantExecuteOnFunction(outerFunc);

        assertUnauthorized(cql, aggregate, "int");
        grantExecuteOnFunction(aggregate);

        getStatement(cql).authorize(clientState);
    }

    @Test
    public void aggregateWrappingFunction() throws Throwable
    {
        String innerFunc = createFunction("int",
                                          "CREATE FUNCTION %s(input int) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS int " +
                                          "LANGUAGE java " +
                                          "AS 'return input;'");

        String sFunc = createSimpleStateFunction();
        String fFunc = createSimpleFinalFunction();
        String aggDef = aggregateCql(sFunc, fFunc);
        grantExecuteOnFunction(sFunc);
        grantExecuteOnFunction(fFunc);

        String aggregate = createAggregate(KEYSPACE, "int", aggDef);

        String cql = String.format("SELECT %s(%s(v1)) FROM %s",
                                   aggregate,
                                   innerFunc,
                                   KEYSPACE + "." + currentTable());

        assertUnauthorized(cql, aggregate, "int");
        grantExecuteOnFunction(aggregate);

        assertUnauthorized(cql, innerFunc, "int");
        grantExecuteOnFunction(innerFunc);

        getStatement(cql).authorize(clientState);
    }

    @Test
    public void grantAndRevokeSyntaxRequiresExplicitKeyspace() throws Throwable
    {
        setupTable("CREATE TABLE %s (k int, s int STATIC, v1 int, v2 int, PRIMARY KEY(k, v1))");
        String functionName = shortFunctionName(createSimpleFunction());
        assertRequiresKeyspace(String.format("GRANT EXECUTE ON FUNCTION %s() TO %s",
                                             functionName,
                                             role.getRoleName()));
        assertRequiresKeyspace(String.format("REVOKE EXECUTE ON FUNCTION %s() FROM %s",
                                             functionName,
                                             role.getRoleName()));
    }

    private void assertRequiresKeyspace(String cql) throws Throwable
    {
        try
        {
            getStatement(cql);
        }
        catch (InvalidRequestException e)
        {
            assertEquals("In this context function name must be explictly qualified by a keyspace", e.getMessage());
        }
    }

    private void assertPermissionsOnNestedFunctions(String innerFunction, String outerFunction) throws Throwable
    {
        String cql = String.format("SELECT k, %s FROM %s WHERE k=0",
                                   functionCall(outerFunction, functionCall(innerFunction)),
                                   KEYSPACE + "." + currentTable());
        // fail fast with an UAE on the first function
        assertUnauthorized(cql, outerFunction, "int");
        grantExecuteOnFunction(outerFunction);

        // after granting execute on the first function, still fail due to the inner function
        assertUnauthorized(cql, innerFunction, "");
        grantExecuteOnFunction(innerFunction);

        // now execution of both is permitted
        getStatement(cql).authorize(clientState);
    }

    private void assertPermissionsOnFunction(String cql, String functionName) throws Throwable
    {
        assertPermissionsOnFunction(cql, functionName, "");
    }

    private void assertPermissionsOnFunction(String cql, String functionName, String argTypes) throws Throwable
    {
        assertUnauthorized(cql, functionName, argTypes);
        grantExecuteOnFunction(functionName);
        getStatement(cql).authorize(clientState);
    }

    private void assertUnauthorized(BatchStatement batch, Iterable<String> functionNames) throws Throwable
    {
        try
        {
            batch.authorize(clientState);
            fail("Expected an UnauthorizedException, but none was thrown");
        }
        catch (UnauthorizedException e)
        {
            String functions = String.format("(%s)", Joiner.on("|").join(functionNames));
            assertTrue(e.getLocalizedMessage()
                        .matches(String.format("User %s has no EXECUTE permission on <function %s\\(\\)> or any of its parents",
                                               roleName,
                                               functions)));
        }
    }

    private void assertUnauthorized(String cql, String functionName, String argTypes) throws Throwable
    {
        try
        {
            getStatement(cql).authorize(clientState);
            fail("Expected an UnauthorizedException, but none was thrown");
        }
        catch (UnauthorizedException e)
        {
            assertEquals(String.format("User %s has no EXECUTE permission on <function %s(%s)> or any of its parents",
                                       roleName,
                                       functionName,
                                       argTypes),
                         e.getLocalizedMessage());
        }
    }

    private void grantExecuteOnFunction(String functionName)
    {
            DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                     ImmutableSet.of(Permission.EXECUTE),
                                                     functionResource(functionName),
                                                     role);
    }

    private void revokeExecuteOnFunction(String functionName)
    {
        DatabaseDescriptor.getAuthorizer().revoke(AuthenticatedUser.SYSTEM_USER,
                                                  ImmutableSet.of(Permission.EXECUTE),
                                                  functionResource(functionName),
                                                  role);
    }

    void setupClientState()
    {

        try
        {
            role = RoleResource.role(roleName);
            // use reflection to set the logged in user so that we don't need to
            // bother setting up an IRoleManager
            user = new AuthenticatedUser(roleName);
            clientState = ClientState.forInternalCalls();
            Field userField = ClientState.class.getDeclaredField("user");
            userField.setAccessible(true);
            userField.set(clientState, user);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void setupTable(String tableDef) throws Throwable
    {
        createTable(tableDef);
        // test user needs SELECT & MODIFY on the table regardless of permissions on any function
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                 ImmutableSet.of(Permission.SELECT, Permission.MODIFY),
                                                 DataResource.table(KEYSPACE, currentTable()),
                                                 RoleResource.role(user.getName()));
    }

    private String aggregateCql(String sFunc, String fFunc)
    {
        return "CREATE AGGREGATE %s(int) " +
               "SFUNC " + shortFunctionName(sFunc) + " " +
               "STYPE int " +
               "FINALFUNC " + shortFunctionName(fFunc) + " " +
               "INITCOND 0";
    }

    private String createSimpleStateFunction() throws Throwable
    {
        return createFunction("int, int",
                              "CREATE FUNCTION %s(a int, b int) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java " +
                              "AS 'return Integer.valueOf( (a != null ? a.intValue() : 0 ) + b.intValue());'");
    }

    private String createSimpleFinalFunction() throws Throwable
    {
        return createFunction("int",
                              "CREATE FUNCTION %s(a int) " +
                              "CALLED ON NULL INPUT " +
                              "RETURNS int " +
                              "LANGUAGE java " +
                              "AS 'return a;'");
    }

    private String createSimpleFunction() throws Throwable
    {
        return createFunction("",
                              "CREATE FUNCTION %s() " +
                              "  CALLED ON NULL INPUT " +
                              "  RETURNS int " +
                              "  LANGUAGE java " +
                              "  AS 'return Integer.valueOf(0);'");
    }

    private String createFunction(String argTypes, String functionDef) throws Throwable
    {
        return createFunction(KEYSPACE, argTypes, functionDef);
    }

    private CQLStatement getStatement(String cql)
    {
        return QueryProcessor.getStatement(cql, clientState);
    }

    private FunctionResource functionResource(String functionName)
    {
        // Note that this is somewhat brittle as it assumes that function names are
        // truly unique. As such, it will break in the face of overloading.
        // It is here to avoid having to duplicate the functionality of CqlParser
        // for transforming cql types into AbstractTypes
        FunctionName fn = parseFunctionName(functionName);
        Collection<UserFunction> functions = Schema.instance.getUserFunctions(fn);
        assertEquals(String.format("Expected a single function definition for %s, but found %s",
                                   functionName,
                                   functions.size()),
                     1, functions.size());
        return FunctionResource.function(fn.keyspace, fn.name, functions.iterator().next().argTypes());
    }

    private String functionCall(String functionName, String...args)
    {
        return String.format("%s(%s)", functionName, Joiner.on(",").join(args));
    }
}
