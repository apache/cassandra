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

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ListRolesStatement;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;

public class ListRolesStatementTest extends CQLTester
{
    @BeforeClass
    public static void startup() throws ConfigurationException
    {
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void mapCqlWithOptionSuperToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH SUPERUSER=true");
        parse("LIST ROLES OF test_user_a WITH SUPERUSER=false");
    }

    @Test
    public void mapCqlWithOptionLoginToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH LOGIN=true");
        parse("LIST ROLES OF test_user_a WITH LOGIN=false");
    }

    @Test(expected = SyntaxException.class)
    public void mapCqlWithOptionPasswordToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH PASSWORD='a1b2c3'");
    }

    @Test(expected = SyntaxException.class)
    public void mapCqlWithOptionHashedPasswordToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH HASHED PASSWORD='a1b2c3dfhfjdhfruow42394'");
    }

    @Test
    public void mapCqlWithOptionAccessToDatacentersToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH ACCESS TO ALL DATACENTERS");
        parse("LIST ROLES OF test_user_a WITH ACCESS TO DATACENTERS {'datacenter1'}");
        parse("LIST ROLES OF test_user_a WITH ACCESS TO DATACENTERS {'datacenter1', 'datacenter2'}");
    }

    @Test
    public void mapCqlWithOptionsToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a WITH OPTIONS = {'option_a' : 'value_a'}");
    }

    @Test
    public void mapCqlWithDefaultOptionsToListRolesStatement()
    {
        parse("LIST ROLES OF test_user_a");
    }

    @Test
    public void withAttrSuperWhenSuperIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = true",
            "LIST ROLES OF 'test_user_a' WITH SUPERUSER = true",
            1,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrNonSuperWhenNonSuperIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = false",
            "LIST ROLES OF 'test_user_a' WITH SUPERUSER = false",
            1,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrSuperWhenNonSuperIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = false",
            "LIST ROLES OF 'test_user_a' WITH SUPERUSER = true",
            0,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrNonSuperWhenSuperIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = true",
            "LIST ROLES OF 'test_user_a' WITH SUPERUSER = false",
            0,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrLoginWhenLoginIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = true",
            "LIST ROLES OF 'test_user_a' WITH LOGIN = true",
            1,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrNonLoginWhenLoginIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = true",
            "LIST ROLES OF 'test_user_a' WITH LOGIN = false",
            0,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrNonLoginWhenNonLoginIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = false",
            "LIST ROLES OF 'test_user_a' WITH LOGIN = false",
            1,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrLoginWhenNonLoginIsExplicit() throws Throwable
    {
        singleExplicitAttributeFlow(
            "CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = false",
            "LIST ROLES OF 'test_user_a' WITH LOGIN = true",
            0,
            "DROP ROLE IF EXISTS 'test_user_a'"
        );
    }

    @Test
    public void withAttrSuperWhenSuperIsInheritedAndWithNoConditions() throws Throwable
    {
        List<Map<String, String>> outputWithCondition = null;
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_b' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_c' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_d' WITH SUPERUSER = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_e' WITH SUPERUSER = true");
            executeNet("Grant 'test_user_d' TO 'test_user_c'");
            executeNet("Grant 'test_user_c' TO 'test_user_b'");
            executeNet("Grant 'test_user_b' TO 'test_user_a'");
            executeNet("Grant 'test_user_e' TO 'test_user_a'");
            outputWithCondition = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' WITH SUPERUSER = true");
            output = collectListRolesCqlRows("LIST ROLES OF 'test_user_a'");
        }
        finally
        {
            executeNet("DROP ROLE IF EXISTS 'test_user_e'");
            executeNet("DROP ROLE IF EXISTS 'test_user_d'");
            executeNet("DROP ROLE IF EXISTS 'test_user_c'");
            executeNet("DROP ROLE IF EXISTS 'test_user_b'");
            executeNet("DROP ROLE IF EXISTS 'test_user_a'");
        }
        Assert.assertEquals(2, outputWithCondition.size());
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") || outputWithCondition.get(0).get("role").equals("test_user_e"));
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") ? outputWithCondition.get(1).get("role").equals("test_user_e") : output.get(1).get("role").equals("test_user_d"));

        Assert.assertEquals(5, output.size());
    }

    @Test
    public void withAttrSuperWhenSuperIsInheritedAndNoConditionsNonRecursive() throws Throwable
    {
        List<Map<String, String>> outputWithCondition = null;
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_b' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_c' WITH SUPERUSER = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_d' WITH SUPERUSER = true");
            executeNet("Grant 'test_user_c' TO 'test_user_b'");
            executeNet("Grant 'test_user_b' TO 'test_user_a'");
            executeNet("Grant 'test_user_d' TO 'test_user_a'");
            outputWithCondition = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' NORECURSIVE WITH SUPERUSER = true");
            output = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' NORECURSIVE");
        }
        finally
        {
            executeNet("DROP ROLE IF EXISTS 'test_user_d'");
            executeNet("DROP ROLE IF EXISTS 'test_user_c'");
            executeNet("DROP ROLE IF EXISTS 'test_user_b'");
            executeNet("DROP ROLE IF EXISTS 'test_user_a'");
        }
        Assert.assertEquals(1, outputWithCondition.size());
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d"));

        Assert.assertEquals(3, output.size());
    }

    @Test
    public void withAttrNonSuperWhenNonSuperIsInheritedAndNoConditions() throws Throwable
    {
        List<Map<String, String>> outputWithCondition = null;
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_a' WITH SUPERUSER = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_b' WITH SUPERUSER = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_c' WITH SUPERUSER = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_d' WITH SUPERUSER = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_e' WITH SUPERUSER = false");
            executeNet("Grant 'test_user_d' TO 'test_user_c'");
            executeNet("Grant 'test_user_c' TO 'test_user_b'");
            executeNet("Grant 'test_user_b' TO 'test_user_a'");
            executeNet("Grant 'test_user_e' TO 'test_user_a'");
            outputWithCondition = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' WITH SUPERUSER = false");
            output = collectListRolesCqlRows("LIST ROLES OF 'test_user_a'");
        }
        finally
        {
            executeNet("DROP ROLE IF EXISTS 'test_user_e'");
            executeNet("DROP ROLE IF EXISTS 'test_user_d'");
            executeNet("DROP ROLE IF EXISTS 'test_user_c'");
            executeNet("DROP ROLE IF EXISTS 'test_user_b'");
            executeNet("DROP ROLE IF EXISTS 'test_user_a'");
        }
        Assert.assertEquals(2, outputWithCondition.size());
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") || outputWithCondition.get(0).get("role").equals("test_user_e"));
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") ? outputWithCondition.get(1).get("role").equals("test_user_e") : outputWithCondition.get(1).get("role").equals("test_user_d"));

        Assert.assertEquals(5, output.size());
    }

    @Test
    public void withAttrLoginWhenLoginIsInheritedAndNoConditions() throws Throwable
    {
        List<Map<String, String>> outputWithCondition = null;
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_b' WITH LOGIN = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_c' WITH LOGIN = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_d' WITH LOGIN = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_e' WITH LOGIN = true");
            executeNet("Grant 'test_user_d' TO 'test_user_c'");
            executeNet("Grant 'test_user_c' TO 'test_user_b'");
            executeNet("Grant 'test_user_b' TO 'test_user_a'");
            executeNet("Grant 'test_user_e' TO 'test_user_a'");
            outputWithCondition = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' WITH LOGIN = true");
            output = collectListRolesCqlRows("LIST ROLES OF 'test_user_a'");
        } finally
        {
            executeNet("DROP ROLE IF EXISTS 'test_user_e'");
            executeNet("DROP ROLE IF EXISTS 'test_user_d'");
            executeNet("DROP ROLE IF EXISTS 'test_user_c'");
            executeNet("DROP ROLE IF EXISTS 'test_user_b'");
            executeNet("DROP ROLE IF EXISTS 'test_user_a'");
        }
        Assert.assertEquals(2, outputWithCondition.size());
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") || outputWithCondition.get(0).get("role").equals("test_user_e"));
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") ? outputWithCondition.get(1).get("role").equals("test_user_e") : outputWithCondition.get(1).get("role").equals("test_user_d"));

        Assert.assertEquals(5, output.size());
    }

    @Test
    public void withAttrNonLoginWhenNonLoginIsInheritedAndNoConditions() throws Throwable
    {
        List<Map<String, String>> outputWithCondition = null;
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_a' WITH LOGIN = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_b' WITH LOGIN = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_c' WITH LOGIN = true");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_d' WITH LOGIN = false");
            executeNet("CREATE ROLE IF NOT EXISTS 'test_user_e' WITH LOGIN = false");
            executeNet("Grant 'test_user_d' TO 'test_user_c'");
            executeNet("Grant 'test_user_c' TO 'test_user_b'");
            executeNet("Grant 'test_user_b' TO 'test_user_a'");
            executeNet("Grant 'test_user_e' TO 'test_user_a'");
            outputWithCondition = collectListRolesCqlRows("LIST ROLES OF 'test_user_a' WITH LOGIN = false");
            output = collectListRolesCqlRows("LIST ROLES OF 'test_user_a'");
        } finally
        {
            executeNet("DROP ROLE IF EXISTS 'test_user_e'");
            executeNet("DROP ROLE IF EXISTS 'test_user_d'");
            executeNet("DROP ROLE IF EXISTS 'test_user_c'");
            executeNet("DROP ROLE IF EXISTS 'test_user_b'");
            executeNet("DROP ROLE IF EXISTS 'test_user_a'");
        }
        Assert.assertEquals(2, outputWithCondition.size());
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") || outputWithCondition.get(0).get("role").equals("test_user_e"));
        Assert.assertTrue(outputWithCondition.get(0).get("role").equals("test_user_d") ? outputWithCondition.get(1).get("role").equals("test_user_e") : outputWithCondition.get(1).get("role").equals("test_user_d"));

        Assert.assertEquals(5, output.size());
    }

    private void singleExplicitAttributeFlow(String roleCreateQuery, String roleSelectQuery, int expectedRowsAmount, String dropRoleQuery) throws Throwable
    {
        List<Map<String, String>> output = null;
        useSuperUser();
        try
        {
            executeNet(roleCreateQuery);
            output = collectListRolesCqlRows(roleSelectQuery);
        }
        finally
        {
            executeNet(dropRoleQuery);
        }
        Assert.assertEquals(expectedRowsAmount, output.size());
    }

    private List<Map<String,String>> collectListRolesCqlRows(String query) throws Throwable
    {
        return executeNet(query)
            .all()
            .stream()
            .map(row -> {
                Map<String, String> cqlRow = new HashMap<String, String>();
                cqlRow.put("role", row.getString("role"));
                cqlRow.put("super", Boolean.toString(row.getBool("super")));
                cqlRow.put("login", Boolean.toString(row.getBool("login")));
                cqlRow.put("options", row.getMap("options", String.class, String.class).toString());
                cqlRow.put("datacenters", row.getString("datacenters"));
                return cqlRow;
            })
            .collect(Collectors.toList());
    }
    
    private static ListRolesStatement parse(String query) throws RequestExecutionException, RequestValidationException
    {
        CQLStatement.Raw stmt = QueryProcessor.parseStatement(query);
        Assert.assertTrue(stmt instanceof ListRolesStatement);
        return (ListRolesStatement) stmt;
    }
}
