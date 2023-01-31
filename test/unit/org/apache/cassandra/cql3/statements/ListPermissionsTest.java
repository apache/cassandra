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

import org.junit.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.CQLTester;

import org.junit.BeforeClass;
import static org.junit.Assert.assertTrue;

public class ListPermissionsTest extends CQLTester
{
    @BeforeClass
    public static void startup()
    {
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void showAllPermissionsForSuperUserTest() throws Throwable
    {
        useSuperUser();
        executeCommandsForNewSuperUser();
        List<Map<String,String>> expectedCqlRowsForAllKeyspaces = expectedSuperRoleCqlRows("jane", "jane", "<all keyspaces>", null);
        List<Map<String,String>> expectedCqlRowsForAllFunctions = expectedSuperRoleCqlRows("jane", "jane", "<all functions>", null);
        List<Map<String,String>> expectedCqlRowsForAllRoles = expectedSuperRoleCqlRows("jane", "jane", "<all roles>", null);
        List<Map<String,String>> expectedCqlRowsForAllMBeans = expectedSuperRoleCqlRows("jane", "jane", "<all mbeans>", null);
        try
        {
            List<Map<String,String>> nonExpectedCqlRows = executeNet("LIST ALL PERMISSIONS OF jane")
                .all()
                .stream()
                .map(
                    actualRow -> {
                        Map<String,String> actualCqlRow = new HashMap<String,String>();
                        actualCqlRow.put("role", actualRow.getString("role"));
                        actualCqlRow.put("username", actualRow.getString("username"));
                        actualCqlRow.put("resource", actualRow.getString("resource"));
                        actualCqlRow.put("permission", actualRow.getString("permission"));
                        return actualCqlRow;
                    }
                )
                .filter(
                    actualCqlRow -> {
                        if (expectedCqlRowsForAllKeyspaces.remove(actualCqlRow))
                            return false;
                        if (expectedCqlRowsForAllFunctions.remove(actualCqlRow))
                            return false;
                        if (expectedCqlRowsForAllRoles.remove(actualCqlRow))
                            return false;
                        if (expectedCqlRowsForAllMBeans.remove(actualCqlRow))
                            return false;
                        return true;
                    }
                )
                .collect(Collectors.toList());
            
            assertTrue(nonExpectedCqlRows.isEmpty());
            assertTrue(expectedCqlRowsForAllKeyspaces.isEmpty());
            assertTrue(expectedCqlRowsForAllFunctions.isEmpty());
            assertTrue(expectedCqlRowsForAllRoles.isEmpty());
            assertTrue(expectedCqlRowsForAllMBeans.isEmpty());
        }
        finally
        {
            executeCommandsToRemoveNewSuperUser();
        }
    }

    @Test
    public void showASinglePermissionForSuperUserTest() throws Throwable
    {
        useSuperUser();
        executeCommandsForNewSuperUser();
        Permission queriedPermissions[] = new Permission[] { Permission.MODIFY, Permission.AUTHORIZE };
        try
        {
            for (int i = 0; i < queriedPermissions.length; i++)
            {
                Set<Permission> expectedPermissions = EnumSet.of(queriedPermissions[i]);
                List<Map<String,String>> expectedCqlRowsForAllKeyspaces = expectedSuperRoleCqlRows("jane", "jane", "<all keyspaces>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllFunctions = expectedSuperRoleCqlRows("jane", "jane", "<all functions>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllRoles = expectedSuperRoleCqlRows("jane", "jane", "<all roles>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllMBeans = expectedSuperRoleCqlRows("jane", "jane", "<all mbeans>", expectedPermissions);
                List<Map<String,String>> nonExpectedCqlRows = executeNet(String.format("LIST %s OF jane", queriedPermissions[i].name()))
                    .all()
                    .stream()
                    .map(
                        actualRow -> {
                            Map<String,String> actualCqlRow = new HashMap<String,String>();
                            actualCqlRow.put("role", actualRow.getString("role"));
                            actualCqlRow.put("username", actualRow.getString("username"));
                            actualCqlRow.put("resource", actualRow.getString("resource"));
                            actualCqlRow.put("permission", actualRow.getString("permission"));
                            return actualCqlRow;
                        }
                    )
                    .filter(
                        actualCqlRow -> {
                            if (expectedCqlRowsForAllKeyspaces.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllFunctions.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllRoles.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllMBeans.remove(actualCqlRow))
                                return false;
                            return true;
                        }
                    )
                    .collect(Collectors.toList());
                
                assertTrue(nonExpectedCqlRows.isEmpty());
                assertTrue(expectedCqlRowsForAllKeyspaces.isEmpty());
                assertTrue(expectedCqlRowsForAllFunctions.isEmpty());
                assertTrue(expectedCqlRowsForAllRoles.isEmpty());
                assertTrue(expectedCqlRowsForAllMBeans.isEmpty());
            }
        }
        finally
        {
            executeCommandsToRemoveNewSuperUser();
        }
    }

    @Test
    public void showACoupleOfPermissionsForSuperUserTest() throws Throwable
    {
        useSuperUser();
        executeCommandsForNewSuperUser();
        Permission queriedPermissions[][] = new Permission[][] { {Permission.ALTER, Permission.EXECUTE}, {Permission.MODIFY, Permission.DROP} };
        try
        {
            for (int i = 0; i < queriedPermissions.length; i++)
            {
                Set<Permission> expectedPermissions = EnumSet.of(queriedPermissions[i][0], queriedPermissions[i][1]);
                List<Map<String,String>> expectedCqlRowsForAllKeyspaces = expectedSuperRoleCqlRows("jane", "jane", "<all keyspaces>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllFunctions = expectedSuperRoleCqlRows("jane", "jane", "<all functions>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllRoles = expectedSuperRoleCqlRows("jane", "jane", "<all roles>", expectedPermissions);
                List<Map<String,String>> expectedCqlRowsForAllMBeans = expectedSuperRoleCqlRows("jane", "jane", "<all mbeans>", expectedPermissions);            
                List<Map<String,String>> nonExpectedCqlRows = executeNet(String.format("LIST %s,%s OF jane", queriedPermissions[i][0].name(), queriedPermissions[i][1].name()))
                    .all()
                    .stream()
                    .map(
                        actualRow -> {
                            Map<String,String> actualCqlRow = new HashMap<String,String>();
                            actualCqlRow.put("role", actualRow.getString("role"));
                            actualCqlRow.put("username", actualRow.getString("username"));
                            actualCqlRow.put("resource", actualRow.getString("resource"));
                            actualCqlRow.put("permission", actualRow.getString("permission"));
                            return actualCqlRow;
                        }
                    )
                    .filter(
                        actualCqlRow -> {
                            if (expectedCqlRowsForAllKeyspaces.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllFunctions.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllRoles.remove(actualCqlRow))
                                return false;
                            if (expectedCqlRowsForAllMBeans.remove(actualCqlRow))
                                return false;
                            return true;
                        }
                    )
                    .collect(Collectors.toList());
                
                assertTrue(nonExpectedCqlRows.isEmpty());
                assertTrue(expectedCqlRowsForAllKeyspaces.isEmpty());
                assertTrue(expectedCqlRowsForAllFunctions.isEmpty());
                assertTrue(expectedCqlRowsForAllRoles.isEmpty());
                assertTrue(expectedCqlRowsForAllMBeans.isEmpty());
            }
        }
        finally
        {
            executeCommandsToRemoveNewSuperUser();
        }
    }

    @Test
    public void showPermissionsForRevokedSuperRoleTest() throws Throwable
    {
        useSuperUser();
        executeCommandsForNewSuperUser();
        executeNet("GRANT CREATE ON ALL KEYSPACES TO jane");
        executeNet("ALTER ROLE jane WITH SUPERUSER=false");
        try
        {
            List<Map<String,String>> nonExpectedCqlRows = executeNet("LIST ALL PERMISSIONS OF jane")
                .all()
                .stream()
                .map(
                    actualRow -> {
                        Map<String,String> actualCqlRow = new HashMap<String,String>();
                        actualCqlRow.put("role", actualRow.getString("role"));
                        actualCqlRow.put("username", actualRow.getString("username"));
                        actualCqlRow.put("resource", actualRow.getString("resource"));
                        actualCqlRow.put("permission", actualRow.getString("permission"));
                        return actualCqlRow;
                    }
                )
                .filter(
                    actualCqlRow -> {
                        if (actualCqlRow.get("permission").equals(Permission.CREATE.name()))
                            return false;
                        return true;
                    }
                )
                .collect(Collectors.toList());
            assertTrue(nonExpectedCqlRows.isEmpty());
        }
        finally
        {
            executeCommandsToRemoveNewSuperUser();
        }
    }

    private void executeCommandsForNewSuperUser() throws Throwable
    {
        executeNet("CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1};");
        executeNet("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (a text, b int, primary key (b));");
        executeNet("CREATE FUNCTION IF NOT EXISTS test_keyspace.test_function ( arg int ) RETURNS NULL on NULL INPUT RETURNS int LANGUAGE java AS $$ return arg; $$;");
        executeNet("CREATE ROLE IF NOT EXISTS jane WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = 'super'");
    }

    private void executeCommandsToRemoveNewSuperUser() throws Throwable
    {
        executeNet("DROP TABLE IF EXISTS test_keyspace.test_table");
        executeNet("DROP FUNCTION IF EXISTS test_keyspace.test_function");
        executeNet("DROP KEYSPACE IF EXISTS test_keyspace");
        executeNet("DROP ROLE IF EXISTS jane");
    }

    private List<Map<String, String>> expectedSuperRoleCqlRows(String role, String username, String resource, Set<Permission> expectedPermissions)
    {
        IResource rootResource = null;
        switch (resource)
        {
            case "<all keyspaces>":
                rootResource = DataResource.root();
                break;
            case "<all functions>":
                rootResource = FunctionResource.root();
                break;
            case "<all roles>":
                rootResource = RoleResource.root();
                break;
            case "<all mbeans>":
                rootResource = JMXResource.root();
                break;
        }
        return rootResource
        .applicablePermissions()
        .stream()
        .map(
            permission -> {
                if (expectedPermissions == null || expectedPermissions.contains(permission))
                {
                    Map<String,String> expectedCqlRow = new HashMap<String,String>();
                    expectedCqlRow.put("role", role);
                    expectedCqlRow.put("username", username);
                    expectedCqlRow.put("resource", resource);
                    expectedCqlRow.put("permission", permission.name());
                    return expectedCqlRow;
                }
                return null;
            }
        )
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    }
}
