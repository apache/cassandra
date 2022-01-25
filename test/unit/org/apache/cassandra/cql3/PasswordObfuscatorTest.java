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

package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.RoleOptions;

import static org.apache.cassandra.cql3.PasswordObfuscator.*;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class PasswordObfuscatorTest
{
    private static final RoleOptions opts = new RoleOptions();
    private static final String optsPassword = "testpassword";

    @BeforeClass
    public static void startup()
    {
        opts.setOption(org.apache.cassandra.auth.IRoleManager.Option.PASSWORD, "testpassword");
    }

    @Test
    public void testCreatRoleWithLoginPriorToPassword()
    {
        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '123'"));

        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '%s'", optsPassword), opts));
    }

    @Test
    public void testCreatRoleWithLoginAfterPassword()
    {
        assertEquals(format("CREATE ROLE role1 WITH password %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE ROLE role1 WITH password = '123' AND LOGIN = true"));

        assertEquals(format("CREATE ROLE role1 WITH password = '%s' AND LOGIN = true", OBFUSCATION_TOKEN),
                     obfuscate(format("CREATE ROLE role1 WITH password = '%s' AND LOGIN = true", optsPassword), opts));
    }

    @Test
    public void testCreateRoleWithoutPassword()
    {
        assertEquals("CREATE ROLE role1", obfuscate("CREATE ROLE role1"));
        assertEquals("CREATE ROLE role1", obfuscate("CREATE ROLE role1", opts));
    }

    @Test
    public void testCreateMultipleRoles()
    {
        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '123';" +
                                                  "CREATE ROLE role2 WITH LOGIN = true AND PASSWORD = '123'"));

        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '%s';"
                            + "CREATE ROLE role2 WITH LOGIN = true AND PASSWORD = '%s'", OBFUSCATION_TOKEN, OBFUSCATION_TOKEN),
                     obfuscate(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '%s';"
                                                         + "CREATE ROLE role2 WITH LOGIN = true AND PASSWORD = '%s'", optsPassword, optsPassword),
                                                  opts));
    }

    @Test
    public void testAlterRoleWithPassword()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER ROLE role1 with PASSWORD = '123'"));

        assertEquals(format("ALTER ROLE role1 with PASSWORD = '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER ROLE role1 with PASSWORD = '%s'", optsPassword), opts));
    }

    @Test
    public void testAlterRoleWithPasswordNoSpace()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER ROLE role1 with PASSWORD='123'"));

        assertEquals(format("ALTER ROLE role1 with PASSWORD='%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER ROLE role1 with PASSWORD='%s'", optsPassword), opts));
    }

    @Test
    public void testAlterRoleWithPasswordNoImmediateSpace()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER ROLE role1 with PASSWORD= '123'"));

        assertEquals(format("ALTER ROLE role1 with PASSWORD= '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER ROLE role1 with PASSWORD= '%s'", optsPassword), opts));
    }

    @Test
    public void testAlterRoleWithoutPassword()
    {
        assertEquals("ALTER ROLE role1", obfuscate("ALTER ROLE role1"));

        assertEquals("ALTER ROLE role1", obfuscate("ALTER ROLE role1", opts));
    }

    @Test
    public void testCreateUserWithPassword()
    {
        assertEquals(format("CREATE USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE USER user1 with PASSWORD '123'"));

        assertEquals(format("CREATE USER user1 with PASSWORD '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("CREATE USER user1 with PASSWORD '%s'", optsPassword), opts));
    }

    @Test
    public void testCreateUserWithoutPassword()
    {
        assertEquals("CREATE USER user1", obfuscate("CREATE USER user1"));

        assertEquals("CREATE USER user1", obfuscate("CREATE USER user1", opts));
    }

    @Test
    public void testAlterUserWithPassword()
    {
        assertEquals(format("ALTER USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER USER user1 with PASSWORD '123'"));

        assertEquals(format("ALTER USER user1 with PASSWORD '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER USER user1 with PASSWORD '%s'", optsPassword), opts));
    }

    @Test
    public void testAlterUserWithPasswordMixedCase()
    {
        assertEquals(format("ALTER USER user1 with paSSwoRd %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER USER user1 with paSSwoRd '123'"));

        assertEquals(format("ALTER USER user1 with paSSwoRd '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER USER user1 with paSSwoRd '%s'", optsPassword), opts));
    }

    @Test
    public void testAlterUserWithPasswordWithNewLine()
    {
        assertEquals(format("ALTER USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("ALTER USER user1 with PASSWORD\n'123'"));

        assertEquals(format("ALTER USER user1 with PASSWORD\n'%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("ALTER USER user1 with PASSWORD\n'%s'", optsPassword), opts));
    }

    @Test
    public void testPasswordWithNewLinesObfuscation()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE USER user1 with PASSWORD 'a\nb'"));

        RoleOptions newLinePassOpts = new RoleOptions();
        newLinePassOpts.setOption(org.apache.cassandra.auth.IRoleManager.Option.PASSWORD, "test\npassword");
        assertEquals(String.format("CREATE USER user1 with PASSWORD '%s'", OBFUSCATION_TOKEN),
                     obfuscate(format("CREATE USER user1 with PASSWORD '%s'", "test\npassword"), newLinePassOpts));
    }

    @Test
    public void testEmptyPasswordObfuscation()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE USER user1 with PASSWORD ''"));

        RoleOptions emptyPassOpts = new RoleOptions();
        emptyPassOpts.setOption(org.apache.cassandra.auth.IRoleManager.Option.PASSWORD, "");
        assertEquals("CREATE USER user1 with PASSWORD ''",
                     obfuscate("CREATE USER user1 with PASSWORD ''", emptyPassOpts));
    }

    @Test
    public void testPasswordWithSpaces()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD %s", OBFUSCATION_TOKEN),
                     obfuscate("CREATE USER user1 with PASSWORD 'p a ss wor d'"));
    }

    @Test
    public void testSimpleBatch()
    {
        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD %s",
                            OBFUSCATION_TOKEN),
                     obfuscate("BEGIN BATCH \n" +
                                          "    CREATE ROLE alice1 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "APPLY BATCH;"));

        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                            "APPLY BATCH;", OBFUSCATION_TOKEN),
                     obfuscate(format("BEGIN BATCH \n" +
                                      "    CREATE ROLE alice1 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                                      "APPLY BATCH;", optsPassword),
                               opts));
    }

    @Test
    public void testComplexBatch()
    {
        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD %s",
                            OBFUSCATION_TOKEN),
                     obfuscate("BEGIN BATCH \n" +
                                          "    CREATE ROLE alice1 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "    CREATE ROLE alice2 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "APPLY BATCH;"));

        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                            "    CREATE ROLE alice2 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                            "APPLY BATCH;"
                            , OBFUSCATION_TOKEN, OBFUSCATION_TOKEN),
                     obfuscate(format("BEGIN BATCH \n" +
                                      "    CREATE ROLE alice1 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                                      "    CREATE ROLE alice2 WITH PASSWORD = '%s' and LOGIN = true; \n" +
                                      "APPLY BATCH;", optsPassword, optsPassword),
                               opts));
    }
}
