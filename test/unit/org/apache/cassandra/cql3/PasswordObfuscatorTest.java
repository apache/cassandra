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

import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class PasswordObfuscatorTest
{
    private static final PasswordObfuscator obfuscator = new PasswordObfuscator();

    @Test
    public void testCreatRoleWithLoginPriorToPassword()
    {
        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '123'"));
    }

    @Test
    public void testCreatRoleWithLoginAfterPassword()
    {
        assertEquals(format("CREATE ROLE role1 WITH password%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE ROLE role1 WITH password = '123' AND LOGIN = true"));
    }

    @Test
    public void testCreateRoleWithoutPassword()
    {
        assertEquals("CREATE ROLE role1", obfuscator.obfuscate("CREATE ROLE role1"));
    }

    @Test
    public void testCreateMultipleRoles()
    {
        assertEquals(format("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE ROLE role1 WITH LOGIN = true AND PASSWORD = '123';" +
                                          "CREATE ROLE role2 WITH LOGIN = true AND PASSWORD = '123'"));
    }

    @Test
    public void testAlterRoleWithPassword()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER ROLE role1 with PASSWORD = '123'"));
    }

    @Test
    public void testAlterRoleWithPasswordNoSpace()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER ROLE role1 with PASSWORD='123'"));
    }

    @Test
    public void testAlterRoleWithPasswordNoImmediateSpace()
    {
        assertEquals(format("ALTER ROLE role1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER ROLE role1 with PASSWORD= '123'"));
    }

    @Test
    public void testAlterRoleWithoutPassword()
    {
        assertEquals("ALTER ROLE role1", obfuscator.obfuscate("ALTER ROLE role1"));
    }

    @Test
    public void testCreateUserWithPassword()
    {
        assertEquals(format("CREATE USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE USER user1 with PASSWORD '123'"));
    }

    @Test
    public void testCreateUserWithoutPassword()
    {
        assertEquals("CREATE USER user1", obfuscator.obfuscate("CREATE USER user1"));
    }

    @Test
    public void testAlterUserWithPassword()
    {
        assertEquals(format("ALTER USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER USER user1 with PASSWORD '123'"));
    }

    @Test
    public void testAlterUserWithPasswordMixedCase()
    {
        assertEquals(format("ALTER USER user1 with paSSwoRd%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER USER user1 with paSSwoRd '123'"));
    }

    @Test
    public void testAlterUserWithPasswordWithNewLine()
    {
        assertEquals(format("ALTER USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("ALTER USER user1 with PASSWORD\n'123'"));
    }

    @Test
    public void testPasswordWithNewLinesObfuscation()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE USER user1 with PASSWORD 'a\nb'"));
    }

    @Test
    public void testEmptyPasswordObfuscation()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE USER user1 with PASSWORD ''"));
    }

    @Test
    public void testPasswordWithSpaces()
    {
        assertEquals(String.format("CREATE USER user1 with PASSWORD%s", PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("CREATE USER user1 with PASSWORD 'p a ss wor d'"));
    }

    @Test
    public void testSimpleBatch()
    {
        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD%s",
                            PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("BEGIN BATCH \n" +
                                          "    CREATE ROLE alice1 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "APPLY BATCH;"));
    }

    @Test
    public void testComplexBatch()
    {
        assertEquals(format("BEGIN BATCH \n" +
                            "    CREATE ROLE alice1 WITH PASSWORD%s",
                            PasswordObfuscator.OBFUSCATION_TOKEN),
                     obfuscator.obfuscate("BEGIN BATCH \n" +
                                          "    CREATE ROLE alice1 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "    CREATE ROLE alice2 WITH PASSWORD = 'alice123' and LOGIN = true; \n" +
                                          "APPLY BATCH;"));
    }
}