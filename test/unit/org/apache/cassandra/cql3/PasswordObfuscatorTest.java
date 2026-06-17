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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleOptions;

import static org.apache.cassandra.cql3.PasswordObfuscator.*;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PasswordObfuscatorTest
{
    private static final RoleOptions opts = new RoleOptions();
    private static final String optsPassword = "testpassword";
    public static final int ASCII_START = 32;
    public static final int ASCII_END = 126;

    @BeforeClass
    public static void startup()
    {
        opts.setOption(org.apache.cassandra.auth.IRoleManager.Option.PASSWORD, optsPassword);
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

    /**
     * Tests that passwords containing the literal string \E are properly obfuscated.
     * <p>
     * The \E sequence is special in Java regex as it ends a \Q...\E quoted block.
     * If inline \Q...\E quoting is used instead of Pattern.quote(), passwords containing
     * \E will break the regex pattern and cause either an exception or failed obfuscation.
     */
    @Test
    public void testPasswordWithRegexEndQuote()
    {
        // \E in the middle
        assertPasswordObfuscated("secret\\Epassword");

        // \E at the start
        assertPasswordObfuscated("\\Esecretpassword");

        // \E at the end
        assertPasswordObfuscated("secretpassword\\E");

        // \E followed by regex special char +
        assertPasswordObfuscated("secret\\E+password");

        // Multiple \E sequences
        assertPasswordObfuscated("sec\\Eret\\Epass");
    }

    /**
     * Helper method to test that a password is properly obfuscated in a CREATE ROLE statement.
     *
     * @param password the password to test (unescaped, as stored in RoleOptions)
     */
    private void assertPasswordObfuscated(String password)
    {
        RoleOptions roleOpts = new RoleOptions();
        roleOpts.setOption(IRoleManager.Option.PASSWORD, password);

        // Escape single quotes for CQL query string
        String escapedPassword = password.replace("'", "''");
        String query = format("CREATE ROLE role1 WITH PASSWORD = '%s'", escapedPassword);
        String expected = format("CREATE ROLE role1 WITH PASSWORD = '%s'", OBFUSCATION_TOKEN);

        assertEquals("Password should be obfuscated: " + password, expected, obfuscate(query, roleOpts));
    }

    /**
     * Tests that passwords containing each printable special character (outside a-z, A-Z, 0-9)
     * are properly obfuscated by PasswordObfuscator.
     * <p>
     * This test iterates through all printable ASCII special characters (32-126, excluding
     * alphanumeric) and tests each character at three positions: start, middle, and end of
     * the password. Any failures are collected and reported at the end.
     */
    @Test
    public void testAllPrintableSpecialCharactersObfuscation()
    {
        List<String> failures = new ArrayList<>();

        for (char c = ASCII_START; c <= ASCII_END; c++)
        {
            // Skip alphanumeric characters
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
                continue;

            // Test character at start, middle, and end of password
            String[] passwords = {
                c + "password",        // at start
                "pass" + c + "word",   // in middle
                "password" + c         // at end
            };

            for (String password : passwords)
            {
                String escapedPassword = password.replace("'", "''");
                String query = format("CREATE ROLE role1 WITH PASSWORD = '%s'", escapedPassword);
                String expected = format("CREATE ROLE role1 WITH PASSWORD = '%s'", OBFUSCATION_TOKEN);

                RoleOptions testOpts = new RoleOptions();
                testOpts.setOption(IRoleManager.Option.PASSWORD, password);

                try
                {
                    String result = obfuscate(query, testOpts);

                    if (!expected.equals(result))
                    {
                        if (result.contains(escapedPassword) || result.contains(password))
                        {
                            failures.add(format("Password '%s': PASSWORD LEAKED - Result: %s",
                                                password, result));
                        }
                        else
                        {
                            failures.add(format("Password '%s': Unexpected result - Expected: %s, Got: %s",
                                                password, expected, result));
                        }
                    }
                }
                catch (Exception e)
                {
                    failures.add(format("Password '%s': Exception thrown - %s: %s",
                                        password, e.getClass().getSimpleName(), e.getMessage()));
                }
            }
        }

        if (!failures.isEmpty())
        {
            StringBuilder sb = new StringBuilder();
            sb.append(format("%d password(s) with special characters failed obfuscation:\n", failures.size()));
            for (String failure : failures)
            {
                sb.append("  - ").append(failure).append("\n");
            }
            fail(sb.toString());
        }
    }
}
