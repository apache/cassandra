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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.CQLTester;

public class RoleSyntaxTest extends CQLTester
{
    private static final String NO_QUOTED_USERNAME = "Quoted strings are are not supported for user names " +
                                                     "and USER is deprecated, please use ROLE";

    @Test
    public void standardOptionsSyntaxTest() throws Throwable
    {
        assertValidSyntax("CREATE ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'");
        assertValidSyntax("CREATE ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false");
        assertValidSyntax("CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
        assertValidSyntax("CREATE ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false");
        assertValidSyntax("CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");

        assertValidSyntax("ALTER ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'");
        assertValidSyntax("ALTER ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false");
        assertValidSyntax("ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
        assertValidSyntax("ALTER ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false");
        assertValidSyntax("ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
    }

    @Test
    public void customOptionsSyntaxTest() throws Throwable
    {
        assertValidSyntax("CREATE ROLE r WITH OPTIONS = {'a':'b', 'b':1}");
        assertInvalidSyntax("CREATE ROLE r WITH OPTIONS = 'term'");
        assertInvalidSyntax("CREATE ROLE r WITH OPTIONS = 99");

        assertValidSyntax("ALTER ROLE r WITH OPTIONS = {'a':'b', 'b':1}");
        assertInvalidSyntax("ALTER ROLE r WITH OPTIONS = 'term'");
        assertInvalidSyntax("ALTER ROLE r WITH OPTIONS = 99");
    }

    @Test
    public void createSyntaxTest() throws Throwable
    {
        assertValidSyntax("CREATE ROLE r1");
        assertValidSyntax("CREATE ROLE 'r1'");
        assertValidSyntax("CREATE ROLE \"r1\"");
        assertValidSyntax("CREATE ROLE $$r1$$");
        assertValidSyntax("CREATE ROLE $$ r1 ' x $ x ' $$");
        assertValidSyntax("CREATE USER u1");
        assertValidSyntax("CREATE USER 'u1'");
        assertValidSyntax("CREATE USER $$u1$$");
        assertValidSyntax("CREATE USER $$ u1 ' x $ x ' $$");
        // user names may not be quoted names
        assertInvalidSyntax("CREATE USER \"u1\"", NO_QUOTED_USERNAME);
    }

    @Test
    public void dropSyntaxTest() throws Throwable
    {
        assertValidSyntax("DROP ROLE r1");
        assertValidSyntax("DROP ROLE 'r1'");
        assertValidSyntax("DROP ROLE \"r1\"");
        assertValidSyntax("DROP ROLE $$r1$$");
        assertValidSyntax("DROP ROLE $$ r1 ' x $ x ' $$");
        assertValidSyntax("DROP USER u1");
        assertValidSyntax("DROP USER 'u1'");
        assertValidSyntax("DROP USER $$u1$$");
        assertValidSyntax("DROP USER $$ u1 ' x $ x ' $$");
        // user names may not be quoted names
        assertInvalidSyntax("DROP USER \"u1\"", NO_QUOTED_USERNAME);
    }

    @Test
    public void alterSyntaxTest() throws Throwable
    {
        assertValidSyntax("ALTER ROLE r1 WITH PASSWORD = 'password'");
        assertValidSyntax("ALTER ROLE 'r1' WITH PASSWORD = 'password'");
        assertValidSyntax("ALTER ROLE \"r1\" WITH PASSWORD = 'password'");
        assertValidSyntax("ALTER ROLE $$r1$$ WITH PASSWORD = 'password'");
        assertValidSyntax("ALTER ROLE $$ r1 ' x $ x ' $$ WITH PASSWORD = 'password'");
        // ALTER has slightly different form for USER (no =)
        assertValidSyntax("ALTER USER u1 WITH PASSWORD 'password'");
        assertValidSyntax("ALTER USER 'u1' WITH PASSWORD 'password'");
        assertValidSyntax("ALTER USER $$u1$$ WITH PASSWORD 'password'");
        assertValidSyntax("ALTER USER $$ u1 ' x $ x ' $$ WITH PASSWORD 'password'");
        // ALTER with IF EXISTS syntax
        assertValidSyntax("ALTER ROLE IF EXISTS r1 WITH PASSWORD = 'password'");
        assertValidSyntax("ALTER USER IF EXISTS u1 WITH PASSWORD 'password'");
        // user names may not be quoted names
        assertInvalidSyntax("ALTER USER \"u1\" WITH PASSWORD 'password'", NO_QUOTED_USERNAME);
    }

    @Test
    public void grantRevokePermissionsSyntaxTest() throws Throwable
    {
        for (String r1 : Arrays.asList("r1", "'r1'", "\"r1\"", "$$r1$$"))
        {
            for (String r2 : Arrays.asList("r2", "\"r2\"", "'r2'", "$$ r '2' $$"))
            {
                // grant/revoke on RoleResource
                assertValidSyntax(String.format("GRANT ALTER ON ROLE %s TO %s", r1, r2));
                assertValidSyntax(String.format("GRANT ALTER PERMISSION ON ROLE %s TO %s", r1, r2));
                assertValidSyntax(String.format("REVOKE ALTER ON ROLE %s FROM %s", r1, r2));
                assertValidSyntax(String.format("REVOKE ALTER PERMISSION ON ROLE %s FROM %s", r1, r2));

                // grant/revoke multiple permissions in a single statement
                assertValidSyntax(String.format("GRANT CREATE, ALTER ON ROLE %s TO %s", r1, r2));
                assertValidSyntax(String.format("GRANT CREATE PERMISSION, ALTER PERMISSION ON ROLE %s TO %s", r1, r2));
                assertValidSyntax(String.format("REVOKE CREATE, ALTER ON ROLE %s FROM %s", r1, r2));
                assertValidSyntax(String.format("REVOKE CREATE PERMISSION, ALTER PERMISSION ON ROLE %s FROM %s", r1, r2));
            }
        }

        for (String r1 : Arrays.asList("r1", "'r1'", "\"r1\"", "$$r1$$", "$$ r '1' $$"))
        {
            // grant/revoke on DataResource
            assertValidSyntax(String.format("GRANT SELECT ON KEYSPACE ks TO %s", r1));
            assertValidSyntax(String.format("GRANT SELECT PERMISSION ON KEYSPACE ks TO %s", r1));
            assertValidSyntax(String.format("REVOKE SELECT ON KEYSPACE ks FROM %s", r1));
            assertValidSyntax(String.format("REVOKE SELECT PERMISSION ON KEYSPACE ks FROM %s", r1));

            // grant/revoke multiple permissions in a single statement
            assertValidSyntax(String.format("GRANT MODIFY, SELECT ON KEYSPACE ks TO %s", r1));
            assertValidSyntax(String.format("GRANT MODIFY PERMISSION, SELECT PERMISSION ON KEYSPACE ks TO %s", r1));
            assertValidSyntax(String.format("GRANT MODIFY, SELECT ON ALL KEYSPACES TO %s", r1));
            assertValidSyntax(String.format("GRANT MODIFY PERMISSION, SELECT PERMISSION ON ALL KEYSPACES TO %s", r1));
            assertValidSyntax(String.format("REVOKE MODIFY, SELECT ON KEYSPACE ks FROM %s", r1));
            assertValidSyntax(String.format("REVOKE MODIFY PERMISSION, SELECT PERMISSION ON KEYSPACE ks FROM %s", r1));
            assertValidSyntax(String.format("REVOKE MODIFY, SELECT ON ALL KEYSPACES FROM %s", r1));
            assertValidSyntax(String.format("REVOKE MODIFY PERMISSION, SELECT PERMISSION ON ALL KEYSPACES FROM %s", r1));
        }
    }

    @Test
    public void listPermissionsSyntaxTest() throws Throwable
    {
        for (String r1 : Arrays.asList("r1", "'r1'", "\"r1\"", "$$r1$$", "$$ r '1' $$"))
        {
            assertValidSyntax(String.format("LIST ALL PERMISSIONS ON ALL ROLES OF %s", r1));
            assertValidSyntax(String.format("LIST ALL PERMISSIONS ON ALL KEYSPACES OF %s", r1));
            assertValidSyntax(String.format("LIST ALL PERMISSIONS OF %s", r1));
            assertValidSyntax(String.format("LIST MODIFY PERMISSION ON KEYSPACE ks OF %s", r1));
            assertValidSyntax(String.format("LIST MODIFY, SELECT OF %s", r1));
            assertValidSyntax(String.format("LIST MODIFY, SELECT PERMISSION ON KEYSPACE ks OF %s", r1));

            for (String r2 : Arrays.asList("r2", "\"r2\"", "'r2'", "$$ r '2' $$"))
            {
                assertValidSyntax(String.format("LIST ALL PERMISSIONS ON ROLE %s OF %s", r1, r2));
                assertValidSyntax(String.format("LIST ALTER PERMISSION ON ROLE %s OF %s", r1, r2));
                assertValidSyntax(String.format("LIST ALTER, DROP PERMISSION ON ROLE %s OF %s", r1, r2));
            }
        }
    }

    @Test
    public void listRolesSyntaxTest() throws Throwable
    {
        assertValidSyntax("LIST ROLES OF r1");
        assertValidSyntax("LIST ROLES OF 'r1'");
        assertValidSyntax("LIST ROLES OF \"r1\"");
        assertValidSyntax("LIST ROLES OF $$ r '1' $$");
    }

    @Test
    public void roleNameTest()
    {
        // we used to split on all "/" which meant role names containing a / would trigger an exception in RoleResource.fromName()
        RoleResource t1 = RoleResource.role("ki/ng");
        RoleResource t2 = RoleResource.role("emperor");
        RoleResource t3 = RoleResource.role("aeou/!@*%");
        RoleResource t4 = RoleResource.role("do$\\$P#?:");
        RoleResource t5 = RoleResource.root();
        RoleResource r1 = RoleResource.fromName("roles/ki/ng");
        RoleResource r2 = RoleResource.fromName("roles/emperor");
        RoleResource r3 = RoleResource.fromName("roles/aeou/!@*%");
        RoleResource r4 = RoleResource.fromName("roles/do$\\$P#?:");
        RoleResource r5 = RoleResource.fromName("roles");
        Assert.assertEquals(t1, r1);
        Assert.assertEquals(t2, r2);
        Assert.assertEquals(t3, r3);
        Assert.assertEquals(t4, r4);
        Assert.assertEquals(t5, r5);
    }
}
