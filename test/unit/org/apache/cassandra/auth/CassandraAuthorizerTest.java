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

package org.apache.cassandra.auth;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;

import static java.lang.String.format;

public class CassandraAuthorizerTest extends CQLTester
{
    private static final String PARENT = "parent";
    private static final String CHILD = "child";
    private static final String OTHER = "other";
    private static final String PASSWORD = "secret";

    @BeforeClass
    public static void setupClass()
    {
        CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @Test
    public void testListPermissionsOfChildByParent() throws Throwable
    {
        useSuperUser();

        // create parent role by super user
        executeNet(format("CREATE ROLE %s WITH login=true AND password='%s'", PARENT, PASSWORD));
        executeNet(format("GRANT CREATE ON ALL ROLES TO %s", PARENT));
        assertRowsNet(executeNet(format("LIST ALL PERMISSIONS OF %s", PARENT)),
                      row(PARENT, PARENT, "<all roles>", "CREATE"));

        // create other role by super user
        executeNet(format("CREATE ROLE %s WITH login=true AND password='%s'", OTHER, PASSWORD));
        assertRowsNet(executeNet(format("LIST ALL PERMISSIONS OF %s", OTHER)));

        useUser(PARENT, PASSWORD);

        // create child role by parent
        executeNet(format("CREATE ROLE %s WITH login = true AND password='%s'", CHILD, PASSWORD));

        // list permissions by parent
        assertRowsNet(executeNet(format("LIST ALL PERMISSIONS OF %s", PARENT)),
                      row(PARENT, PARENT, "<all roles>", "CREATE"),
                      row(PARENT, PARENT, "<role child>", "ALTER"),
                      row(PARENT, PARENT, "<role child>", "DROP"),
                      row(PARENT, PARENT, "<role child>", "AUTHORIZE"),
                      row(PARENT, PARENT, "<role child>", "DESCRIBE"));
        assertRowsNet(executeNet(format("LIST ALL PERMISSIONS OF %s", CHILD)));
        assertInvalidMessageNet(format("You are not authorized to view %s's permissions", OTHER),
                                format("LIST ALL PERMISSIONS OF %s", OTHER));

        useUser(CHILD, PASSWORD);

        // list permissions by child
        assertInvalidMessageNet(format("You are not authorized to view %s's permissions", PARENT),
                                format("LIST ALL PERMISSIONS OF %s", PARENT));
        assertRowsNet(executeNet(format("LIST ALL PERMISSIONS OF %s", CHILD)));
        assertInvalidMessageNet(format("You are not authorized to view %s's permissions", OTHER),
                                format("LIST ALL PERMISSIONS OF %s", OTHER));

        // try to create role by child
        assertInvalidMessageNet(format("User %s does not have sufficient privileges to perform the requested operation", CHILD),
                                format("CREATE ROLE %s WITH login=true AND password='%s'", "nope", PASSWORD));

        useUser(PARENT, PASSWORD);

        // alter child's role by parent
        executeNet(format("ALTER ROLE %s WITH login = false", CHILD));
        executeNet(format("DROP ROLE %s", CHILD));
    }
}
