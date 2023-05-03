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

package org.apache.cassandra.auth.jmx;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.*;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.utils.JMXServerUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_AUTHORIZER;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_LOGIN_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_SECURITY_AUTH_LOGIN_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JMXAuthTest extends CQLTester
{
    private static JMXConnectorServer jmxServer;
    private static MBeanServerConnection connection;
    private RoleResource role;
    private String tableName;
    private JMXResource tableMBean;

    @FunctionalInterface
    private interface MBeanAction
    {
        void execute();
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        setupAuthorizer();
        setupJMXServer();
    }

    private static void setupAuthorizer()
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

    private static void setupJMXServer() throws Exception
    {
        String config = Paths.get(ClassLoader.getSystemResource("auth/cassandra-test-jaas.conf").toURI()).toString();
        COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE.setBoolean(true);
        JAVA_SECURITY_AUTH_LOGIN_CONFIG.setString(config);
        CASSANDRA_JMX_REMOTE_LOGIN_CONFIG.setString("TestLogin");
        CASSANDRA_JMX_AUTHORIZER.setString(NoSuperUserAuthorizationProxy.class.getName());
        jmxServer = JMXServerUtils.createJMXServer(9999, "localhost", true);
        jmxServer.start();

        JMXServiceURL jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", RMISocketFactory.getDefaultSocketFactory());
        JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        connection = jmxc.getMBeanServerConnection();
    }

    @Before
    public void setup() throws Throwable
    {
        role = RoleResource.role("test_role");
        clearAllPermissions();
        tableName = createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k))");
        tableMBean = JMXResource.mbean(String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,table=%s",
                                                     KEYSPACE, tableName));
    }

    @Test
    public void readAttribute() throws Throwable
    {
        ColumnFamilyStoreMBean proxy = JMX.newMBeanProxy(connection,
                                                         ObjectName.getInstance(tableMBean.getObjectName()),
                                                         ColumnFamilyStoreMBean.class);

        // grant SELECT on a single specific Table mbean
        assertPermissionOnResource(Permission.SELECT, tableMBean, proxy::getTableName);

        // grant SELECT on all Table mbeans in named keyspace
        clearAllPermissions();
        JMXResource allTablesInKeyspace = JMXResource.mbean(String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,*",
                                                                          KEYSPACE));
        assertPermissionOnResource(Permission.SELECT, allTablesInKeyspace, proxy::getTableName);

        // grant SELECT on all Table mbeans
        clearAllPermissions();
        JMXResource allTables = JMXResource.mbean("org.apache.cassandra.db:type=Tables,*");
        assertPermissionOnResource(Permission.SELECT, allTables, proxy::getTableName);

        // grant SELECT ON ALL MBEANS
        clearAllPermissions();
        assertPermissionOnResource(Permission.SELECT, JMXResource.root(), proxy::getTableName);
    }

    @Test
    public void writeAttribute() throws Throwable
    {
        ColumnFamilyStoreMBean proxy = JMX.newMBeanProxy(connection,
                                                         ObjectName.getInstance(tableMBean.getObjectName()),
                                                         ColumnFamilyStoreMBean.class);
        MBeanAction action = () -> proxy.setMinimumCompactionThreshold(4);

        // grant MODIFY on a single specific Table mbean
        assertPermissionOnResource(Permission.MODIFY, tableMBean, action);

        // grant MODIFY on all Table mbeans in named keyspace
        clearAllPermissions();
        JMXResource allTablesInKeyspace = JMXResource.mbean(String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,*",
                                                                          KEYSPACE));
        assertPermissionOnResource(Permission.MODIFY, allTablesInKeyspace, action);

        // grant MODIFY on all Table mbeans
        clearAllPermissions();
        JMXResource allTables = JMXResource.mbean("org.apache.cassandra.db:type=Tables,*");
        assertPermissionOnResource(Permission.MODIFY, allTables, action);

        // grant MODIFY ON ALL MBEANS
        clearAllPermissions();
        assertPermissionOnResource(Permission.MODIFY, JMXResource.root(), action);
    }

    @Test
    public void executeMethod() throws Throwable
    {
        ColumnFamilyStoreMBean proxy = JMX.newMBeanProxy(connection,
                                                         ObjectName.getInstance(tableMBean.getObjectName()),
                                                         ColumnFamilyStoreMBean.class);

        // grant EXECUTE on a single specific Table mbean
        assertPermissionOnResource(Permission.EXECUTE, tableMBean, proxy::estimateKeys);

        // grant EXECUTE on all Table mbeans in named keyspace
        clearAllPermissions();
        JMXResource allTablesInKeyspace = JMXResource.mbean(String.format("org.apache.cassandra.db:type=Tables,keyspace=%s,*",
                                                                          KEYSPACE));
        assertPermissionOnResource(Permission.EXECUTE, allTablesInKeyspace, proxy::estimateKeys);

        // grant EXECUTE on all Table mbeans
        clearAllPermissions();
        JMXResource allTables = JMXResource.mbean("org.apache.cassandra.db:type=Tables,*");
        assertPermissionOnResource(Permission.EXECUTE, allTables, proxy::estimateKeys);

        // grant EXECUTE ON ALL MBEANS
        clearAllPermissions();
        assertPermissionOnResource(Permission.EXECUTE, JMXResource.root(), proxy::estimateKeys);
    }

    private void assertPermissionOnResource(Permission permission,
                                            JMXResource resource,
                                            MBeanAction action)
    {
        assertUnauthorized(action);
        grantPermission(permission, resource, role);
        assertAuthorized(action);
    }

    private void grantPermission(Permission permission, JMXResource resource, RoleResource role)
    {
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                 ImmutableSet.of(permission),
                                                 resource,
                                                 role);
    }

    private void assertAuthorized(MBeanAction action)
    {
        action.execute();
    }

    private void assertUnauthorized(MBeanAction action)
    {
        try
        {
            action.execute();
            fail("Expected an UnauthorizedException, but none was thrown");
        }
        catch (SecurityException e)
        {
            assertEquals("Access Denied", e.getLocalizedMessage());
        }
    }

    private void clearAllPermissions()
    {
        ((StubAuthorizer) DatabaseDescriptor.getAuthorizer()).clear();
    }

    public static class StubLoginModule implements LoginModule
    {
        private CassandraPrincipal principal;
        private Subject subject;

        public StubLoginModule(){}

        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options)
        {
            this.subject = subject;
            principal = new CassandraPrincipal((String)options.get("role_name"));
        }

        public boolean login() throws LoginException
        {
            return true;
        }

        public boolean commit() throws LoginException
        {
            if (!subject.getPrincipals().contains(principal))
                subject.getPrincipals().add(principal);
            return true;
        }

        public boolean abort() throws LoginException
        {
            return true;
        }

        public boolean logout() throws LoginException
        {
            return true;
        }
    }

    // always answers false to isSuperUser and true to isAuthSetup complete - saves us having to initialize
    // a real IRoleManager and StorageService for the test
    public static class NoSuperUserAuthorizationProxy extends AuthorizationProxy
    {
        public NoSuperUserAuthorizationProxy()
        {
            super();
            this.isSuperuser = (role) -> false;
            this.isAuthSetupComplete = () -> true;
        }
    }
}
