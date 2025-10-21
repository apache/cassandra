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

import java.nio.file.Paths;

import org.junit.BeforeClass;

import org.apache.cassandra.config.JMXServerOptions;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_AUTHORIZER;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_LOGIN_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_SECURITY_AUTH_LOGIN_CONFIG;

/**
 * Tests via system properties normally set in cassandra-env.sh
 */
public class JMXAuthSystemPropertiesTest extends AbstractJMXAuthTest
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        setupAuthorizer();
        setupJMXServer(getJMXServerOptions());
    }

    private static JMXServerOptions getJMXServerOptions() throws Exception
    {
        String config = Paths.get(ClassLoader.getSystemResource("auth/cassandra-test-jaas.conf").toURI()).toString();
        COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE.setBoolean(true);
        JAVA_SECURITY_AUTH_LOGIN_CONFIG.setString(config);
        CASSANDRA_JMX_REMOTE_LOGIN_CONFIG.setString("TestLogin");
        CASSANDRA_JMX_AUTHORIZER.setString(NoSuperUserAuthorizationProxy.class.getName());
        CASSANDRA_JMX_LOCAL_PORT.setInt(9999);
        return JMXServerOptions.createParsingSystemProperties();
    }
}
