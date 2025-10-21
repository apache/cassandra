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

package org.apache.cassandra.utils.jmx;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTOREPASSWORD;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTOREPASSWORD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DuplicateJMXConfigurationTest
{
    @Test
    public void testDuplicateConfiguration()
    {
        String enabledProtocols = "TLSv1.2,TLSv1.3,TLSv1.1";
        String cipherSuites = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";

        try (WithProperties props = new WithProperties().set(CASSANDRA_CONFIG, "cassandra-jmx-sslconfig.yaml")
                                                        .set(CASSANDRA_JMX_REMOTE_PORT, 7199)
                                                        .set(COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE, true)
                                                        .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL, true)
                                                        .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH, true)
                                                        .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS, enabledProtocols)
                                                        .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES, cipherSuites)
                                                        .set(JAVAX_NET_SSL_KEYSTORE, "test/conf/cassandra_ssl_test.keystore")
                                                        .set(JAVAX_NET_SSL_TRUSTSTORE, "test/conf/cassandra_ssl_test.truststore")
                                                        .set(JAVAX_NET_SSL_KEYSTOREPASSWORD, "cassandra")
                                                        .set(JAVAX_NET_SSL_TRUSTSTOREPASSWORD, "cassandra"))
        {
            assertThatThrownBy(DatabaseDescriptor::daemonInitialization)
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Configure either jmx_server_options in cassandra.yaml and comment out configure_jmx function " +
                                  "call in cassandra-env.sh or keep cassandra-env.sh to call configure_jmx function but you have to keep " +
                                  "jmx_server_options in cassandra.yaml commented out.");
        }
    }
}
