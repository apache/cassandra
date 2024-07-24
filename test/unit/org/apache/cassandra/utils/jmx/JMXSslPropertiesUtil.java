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

import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;

public final class JMXSslPropertiesUtil
{
    public static WithProperties use(boolean enableRemoteSsl, boolean needClientAuth, String enabledProtocols, String cipherSuites)
    {
        return preserveAllProperties()
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL, enableRemoteSsl)
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH, needClientAuth)
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS, enabledProtocols)
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES, cipherSuites);
    }

    public static WithProperties use(boolean enableRemoteSsl, boolean needClientAuth, String enabledProtocols)
    {
        return preserveAllProperties()
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL, enableRemoteSsl)
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH, needClientAuth)
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS, enabledProtocols);
    }

    public static WithProperties use(boolean enableRemoteSsl)
    {
        return preserveAllProperties()
               .set(COM_SUN_MANAGEMENT_JMXREMOTE_SSL, enableRemoteSsl);
    }

    public static WithProperties preserveAllProperties()
    {
        return new WithProperties()
               .preserve(COM_SUN_MANAGEMENT_JMXREMOTE_SSL)
               .preserve(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH)
               .preserve(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS)
               .preserve(COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES)
               .preserve(JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS)
               .preserve(JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES);
    }
}
