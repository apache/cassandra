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
package org.apache.cassandra.config;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_AUTHORIZER;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_LOGIN_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_KEYSTOREPASSWORD;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTORE;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_NET_SSL_TRUSTSTOREPASSWORD;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_SECURITY_AUTH_LOGIN_CONFIG;

public class JMXServerOptions
{
    //jmx server settings
    public final Boolean enabled;
    public final Boolean remote;
    public final int jmx_port;
    public final int rmi_port;
    public final Boolean authenticate;

    // ssl options
    public final EncryptionOptions jmx_encryption_options;

    // options for using Cassandra's own authentication mechanisms
    public final String login_config_name;
    public final String login_config_file;

    // location for credentials file if using JVM's file-based authentication
    public final String password_file;
    // location of standard access file, if using JVM's file-based access control
    public final String access_file;

    // classname of authorizer if using a custom authz mechanism. Usually, this will
    // refer to o.a.c.auth.jmx.AuthorizationProxy which delegates to the IAuthorizer
    // configured in cassandra.yaml
    public final String authorizer;

    public JMXServerOptions()
    {
        this(true, false, 7199, 0, false,
             new EncryptionOptions(), null, null, null,
             null, null);
    }

    public static JMXServerOptions create(boolean enabled, boolean local, int jmxPort, EncryptionOptions options)
    {
        return new JMXServerOptions(enabled, !local, jmxPort, 0, false,
                                    options, null, null, null,
                                    null, null);
    }

    public static JMXServerOptions fromDescriptor(boolean enabled, boolean local, int jmxPort)
    {
        JMXServerOptions from = DatabaseDescriptor.getJmxServerOptions();
        return new JMXServerOptions(enabled, !local, jmxPort, jmxPort, from.authenticate,
                                    from.jmx_encryption_options, from.login_config_name, from.login_config_file, from.password_file,
                                    from.access_file, from.authorizer);
    }

    public JMXServerOptions(Boolean enabled,
                            Boolean remote,
                            int jmxPort,
                            int rmiPort,
                            Boolean authenticate,
                            EncryptionOptions jmx_encryption_options,
                            String loginConfigName,
                            String loginConfigFile,
                            String passwordFile,
                            String accessFile,
                            String authorizer)
    {
        this.enabled = enabled;
        this.remote = remote;
        this.jmx_port = jmxPort;
        this.rmi_port = rmiPort;
        this.authenticate = authenticate;
        this.jmx_encryption_options = jmx_encryption_options;
        this.login_config_name = loginConfigName;
        this.login_config_file = loginConfigFile;
        this.password_file = passwordFile;
        this.access_file = accessFile;
        this.authorizer = authorizer;
    }

    @Override
    public String toString()
    {
        // we are not including encryption options on purpose
        // as that contains credentials etc.
        String jmxOptionsString;
        if (jmx_encryption_options == null)
            jmxOptionsString = "unspecified";
        else
            jmxOptionsString = jmx_encryption_options.enabled ? "enabled" : "disabled";

        return "JMXServerOptions{" +
               "enabled=" + enabled +
               ", remote=" + remote +
               ", jmx_port=" + jmx_port +
               ", rmi_port=" + rmi_port +
               ", authenticate=" + authenticate +
               ", jmx_encryption_options=" + jmx_encryption_options +
               ", login_config_name='" + login_config_name + '\'' +
               ", login_config_file='" + login_config_file + '\'' +
               ", password_file='" + password_file + '\'' +
               ", access_file='" + access_file + '\'' +
               ", authorizer='" + authorizer + '\'' +
               '}';
    }

    public static boolean isEnabledBySystemProperties()
    {
        return CASSANDRA_JMX_REMOTE_PORT.isPresent() || CASSANDRA_JMX_LOCAL_PORT.isPresent();
    }

    public static JMXServerOptions createParsingSystemProperties()
    {
        int jmxPort;
        boolean remote;
        if (CASSANDRA_JMX_REMOTE_PORT.isPresent())
        {
            jmxPort = CASSANDRA_JMX_REMOTE_PORT.getInt();
            remote = true;
        }
        else
        {
            jmxPort = CASSANDRA_JMX_LOCAL_PORT.getInt(7199);
            remote = false;
        }

        boolean enabled = isEnabledBySystemProperties();

        int rmiPort = COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT.getInt();

        boolean authenticate = COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE.getBoolean();

        String loginConfigName = CASSANDRA_JMX_REMOTE_LOGIN_CONFIG.getString();
        String loginConfigFile = JAVA_SECURITY_AUTH_LOGIN_CONFIG.getString();
        String accessFile = COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE.getString();
        String passwordFile = COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE.getString();
        String authorizer = CASSANDRA_JMX_AUTHORIZER.getString();

        // encryption options

        String keystore = JAVAX_NET_SSL_KEYSTORE.getString();
        String keystorePassword = JAVAX_NET_SSL_KEYSTOREPASSWORD.getString();
        String truststore = JAVAX_NET_SSL_TRUSTSTORE.getString();
        String truststorePassword = JAVAX_NET_SSL_TRUSTSTOREPASSWORD.getString();

        String rawCipherSuites = COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES.getString();
        List<String> cipherSuites = null;
        if (rawCipherSuites != null)
            cipherSuites = List.of(StringUtils.split(rawCipherSuites, ","));

        String rawSslProtocols = COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS.getString();
        List<String> acceptedProtocols = null;
        if (rawSslProtocols != null)
            acceptedProtocols = List.of(StringUtils.split(rawSslProtocols, ","));

        String requireClientAuth = COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH.getString("false");

        boolean sslEnabled = COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean();

        EncryptionOptions encryptionOptions = new EncryptionOptions(new ParameterizedClass("org.apache.cassandra.security.DefaultSslContextFactory", new HashMap<>()),
                                                                    keystore,
                                                                    keystorePassword,
                                                                    truststore,
                                                                    truststorePassword,
                                                                    cipherSuites,
                                                                    null, // protocol
                                                                    acceptedProtocols,
                                                                    null, // algorithm
                                                                    null, // store_type
                                                                    requireClientAuth,
                                                                    false, // require endpoint verification
                                                                    sslEnabled,
                                                                    false, // optional
                                                                    null, // max_certificate_validity_period
                                                                    null); // certificate_validity_warn_threshold

        return new JMXServerOptions(enabled, remote, jmxPort, rmiPort, authenticate,
                                    encryptionOptions, loginConfigName, loginConfigFile, passwordFile, accessFile,
                                    authorizer);
    }


    /**
     * Sets the following JMX system properties.
     * <pre>
     *     com.sun.management.jmxremote.ssl=true
     *     javax.rmi.ssl.client.enabledCipherSuites=&lt;applicable cipher suites provided in the configuration&gt;
     *     javax.rmi.ssl.client.enabledProtocols=&lt;applicable protocols provided in the configuration&gt;
     * </pre>
     *
     * @param acceptedProtocols for the SSL communication
     * @param cipherSuites      for the SSL communication
     */
    public static void setJmxSystemProperties(List<String> acceptedProtocols, List<String> cipherSuites)
    {
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL.setBoolean(true);
        if (acceptedProtocols != null)
            JAVAX_RMI_SSL_CLIENT_ENABLED_PROTOCOLS.setString(StringUtils.join(acceptedProtocols, ","));

        if (cipherSuites != null)
            JAVAX_RMI_SSL_CLIENT_ENABLED_CIPHER_SUITES.setString(StringUtils.join(cipherSuites, ","));
    }
}
