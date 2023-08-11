package org.apache.cassandra.stress.settings;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY;

public class SettingsTransport implements Serializable
{
    private final TOptions options;
    private final SettingsCredentials credentials;

    public SettingsTransport(TOptions options, SettingsCredentials credentials)
    {
        this.options = options;
        this.credentials = credentials;
    }

    public EncryptionOptions getEncryptionOptions()
    {
        EncryptionOptions encOptions = new EncryptionOptions().applyConfig();
        if (options.trustStore.present())
        {
            encOptions = encOptions
                         .withEnabled(true)
                         .withTrustStore(options.trustStore.value())
                         .withTrustStorePassword(options.trustStorePw.setByUser() ? options.trustStorePw.value() : credentials.transportTruststorePassword)
                         .withAlgorithm(options.alg.value())
                         .withProtocol(options.protocol.value())
                         .withCipherSuites(options.ciphers.value().split(","));
            if (options.keyStore.present())
            {
                encOptions = encOptions
                             .withKeyStore(options.keyStore.value())
                             .withKeyStorePassword(options.keyStorePw.setByUser() ? options.keyStorePw.value() : credentials.transportKeystorePassword);
            }
            else
            {
                // mandatory for SSLFactory.createSSLContext(), see CASSANDRA-9325
                encOptions = encOptions
                             .withKeyStore(encOptions.truststore)
                             .withKeyStorePassword(encOptions.truststore_password != null ? encOptions.truststore_password : credentials.transportTruststorePassword);
            }
        }
        return encOptions;
    }

    // Option Declarations

    static class TOptions extends GroupedOptions implements Serializable
    {
        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null,
                                                           format("SSL: truststore password, when specified, it will override the value in credentials file of key '%s'",
                                                                  TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY), false);
        final OptionSimple keyStore = new OptionSimple("keystore=", ".*", null, "SSL: full path to keystore", false);
        final OptionSimple keyStorePw = new OptionSimple("keystore-password=", ".*", null,
                                                         format("SSL: keystore password, when specified, it will override the value in credentials file for key '%s'",
                                                                TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY), false);
        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connection protocol to use", false);
        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", null, "SSL: algorithm", false);
        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*",
                                                      "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA",
                                                      "SSL: comma delimited list of encryption suites to use", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(trustStore, trustStorePw, keyStore, keyStorePw, protocol, alg, ciphers);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        String tPassword = options.trustStorePw.setByUser() ? options.trustStorePw.value() : credentials.transportTruststorePassword;
        tPassword = tPassword != null ? "*suppressed*" : tPassword;

        String kPassword = options.keyStorePw.setByUser() ? options.keyStore.value() : credentials.transportKeystorePassword;
        kPassword = kPassword != null ? "*suppressed*" : kPassword;

        out.printf("  Truststore: %s%n", options.trustStore.value());
        out.printf("  Truststore Password: %s%n", tPassword);
        out.printf("  Keystore: %s%n", options.keyStore.value());
        out.printf("  Keystore Password: %s%n", kPassword);
        out.printf("  SSL Protocol: %s%n", options.protocol.value());
        out.printf("  SSL Algorithm: %s%n", options.alg.value());
        out.printf("  SSL Ciphers: %s%n", options.ciphers.value());
    }

    public static SettingsTransport get(Map<String, String[]> clArgs, SettingsCredentials credentials)
    {
        String[] params = clArgs.remove("-transport");
        if (params == null)
            return new SettingsTransport(new TOptions(), credentials);

        GroupedOptions options = GroupedOptions.select(params, new TOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -transport options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTransport((TOptions) options, credentials);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-transport", new TOptions());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }

}
