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

public class SettingsTransport implements Serializable
{
    private final TOptions options;

    public SettingsTransport(TOptions options)
    {
        this.options = options;
    }

    public EncryptionOptions getEncryptionOptions()
    {
        EncryptionOptions encOptions = new EncryptionOptions().applyConfig();
        if (options.trustStore.present())
        {
            encOptions = encOptions
                         .withEnabled(true)
                         .withTrustStore(options.trustStore.value())
                         .withTrustStorePassword(options.trustStorePw.value())
                         .withAlgorithm(options.alg.value())
                         .withProtocol(options.protocol.value())
                         .withCipherSuites(options.ciphers.value().split(","));
            if (options.keyStore.present())
            {
                encOptions = encOptions
                             .withKeyStore(options.keyStore.value())
                             .withKeyStorePassword(options.keyStorePw.value());
            }
            else
            {
                // mandatory for SSLFactory.createSSLContext(), see CASSANDRA-9325
                encOptions = encOptions
                             .withKeyStore(encOptions.truststore)
                             .withKeyStorePassword(encOptions.truststore_password);
            }
        }
        return encOptions;
    }

    // Option Declarations

    static class TOptions extends GroupedOptions implements Serializable
    {
        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null, "SSL: truststore password", false);
        final OptionSimple keyStore = new OptionSimple("keystore=", ".*", null, "SSL: full path to keystore", false);
        final OptionSimple keyStorePw = new OptionSimple("keystore-password=", ".*", null, "SSL: keystore password", false);
        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connection protocol to use", false);
        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", null, "SSL: algorithm", false);
        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*", "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA", "SSL: comma delimited list of encryption suites to use", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(trustStore, trustStorePw, keyStore, keyStorePw, protocol, alg, ciphers);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.println("  " + options.getOptionAsString());
    }

    public static SettingsTransport get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-transport");
        if (params == null)
            return new SettingsTransport(new TOptions());

        GroupedOptions options = GroupedOptions.select(params, new TOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -transport options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsTransport((TOptions) options);
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
