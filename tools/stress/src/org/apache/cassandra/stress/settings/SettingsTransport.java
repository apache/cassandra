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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.thrift.ITransportFactory;
import org.apache.cassandra.thrift.SSLTransportFactory;
import org.apache.cassandra.thrift.TFramedTransportFactory;

public class SettingsTransport implements Serializable
{

    private final String fqFactoryClass;
    private final TOptions options;
    private ITransportFactory factory;

    public SettingsTransport(TOptions options)
    {
        this.options = options;
        this.fqFactoryClass = options.factory.value();
        try
        {
            Class<?> clazz = Class.forName(fqFactoryClass);
            if (!ITransportFactory.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException(clazz + " is not a valid transport factory");
            // check we can instantiate it
            clazz.newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Invalid transport factory class: " + options.factory.value(), e);
        }
    }

    private void configureTransportFactory(ITransportFactory transportFactory, TOptions options)
    {
        Map<String, String> factoryOptions = new HashMap<>();
        // If the supplied factory supports the same set of options as our SSL impl, set those
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.TRUSTSTORE))
            factoryOptions.put(SSLTransportFactory.TRUSTSTORE, options.trustStore.value());
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.TRUSTSTORE_PASSWORD))
            factoryOptions.put(SSLTransportFactory.TRUSTSTORE_PASSWORD, options.trustStorePw.value());
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.KEYSTORE))
            factoryOptions.put(SSLTransportFactory.KEYSTORE, options.keyStore.value());
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.KEYSTORE_PASSWORD))
            factoryOptions.put(SSLTransportFactory.KEYSTORE_PASSWORD, options.keyStorePw.value());
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.PROTOCOL))
            factoryOptions.put(SSLTransportFactory.PROTOCOL, options.protocol.value());
        if (transportFactory.supportedOptions().contains(SSLTransportFactory.CIPHER_SUITES))
            factoryOptions.put(SSLTransportFactory.CIPHER_SUITES, options.ciphers.value());
        // Now check if any of the factory's supported options are set as system properties
        for (String optionKey : transportFactory.supportedOptions())
            if (System.getProperty(optionKey) != null)
                factoryOptions.put(optionKey, System.getProperty(optionKey));

        transportFactory.setOptions(factoryOptions);
    }

    public synchronized ITransportFactory getFactory()
    {
        if (factory == null)
        {
            try
            {
                this.factory = (ITransportFactory) Class.forName(fqFactoryClass).newInstance();
                configureTransportFactory(this.factory, this.options);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        return factory;
    }

    public EncryptionOptions.ClientEncryptionOptions getEncryptionOptions()
    {
        EncryptionOptions.ClientEncryptionOptions encOptions = new EncryptionOptions.ClientEncryptionOptions();
        if (options.trustStore.present())
        {
            encOptions.enabled = true;
            encOptions.truststore = options.trustStore.value();
            encOptions.truststore_password = options.trustStorePw.value();
            if (options.keyStore.present())
            {
                encOptions.keystore = options.keyStore.value();
                encOptions.keystore_password = options.keyStorePw.value();
            }
            else
            {
                // mandatory for SSLFactory.createSSLContext(), see CASSANDRA-9325
                encOptions.keystore = encOptions.truststore;
                encOptions.keystore_password = encOptions.truststore_password;
            }
            encOptions.algorithm = options.alg.value();
            encOptions.protocol = options.protocol.value();
            encOptions.cipher_suites = options.ciphers.value().split(",");
        }
        return encOptions;
    }

    // Option Declarations

    static class TOptions extends GroupedOptions implements Serializable
    {
        final OptionSimple factory = new OptionSimple("factory=", ".*", TFramedTransportFactory.class.getName(), "Fully-qualified ITransportFactory class name for creating a connection. Note: For Thrift over SSL, use org.apache.cassandra.thrift.SSLTransportFactory.", false);
        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null, "SSL: truststore password", false);
        final OptionSimple keyStore = new OptionSimple("keystore=", ".*", null, "SSL: full path to keystore", false);
        final OptionSimple keyStorePw = new OptionSimple("keystore-password=", ".*", null, "SSL: keystore password", false);
        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connection protocol to use", false);
        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", "SunX509", "SSL: algorithm", false);
        final OptionSimple storeType = new OptionSimple("store-type=", ".*", "JKS", "SSL: keystore format", false);
        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*", "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA", "SSL: comma delimited list of encryption suites to use", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(factory, trustStore, trustStorePw, keyStore, keyStorePw, protocol, alg, storeType, ciphers);
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
