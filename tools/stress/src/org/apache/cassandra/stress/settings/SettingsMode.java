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

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ProtocolOptions;

public class SettingsMode implements Serializable
{

    public final ConnectionAPI api;
    public final ConnectionStyle style;
    public final CqlVersion cqlVersion;

    public final String username;
    public final String password;
    public final String authProviderClassname;
    public final AuthProvider authProvider;

    private final String compression;

    public SettingsMode(GroupedOptions options)
    {
        if (options instanceof Cql3Options)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3Options opts = (Cql3Options) options;
            api = opts.mode().displayPrefix.equals("native") ? ConnectionAPI.JAVA_DRIVER_NATIVE : ConnectionAPI.THRIFT;
            style = opts.useUnPrepared.setByUser() ? ConnectionStyle.CQL :  ConnectionStyle.CQL_PREPARED;
            compression = ProtocolOptions.Compression.valueOf(opts.useCompression.value().toUpperCase()).name();
            username = opts.user.value();
            password = opts.password.value();
            authProviderClassname = opts.authProvider.value();
            if (authProviderClassname != null)
            {
                try
                {
                    Class<?> clazz = Class.forName(authProviderClassname);
                    if (!AuthProvider.class.isAssignableFrom(clazz))
                        throw new IllegalArgumentException(clazz + " is not a valid auth provider");
                    // check we can instantiate it
                    if (PlainTextAuthProvider.class.equals(clazz))
                    {
                        authProvider = (AuthProvider) clazz.getConstructor(String.class, String.class)
                            .newInstance(username, password);
                    } else
                    {
                        authProvider = (AuthProvider) clazz.newInstance();
                    }
                }
                catch (Exception e)
                {
                    throw new IllegalArgumentException("Invalid auth provider class: " + opts.authProvider.value(), e);
                }
            }
            else
            {
                authProvider = null;
            }
        }
        else if (options instanceof Cql3SimpleNativeOptions)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3SimpleNativeOptions opts = (Cql3SimpleNativeOptions) options;
            api = ConnectionAPI.SIMPLE_NATIVE;
            style = opts.usePrepared.setByUser() ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
            compression = ProtocolOptions.Compression.NONE.name();
            username = null;
            password = null;
            authProvider = null;
            authProviderClassname = null;
        }
        else if (options instanceof ThriftOptions)
        {
            ThriftOptions opts = (ThriftOptions) options;
            cqlVersion = CqlVersion.NOCQL;
            api = opts.smart.setByUser() ? ConnectionAPI.THRIFT_SMART : ConnectionAPI.THRIFT;
            style = ConnectionStyle.THRIFT;
            compression = ProtocolOptions.Compression.NONE.name();
            username = opts.user.value();
            password = opts.password.value();
            authProviderClassname = null;
            authProvider = null;
        }
        else
            throw new IllegalStateException();
    }

    public ProtocolOptions.Compression compression()
    {
        return ProtocolOptions.Compression.valueOf(compression);
    }

    // Option Declarations

    private static final class Cql3NativeOptions extends Cql3Options
    {
        final OptionSimple mode = new OptionSimple("native", "", null, "", true);
        OptionSimple mode()
        {
            return mode;
        }
    }

    private static final class Cql3ThriftOptions extends Cql3Options
    {
        final OptionSimple mode = new OptionSimple("thrift", "", null, "", true);
        OptionSimple mode()
        {
            return mode;
        }
    }

    private static abstract class Cql3Options extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql3", "", null, "", true);
        final OptionSimple useUnPrepared = new OptionSimple("unprepared", "", null, "force use of unprepared statements", false);
        final OptionSimple useCompression = new OptionSimple("compression=", "none|lz4|snappy", "none", "", false);
        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);
        final OptionSimple user = new OptionSimple("user=", ".+", null, "username", false);
        final OptionSimple password = new OptionSimple("password=", ".+", null, "password", false);
        final OptionSimple authProvider = new OptionSimple("auth-provider=", ".*", null, "Fully qualified implementation of com.datastax.driver.core.AuthProvider", false);

        abstract OptionSimple mode();
        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(mode(), useUnPrepared, api, useCompression, port, user, password, authProvider);
        }
    }


    private static final class Cql3SimpleNativeOptions extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql3", "", null, "", true);
        final OptionSimple useSimpleNative = new OptionSimple("simplenative", "", null, "", true);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);
        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(useSimpleNative, usePrepared, api, port);
        }
    }

    private static final class ThriftOptions extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("thrift", "", null, "", true);
        final OptionSimple smart = new OptionSimple("smart", "", null, "", false);
        final OptionSimple user = new OptionSimple("user=", ".+", null, "username", false);
        final OptionSimple password = new OptionSimple("password=", ".+", null, "password", false);


        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(api, smart, user, password);
        }
    }

    // CLI Utility Methods

    public static SettingsMode get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-mode");
        if (params == null)
        {
            Cql3NativeOptions opts = new Cql3NativeOptions();
            opts.accept("cql3");
            opts.accept("native");
            opts.accept("prepared");
            return new SettingsMode(opts);
        }

        GroupedOptions options = GroupedOptions.select(params, new ThriftOptions(), new Cql3NativeOptions(), new Cql3SimpleNativeOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -mode options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsMode(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-mode", new ThriftOptions(), new Cql3NativeOptions(), new Cql3SimpleNativeOptions());
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
