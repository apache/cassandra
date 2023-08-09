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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_USERNAME_PROPERTY_KEY;

public class SettingsMode implements Serializable
{

    public final ConnectionAPI api;
    public final ConnectionStyle style;
    public final ProtocolVersion protocolVersion;

    public final String username;
    public final String password;
    public final String authProviderClassname;
    public final AuthProvider authProvider;

    public final Integer maxPendingPerConnection;
    public final Integer connectionsPerHost;

    private final String compression;


    public SettingsMode(GroupedOptions options, SettingsCredentials credentials)
    {
        Cql3Options opts = (Cql3Options) options;

        if (opts.simplenative.setByUser())
        {
            protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
            api = ConnectionAPI.SIMPLE_NATIVE;
            style = opts.usePrepared.setByUser() ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
            compression = ProtocolOptions.Compression.NONE.name();
            username = null;
            password = null;
            authProvider = null;
            authProviderClassname = null;
            maxPendingPerConnection = null;
            connectionsPerHost = null;
        }
        else
        {
            protocolVersion = "NEWEST_SUPPORTED".equals(opts.protocolVersion.value())
                    ? ProtocolVersion.NEWEST_SUPPORTED
                    : ProtocolVersion.fromInt(Integer.parseInt(opts.protocolVersion.value()));
            api = ConnectionAPI.JAVA_DRIVER_NATIVE;
            style = opts.useUnPrepared.setByUser() ? ConnectionStyle.CQL : ConnectionStyle.CQL_PREPARED;
            compression = ProtocolOptions.Compression.valueOf(opts.useCompression.value().toUpperCase()).name();
            username = opts.user.setByUser() ? opts.user.value() : credentials.cqlUsername;
            password = opts.password.setByUser() ? opts.password.value() : credentials.cqlPassword;
            maxPendingPerConnection = opts.maxPendingPerConnection.value().isEmpty() ? null : Integer.valueOf(opts.maxPendingPerConnection.value());
            connectionsPerHost = opts.connectionsPerHost.value().isEmpty() ? null : Integer.valueOf(opts.connectionsPerHost.value());
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
                        authProvider = (AuthProvider) clazz.getConstructor(String.class, String.class).newInstance(username, password);
                    }
                    else
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
    }

    public ProtocolOptions.Compression compression()
    {
        return ProtocolOptions.Compression.valueOf(compression);
    }

    private static class Cql3Options extends GroupedOptions
    {
        final OptionSimple protocolVersion = new OptionSimple("protocolVersion=", "[2-5]+", "NEWEST_SUPPORTED", "CQL Protocol Version", false);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "Use prepared statements", false);
        final OptionSimple useUnPrepared = new OptionSimple("unprepared", "", null, "Use unprepared statements", false);
        final OptionSimple useCompression = new OptionSimple("compression=", "none|lz4|snappy", "none", "", false);
        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);
        final OptionSimple user = new OptionSimple("user=", ".+", null,
                                                   format("CQL user, when specified, it will override the value in credentials file for key '%s'", CQL_USERNAME_PROPERTY_KEY),
                                                   false);
        final OptionSimple password = new OptionSimple("password=", ".+", null,
                                                       format("CQL password, when specified, it will override the value in credentials file for key '%s'", CQL_PASSWORD_PROPERTY_KEY),
                                                       false);
        final OptionSimple authProvider = new OptionSimple("auth-provider=", ".*", null, "Fully qualified implementation of com.datastax.driver.core.AuthProvider", false);
        final OptionSimple maxPendingPerConnection = new OptionSimple("maxPending=", "[0-9]+", "128", "Maximum pending requests per connection", false);
        final OptionSimple connectionsPerHost = new OptionSimple("connectionsPerHost=", "[0-9]+", "8", "Number of connections per host", false);
        final OptionSimple simplenative = new OptionSimple("simplenative", "", null, "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(user, password, port, authProvider,maxPendingPerConnection,
                                 useCompression, connectionsPerHost, usePrepared, useUnPrepared,
                                 protocolVersion, simplenative);
        }
    }
    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  API: %s%n", api);
        out.printf("  Connection Style: %s%n", style);
        out.printf("  Protocol Version: %s%n", protocolVersion);
        out.printf("  Username: %s%n", username);
        out.printf("  Password: %s%n", (password == null ? password : "*suppressed*"));
        out.printf("  Auth Provide Class: %s%n", authProviderClassname);
        out.printf("  Max Pending Per Connection: %d%n", maxPendingPerConnection);
        out.printf("  Connections Per Host: %d%n", connectionsPerHost);
        out.printf("  Compression: %s%n", compression);
    }

    public static SettingsMode get(Map<String, String[]> clArgs, SettingsCredentials credentials)
    {
        String[] params = clArgs.remove("-mode");
        List<String> paramList = new ArrayList<>();
        if (params == null)
        {
            Cql3Options opts = new Cql3Options();
            opts.accept("prepared");
            return new SettingsMode(opts, credentials);
        }
        for (String item : params)
        {
            // Warn on obsolete arguments, to be removed in future release
            if (item.equals("cql3") || item.equals("native"))
            {
                System.err.println("Warning: ignoring deprecated parameter: " + item);
            }
            else
            {
                paramList.add(item);
            }
        }
        if (paramList.contains("prepared") && paramList.contains("unprepared"))
        {
            System.err.println("Warning: can't specify both prepared and unprepared, using prepared");
            paramList.remove("unprepared");
        }
        String[] updated = paramList.toArray(new String[paramList.size()]);
        GroupedOptions options = new Cql3Options();
        GroupedOptions.select(updated, options);
        return new SettingsMode(options, credentials);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-mode", new Cql3Options());
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
