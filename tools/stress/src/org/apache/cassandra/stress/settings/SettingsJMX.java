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

package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_USERNAME_PROPERTY_KEY;

public class SettingsJMX implements Serializable
{
    public final String user;
    public final String password;

    public SettingsJMX(Options options, SettingsCredentials credentials)
    {
        this.user = options.user.setByUser() ? options.user.value() : credentials.jmxUsername;
        this.password = options.password.setByUser() ? options.password.value() : credentials.jmxPassword;
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple user = new OptionSimple("user=",
                                                   ".*",
                                                   null,
                                                   format("Username for JMX connection, when specified, it will override the value in credentials file for key '%s'", JMX_USERNAME_PROPERTY_KEY),
                                                   false);

        final OptionSimple password = new OptionSimple("password=",
                                                       ".*",
                                                       null,
                                                       format("Password for JMX connection, when specified, it will override the value in credentials file for key '%s'", JMX_PASSWORD_PROPERTY_KEY),
                                                       false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(user, password);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Username: %s%n", user);
        out.printf("  Password: %s%n", (password == null ? "*not set*" : "*suppressed*"));
    }

    public static SettingsJMX get(Map<String, String[]> clArgs, SettingsCredentials credentials)
    {
        String[] params = clArgs.remove("-jmx");
        if (params == null)
            return new SettingsJMX(new SettingsJMX.Options(), credentials);

        GroupedOptions options = GroupedOptions.select(params, new SettingsJMX.Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -jmx options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsJMX((SettingsJMX.Options) options, credentials);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-jmx", new SettingsJMX.Options());
    }

    public static Runnable helpPrinter()
    {
        return SettingsJMX::printHelp;
    }
}
