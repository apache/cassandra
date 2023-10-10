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
package org.apache.cassandra.tools;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.net.HostAndPort;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND;

public class LoaderOptions
{
    private static final Logger logger = LoggerFactory.getLogger(LoaderOptions.class);

    public static final String HELP_OPTION = "help";
    public static final String VERBOSE_OPTION = "verbose";
    public static final String NOPROGRESS_OPTION = "no-progress";
    public static final String NATIVE_PORT_OPTION = "port";
    public static final String STORAGE_PORT_OPTION = "storage-port";
    /** @deprecated See CASSANDRA-17602 */
    @Deprecated(since = "5.0")
    public static final String SSL_STORAGE_PORT_OPTION = "ssl-storage-port";
    public static final String USER_OPTION = "username";
    public static final String PASSWD_OPTION = "password";
    public static final String AUTH_PROVIDER_OPTION = "auth-provider";
    public static final String INITIAL_HOST_ADDRESS_OPTION = "nodes";
    public static final String IGNORE_NODES_OPTION = "ignore";
    public static final String CONNECTIONS_PER_HOST = "connections-per-host";
    public static final String CONFIG_PATH = "conf-path";

    /**
     * Throttle defined in megabits per second. CASSANDRA-10637 introduced a builder and is the preferred way to
     * provide options instead of using these constant fields.
     * @deprecated Use {@code throttle-mib} instead
     */
    /** @deprecated See CASSANDRA-17677 */
    @Deprecated(since = "5.0")
    public static final String THROTTLE_MBITS = "throttle";
    public static final String THROTTLE_MEBIBYTES = "throttle-mib";
    /**
     * Inter-datacenter throttle defined in megabits per second. CASSANDRA-10637 introduced a builder and is the
     * preferred way to provide options instead of using these constant fields.
     * @deprecated Use {@code inter-dc-throttle-mib} instead. See CASSANDRA-17677
     */
    @Deprecated(since = "5.0")
    public static final String INTER_DC_THROTTLE_MBITS = "inter-dc-throttle";
    public static final String INTER_DC_THROTTLE_MEBIBYTES = "inter-dc-throttle-mib";
    public static final String ENTIRE_SSTABLE_THROTTLE_MEBIBYTES = "entire-sstable-throttle-mib";
    public static final String ENTIRE_SSTABLE_INTER_DC_THROTTLE_MEBIBYTES = "entire-sstable-inter-dc-throttle-mib";
    public static final String TOOL_NAME = "sstableloader";
    public static final String TARGET_KEYSPACE = "target-keyspace";
    public static final String TARGET_TABLE = "target-table";

    /* client encryption options */
    public static final String SSL_TRUSTSTORE = "truststore";
    public static final String SSL_TRUSTSTORE_PW = "truststore-password";
    public static final String SSL_KEYSTORE = "keystore";
    public static final String SSL_KEYSTORE_PW = "keystore-password";
    public static final String SSL_PROTOCOL = "ssl-protocol";
    public static final String SSL_ALGORITHM = "ssl-alg";
    public static final String SSL_STORE_TYPE = "store-type";
    public static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    public final File directory;
    public final boolean debug;
    public final boolean verbose;
    public final boolean noProgress;
    public final int nativePort;
    public final String user;
    public final String passwd;
    public final AuthProvider authProvider;
    public final long throttleBytes;
    public final long interDcThrottleBytes;
    public final int entireSSTableThrottleMebibytes;
    public final int entireSSTableInterDcThrottleMebibytes;
    public final int storagePort;
    public final int sslStoragePort;
    public final EncryptionOptions clientEncOptions;
    public final int connectionsPerHost;
    public final EncryptionOptions.ServerEncryptionOptions serverEncOptions;
    public final Set<InetSocketAddress> hosts;
    public final Set<InetAddressAndPort> ignores;
    public final String targetKeyspace;
    public final String targetTable;

    LoaderOptions(Builder builder)
    {
        directory = builder.directory;
        debug = builder.debug;
        verbose = builder.verbose;
        noProgress = builder.noProgress;
        nativePort = builder.nativePort;
        user = builder.user;
        passwd = builder.passwd;
        authProvider = builder.authProvider;
        throttleBytes = builder.throttleBytes;
        interDcThrottleBytes = builder.interDcThrottleBytes;
        entireSSTableThrottleMebibytes = builder.entireSSTableThrottleMebibytes;
        entireSSTableInterDcThrottleMebibytes = builder.entireSSTableInterDcThrottleMebibytes;
        storagePort = builder.storagePort;
        sslStoragePort = builder.sslStoragePort;
        clientEncOptions = builder.clientEncOptions;
        connectionsPerHost = builder.connectionsPerHost;
        serverEncOptions = builder.serverEncOptions;
        hosts = builder.hosts;
        ignores = builder.ignores;
        targetKeyspace = builder.targetKeyspace;
        targetTable = builder.targetTable;
    }

    static class Builder
    {
        File directory;
        boolean debug;
        boolean verbose;
        boolean noProgress;
        int nativePort = 9042;
        String user;
        String passwd;
        String authProviderName;
        AuthProvider authProvider;
        long throttleBytes = 0;
        long interDcThrottleBytes = 0;
        int entireSSTableThrottleMebibytes = 0;
        int entireSSTableInterDcThrottleMebibytes = 0;

        int storagePort;
        int sslStoragePort;
        EncryptionOptions clientEncOptions = new EncryptionOptions();
        int connectionsPerHost = 1;
        EncryptionOptions.ServerEncryptionOptions serverEncOptions = new EncryptionOptions.ServerEncryptionOptions();
        Set<InetAddress> hostsArg = new HashSet<>();
        Set<InetAddress> ignoresArg = new HashSet<>();
        Set<InetSocketAddress> hosts = new HashSet<>();
        Set<InetAddressAndPort> ignores = new HashSet<>();
        String targetKeyspace;
        String targetTable;

        Builder()
        {
            //
        }

        public LoaderOptions build()
        {
            constructAuthProvider();

            try
            {
                for (InetAddress host : hostsArg)
                {
                    hosts.add(new InetSocketAddress(host, nativePort));
                }
                for (InetAddress host : ignoresArg)
                {
                    ignores.add(InetAddressAndPort.getByNameOverrideDefaults(host.getHostAddress(), storagePort));
                }
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }

            return new LoaderOptions(this);
        }

        public Builder directory(File directory)
        {
            this.directory = directory;
            return this;
        }

        public Builder debug(boolean debug)
        {
            this.debug = debug;
            return this;
        }

        public Builder verbose(boolean verbose)
        {
            this.verbose = verbose;
            return this;
        }

        public Builder noProgress(boolean noProgress)
        {
            this.noProgress = noProgress;
            return this;
        }

        public Builder nativePort(int nativePort)
        {
            this.nativePort = nativePort;
            return this;
        }

        public Builder user(String user)
        {
            this.user = user;
            return this;
        }

        public Builder password(String passwd)
        {
            this.passwd = passwd;
            return this;
        }

        public Builder authProvider(AuthProvider authProvider)
        {
            this.authProvider = authProvider;
            return this;
        }

        public Builder throttleMebibytes(int throttleMebibytes)
        {
            this.throttleBytes = (long) MEBIBYTES_PER_SECOND.toBytesPerSecond(throttleMebibytes);
            return this;
        }

        /** @deprecated See CASSANDRA-17677 */
        @Deprecated(since = "5.0")
        public Builder throttle(int throttleMegabits)
        {
            this.throttleBytes = (long) DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(throttleMegabits).toBytesPerSecond();
            return this;
        }

        public Builder interDcThrottleMebibytes(int interDcThrottleMebibytes)
        {
            this.interDcThrottleBytes = (long) MEBIBYTES_PER_SECOND.toBytesPerSecond(interDcThrottleMebibytes);
            return this;
        }

        public Builder interDcThrottleMegabits(int interDcThrottleMegabits)
        {
            this.interDcThrottleBytes = (long) DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(interDcThrottleMegabits).toBytesPerSecond();
            return this;
        }

        /** @deprecated See CASSANDRA-17677 */
        @Deprecated(since = "5.0")
        public Builder interDcThrottle(int interDcThrottle)
        {
            return interDcThrottleMegabits(interDcThrottle);
        }

        public Builder entireSSTableThrottleMebibytes(int entireSSTableThrottleMebibytes)
        {
            this.entireSSTableThrottleMebibytes = entireSSTableThrottleMebibytes;
            return this;
        }

        /** @deprecated See CASSANDRA-17677 */
        @Deprecated(since = "5.0")
        public Builder entireSSTableThrottle(int entireSSTableThrottle)
        {
            this.entireSSTableThrottleMebibytes = entireSSTableThrottle;
            return this;
        }

        public Builder entireSSTableInterDcThrottleMebibytes(int entireSSTableInterDcThrottleMebibytes)
        {
            this.entireSSTableInterDcThrottleMebibytes = entireSSTableInterDcThrottleMebibytes;
            return this;
        }

        /** @deprecated See CASSANDRA-17677 */
        @Deprecated(since = "5.0")
        public Builder entireSSTableInterDcThrottle(int entireSSTableInterDcThrottle)
        {
            this.entireSSTableInterDcThrottleMebibytes = entireSSTableInterDcThrottle;
            return this;
        }

        public Builder storagePort(int storagePort)
        {
            this.storagePort = storagePort;
            return this;
        }

        /** @deprecated See CASSANDRA-17602 */
        @Deprecated(since = "5.0")
        public Builder sslStoragePort(int sslStoragePort)
        {
            this.sslStoragePort = storagePort;
            return this;
        }

        public Builder encOptions(EncryptionOptions encOptions)
        {
            this.clientEncOptions = encOptions;
            return this;
        }

        public Builder connectionsPerHost(int connectionsPerHost)
        {
            this.connectionsPerHost = connectionsPerHost;
            return this;
        }

        public Builder serverEncOptions(EncryptionOptions.ServerEncryptionOptions serverEncOptions)
        {
            this.serverEncOptions = serverEncOptions;
            return this;
        }

        /** @deprecated See CASSANDRA-7544 */
        @Deprecated(since = "4.0")
        public Builder hosts(Set<InetAddress> hosts)
        {
            this.hostsArg.addAll(hosts);
            return this;
        }

        public Builder hostsAndNativePort(Set<InetSocketAddress> hosts)
        {
            this.hosts.addAll(hosts);
            return this;
        }

        public Builder host(InetAddress host)
        {
            hostsArg.add(host);
            return this;
        }

        public Builder hostAndNativePort(InetSocketAddress host)
        {
            hosts.add(host);
            return this;
        }

        public Builder ignore(Set<InetAddress> ignores)
        {
            this.ignoresArg.addAll(ignores);
            return this;
        }

        public Builder ignoresAndInternalPorts(Set<InetAddressAndPort> ignores)
        {
            this.ignores.addAll(ignores);
            return this;
        }

        public Builder ignore(InetAddress ignore)
        {
            ignoresArg.add(ignore);
            return this;
        }

        public Builder ignoreAndInternalPorts(InetAddressAndPort ignore)
        {
            ignores.add(ignore);
            return this;
        }

        public Builder targetKeyspace(String keyspace)
        {
            this.targetKeyspace = keyspace;
            return this;
        }

        public Builder targetTable(String table)
        {
            this.targetKeyspace = table;
            return this;
        }

        public Builder parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length == 0)
                {
                    System.err.println("Missing sstable directory argument");
                    printUsage(options);
                    System.exit(1);
                }

                if (args.length > 1)
                {
                    System.err.println("Too many arguments");
                    printUsage(options);
                    System.exit(1);
                }

                String dirname = args[0];
                File dir = new File(dirname);

                if (!dir.exists())
                {
                    errorMsg("Unknown directory: " + dirname, options);
                }

                if (!dir.isDirectory())
                {
                    errorMsg(dirname + " is not a directory", options);
                }

                directory = dir;

                verbose = cmd.hasOption(VERBOSE_OPTION);
                noProgress = cmd.hasOption(NOPROGRESS_OPTION);

                if (cmd.hasOption(USER_OPTION))
                {
                    user = cmd.getOptionValue(USER_OPTION);
                }

                if (cmd.hasOption(PASSWD_OPTION))
                {
                    passwd = cmd.getOptionValue(PASSWD_OPTION);
                }

                if (cmd.hasOption(AUTH_PROVIDER_OPTION))
                {
                    authProviderName = cmd.getOptionValue(AUTH_PROVIDER_OPTION);
                }

                // try to load config file first, so that values can be
                // rewritten with other option values.
                // otherwise use default config.
                Config config;
                if (cmd.hasOption(CONFIG_PATH))
                {
                    File configFile = new File(cmd.getOptionValue(CONFIG_PATH));
                    if (!configFile.exists())
                    {
                        errorMsg("Config file not found", options);
                    }
                    config = new YamlConfigurationLoader().loadConfig(configFile.toPath().toUri().toURL());

                    // below 2 checks are needed in order to match the pre-CASSANDRA-15234 upper bound for those parameters which were still in megabits per second
                    if (config.stream_throughput_outbound.toMegabitsPerSecond() >= Integer.MAX_VALUE)
                    {
                        throw new ConfigurationException("stream_throughput_outbound: " + config.stream_throughput_outbound.toString() + " is too large", false);
                    }

                    if (config.inter_dc_stream_throughput_outbound.toMegabitsPerSecond() >= Integer.MAX_VALUE)
                    {
                        throw new ConfigurationException("inter_dc_stream_throughput_outbound: " + config.inter_dc_stream_throughput_outbound.toString() + " is too large", false);
                    }

                    if (config.entire_sstable_stream_throughput_outbound.toMebibytesPerSecond() >= Integer.MAX_VALUE)
                    {
                        throw new ConfigurationException("entire_sstable_stream_throughput_outbound: " + config.entire_sstable_stream_throughput_outbound.toString() + " is too large", false);
                    }

                    if (config.entire_sstable_inter_dc_stream_throughput_outbound.toMebibytesPerSecond() >= Integer.MAX_VALUE)
                    {
                        throw new ConfigurationException("entire_sstable_inter_dc_stream_throughput_outbound: " + config.entire_sstable_inter_dc_stream_throughput_outbound.toString() + " is too large", false);
                    }
                }
                else
                {
                    config = new Config();
                    // unthrottle stream by default
                    config.stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(0);
                    config.inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(0);
                    config.entire_sstable_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(0);
                    config.entire_sstable_inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(0);
                }

                if (cmd.hasOption(STORAGE_PORT_OPTION))
                    storagePort = Integer.parseInt(cmd.getOptionValue(STORAGE_PORT_OPTION));
                else
                    storagePort = config.storage_port;

                if (cmd.hasOption(IGNORE_NODES_OPTION))
                {
                    String[] nodes = cmd.getOptionValue(IGNORE_NODES_OPTION).split(",");
                    try
                    {
                        for (String node : nodes)
                        {
                            ignores.add(InetAddressAndPort.getByNameOverrideDefaults(node.trim(), storagePort));
                        }
                    } catch (UnknownHostException e)
                    {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }
                }

                if (cmd.hasOption(CONNECTIONS_PER_HOST))
                {
                    connectionsPerHost = Integer.parseInt(cmd.getOptionValue(CONNECTIONS_PER_HOST));
                }

                throttleBytes = config.stream_throughput_outbound.toBytesPerSecondAsInt();

                if (cmd.hasOption(SSL_STORAGE_PORT_OPTION))
                    logger.info("ssl storage port is deprecated and not used, all communication goes though storage port " +
                                "which is able to handle encrypted communication too.");

                // Copy the encryption options and apply the config so that argument parsing can accesss isEnabled.
                clientEncOptions = config.client_encryption_options.applyConfig();
                serverEncOptions = config.server_encryption_options;
                serverEncOptions.applyConfig();

                if (cmd.hasOption(NATIVE_PORT_OPTION))
                {
                    nativePort = Integer.parseInt(cmd.getOptionValue(NATIVE_PORT_OPTION));
                }
                else
                {
                    if (config.native_transport_port_ssl != null && (config.client_encryption_options.getEnabled() || clientEncOptions.getEnabled()))
                        nativePort = config.native_transport_port_ssl;
                    else
                        nativePort = config.native_transport_port;
                }

                if (cmd.hasOption(INITIAL_HOST_ADDRESS_OPTION))
                {
                    String[] nodes = cmd.getOptionValue(INITIAL_HOST_ADDRESS_OPTION).split(",");
                    try
                    {
                        for (String node : nodes)
                        {
                            HostAndPort hap = HostAndPort.fromString(node);
                            hosts.add(new InetSocketAddress(InetAddress.getByName(hap.getHost()), hap.getPortOrDefault(nativePort)));
                        }
                    } catch (UnknownHostException e)
                    {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }

                } else
                {
                    System.err.println("Initial hosts must be specified (-d)");
                    printUsage(options);
                    System.exit(1);
                }

                if (cmd.hasOption(THROTTLE_MBITS) && cmd.hasOption(THROTTLE_MEBIBYTES))
                {
                    errorMsg(String.format("Both '%s' and '%s' were provided. Please only provide one of the two options", THROTTLE_MBITS, THROTTLE_MEBIBYTES), options);
                }

                if (cmd.hasOption(INTER_DC_THROTTLE_MBITS) && cmd.hasOption(INTER_DC_THROTTLE_MEBIBYTES))
                {
                    errorMsg(String.format("Both '%s' and '%s' were provided. Please only provide one of the two options", INTER_DC_THROTTLE_MBITS, INTER_DC_THROTTLE_MEBIBYTES), options);
                }

                if (cmd.hasOption(THROTTLE_MBITS))
                {
                    throttle(Integer.parseInt(cmd.getOptionValue(THROTTLE_MBITS)));
                }

                if (cmd.hasOption(THROTTLE_MEBIBYTES))
                {
                    throttleMebibytes(Integer.parseInt(cmd.getOptionValue(THROTTLE_MEBIBYTES)));
                }

                if (cmd.hasOption(INTER_DC_THROTTLE_MBITS))
                {
                    interDcThrottleMegabits(Integer.parseInt(cmd.getOptionValue(INTER_DC_THROTTLE_MBITS)));
                }

                if (cmd.hasOption(INTER_DC_THROTTLE_MEBIBYTES))
                {
                    interDcThrottleMebibytes(Integer.parseInt(cmd.getOptionValue(INTER_DC_THROTTLE_MEBIBYTES)));
                }

                if (cmd.hasOption(ENTIRE_SSTABLE_THROTTLE_MEBIBYTES))
                {
                    entireSSTableThrottleMebibytes(Integer.parseInt(cmd.getOptionValue(ENTIRE_SSTABLE_THROTTLE_MEBIBYTES)));
                }

                if (cmd.hasOption(ENTIRE_SSTABLE_INTER_DC_THROTTLE_MEBIBYTES))
                {
                    entireSSTableInterDcThrottleMebibytes(Integer.parseInt(cmd.getOptionValue(ENTIRE_SSTABLE_INTER_DC_THROTTLE_MEBIBYTES)));
                }

                if (cmd.hasOption(SSL_TRUSTSTORE) || cmd.hasOption(SSL_TRUSTSTORE_PW) ||
                    cmd.hasOption(SSL_KEYSTORE) || cmd.hasOption(SSL_KEYSTORE_PW))
                {
                    clientEncOptions = clientEncOptions.withEnabled(true);
                }

                if (cmd.hasOption(SSL_TRUSTSTORE))
                {
                    clientEncOptions = clientEncOptions.withTrustStore(cmd.getOptionValue(SSL_TRUSTSTORE));
                }

                if (cmd.hasOption(SSL_TRUSTSTORE_PW))
                {
                    clientEncOptions = clientEncOptions.withTrustStorePassword(cmd.getOptionValue(SSL_TRUSTSTORE_PW));
                }

                if (cmd.hasOption(SSL_KEYSTORE))
                {
                    // if a keystore was provided, lets assume we'll need to use
                    clientEncOptions = clientEncOptions.withKeyStore(cmd.getOptionValue(SSL_KEYSTORE))
                                                       .withRequireClientAuth(true);
                }

                if (cmd.hasOption(SSL_KEYSTORE_PW))
                {
                    clientEncOptions = clientEncOptions.withKeyStorePassword(cmd.getOptionValue(SSL_KEYSTORE_PW));
                }

                if (cmd.hasOption(SSL_PROTOCOL))
                {
                    clientEncOptions = clientEncOptions.withProtocol(cmd.getOptionValue(SSL_PROTOCOL));
                }

                if (cmd.hasOption(SSL_ALGORITHM))
                {
                    clientEncOptions = clientEncOptions.withAlgorithm(cmd.getOptionValue(SSL_ALGORITHM));
                }

                if (cmd.hasOption(SSL_STORE_TYPE))
                {
                    clientEncOptions = clientEncOptions.withStoreType(cmd.getOptionValue(SSL_STORE_TYPE));
                }

                if (cmd.hasOption(SSL_CIPHER_SUITES))
                {
                    clientEncOptions = clientEncOptions.withCipherSuites(cmd.getOptionValue(SSL_CIPHER_SUITES).split(","));
                }

                if (cmd.hasOption(TARGET_KEYSPACE))
                {
                    targetKeyspace = cmd.getOptionValue(TARGET_KEYSPACE);
                    if (StringUtils.isBlank(targetKeyspace))
                        errorMsg("Empty keyspace is not supported.", options);
                }

                if (cmd.hasOption(TARGET_TABLE))
                {
                    targetTable = cmd.getOptionValue(TARGET_TABLE);
                    if (StringUtils.isBlank(targetTable))
                        errorMsg("Empty table is not supported.", options);
                }

                return this;
            }
            catch (ParseException | ConfigurationException | MalformedURLException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private void constructAuthProvider()
        {
            // Both username and password need to be provided
            if ((user != null) != (passwd != null))
                errorMsg("Username and password must both be provided", getCmdLineOptions());

            if (user != null)
            {
                // Support for 3rd party auth providers that support plain text credentials.
                // In this case the auth provider must provide a constructor of the form:
                //
                // public MyAuthProvider(String username, String password)
                if (authProviderName != null)
                {
                    try
                    {
                        Class authProviderClass = Class.forName(authProviderName);
                        Constructor constructor = authProviderClass.getConstructor(String.class, String.class);
                        authProvider = (AuthProvider)constructor.newInstance(user, passwd);
                    }
                    catch (ClassNotFoundException e)
                    {
                        errorMsg("Unknown auth provider: " + e.getMessage(), getCmdLineOptions());
                    }
                    catch (NoSuchMethodException e)
                    {
                        errorMsg("Auth provider does not support plain text credentials: " + e.getMessage(), getCmdLineOptions());
                    }
                    catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e)
                    {
                        errorMsg("Could not create auth provider with plain text credentials: " + e.getMessage(), getCmdLineOptions());
                    }
                }
                else
                {
                    // If a 3rd party auth provider wasn't provided use the driver plain text provider
                    this.authProvider = new PlainTextAuthProvider(user, passwd);
                }
            }
            // Alternate support for 3rd party auth providers that don't use plain text credentials.
            // In this case the auth provider must provide a nullary constructor of the form:
            //
            // public MyAuthProvider()
            else if (authProviderName != null)
            {
                try
                {
                    authProvider = (AuthProvider)Class.forName(authProviderName).newInstance();
                }
                catch (ClassNotFoundException | InstantiationException | IllegalAccessException e)
                {
                    errorMsg("Unknown auth provider: " + e.getMessage(), getCmdLineOptions());
                }
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private static void errorMsg(String msg, CmdLineOptions options)
    {
        System.err.println(msg);
        printUsage(options);
        System.exit(1);
    }

    private static CmdLineOptions getCmdLineOptions()
    {
        CmdLineOptions options = new CmdLineOptions();
        options.addOption("v", VERBOSE_OPTION, "verbose output");
        options.addOption("h", HELP_OPTION, "display this help message");
        options.addOption(null, NOPROGRESS_OPTION, "don't display progress");
        options.addOption("i", IGNORE_NODES_OPTION, "NODES", "don't stream to this (comma separated) list of nodes");
        options.addOption("d", INITIAL_HOST_ADDRESS_OPTION, "initial hosts", "Required. try to connect to these hosts (comma separated) initially for ring information");
        options.addOption("p",  NATIVE_PORT_OPTION, "native transport port", "port used for native connection (default 9042)");
        options.addOption("sp",  STORAGE_PORT_OPTION, "storage port", "port used for internode communication (default 7000)");
        options.addOption("ssp",  SSL_STORAGE_PORT_OPTION, "ssl storage port", "port used for TLS internode communication (default 7001), this option is deprecated, all communication goes through storage port which handles encrypted communication as well");
        options.addOption("t", THROTTLE_MBITS, "throttle", "throttle speed in Mbps (default 0 for unlimited), this option is deprecated, use \"throttle-mib\" instead");
        options.addOption(null, THROTTLE_MEBIBYTES, "throttle-mib", "throttle speed in MiB/s (default 0 for unlimited)");
        options.addOption("idct", INTER_DC_THROTTLE_MBITS, "inter-dc-throttle", "inter-datacenter throttle speed in Mbps (default 0 for unlimited), this option is deprecated, use \"inter-dc-throttle-mib\" instead");
        options.addOption(null, INTER_DC_THROTTLE_MEBIBYTES, "inter-dc-throttle-mib", "inter-datacenter throttle speed in MiB/s (default 0 for unlimited)");
        options.addOption(null, ENTIRE_SSTABLE_THROTTLE_MEBIBYTES, "entire-sstable-throttle-mib", "entire SSTable throttle speed in MiB/s (default 0 for unlimited)");
        options.addOption(null, ENTIRE_SSTABLE_INTER_DC_THROTTLE_MEBIBYTES, "entire-sstable-inter-dc-throttle-mib", "entire SSTable inter-datacenter throttle speed in MiB/s (default 0 for unlimited)");
        options.addOption("u", USER_OPTION, "username", "username for cassandra authentication");
        options.addOption("pw", PASSWD_OPTION, "password", "password for cassandra authentication");
        options.addOption("ap", AUTH_PROVIDER_OPTION, "auth provider", "custom AuthProvider class name for cassandra authentication");
        options.addOption("cph", CONNECTIONS_PER_HOST, "connectionsPerHost", "number of concurrent connections-per-host.");
        // ssl connection-related options
        options.addOption("ts", SSL_TRUSTSTORE, "TRUSTSTORE", "Client SSL: full path to truststore");
        options.addOption("tspw", SSL_TRUSTSTORE_PW, "TRUSTSTORE-PASSWORD", "Client SSL: password of the truststore");
        options.addOption("ks", SSL_KEYSTORE, "KEYSTORE", "Client SSL: full path to keystore");
        options.addOption("kspw", SSL_KEYSTORE_PW, "KEYSTORE-PASSWORD", "Client SSL: password of the keystore");
        options.addOption("prtcl", SSL_PROTOCOL, "PROTOCOL", "Client SSL: connections protocol to use (default: TLS)");
        options.addOption("alg", SSL_ALGORITHM, "ALGORITHM", "Client SSL: algorithm");
        options.addOption("st", SSL_STORE_TYPE, "STORE-TYPE", "Client SSL: type of store");
        options.addOption("ciphers", SSL_CIPHER_SUITES, "CIPHER-SUITES", "Client SSL: comma-separated list of encryption suites to use");
        options.addOption("f", CONFIG_PATH, "path to config file", "cassandra.yaml file path for streaming throughput and client/server SSL.");
        options.addOption("k", TARGET_KEYSPACE, "target keyspace name", "target keyspace name");
        options.addOption("tb", TARGET_TABLE, "target table name", "target table name");
        return options;
    }

    public static void printUsage(Options options)
    {
        String usage = String.format("%s [options] <dir_path>", TOOL_NAME);
        String header = System.lineSeparator() +
                "Bulk load the sstables found in the directory <dir_path> to the configured cluster." +
                "The parent directories of <dir_path> are used as the target keyspace/table name. " +
                "So for instance, to load an sstable named Standard1-g-1-Data.db into Keyspace1/Standard1, " +
                "you will need to have the files Standard1-g-1-Data.db and Standard1-g-1-Index.db into a directory /path/to/Keyspace1/Standard1/.";
        String footer = System.lineSeparator() +
                "You can provide cassandra.yaml file with -f command line option to set up streaming throughput, client and server encryption options. " +
                "Only stream_throughput_outbound, server_encryption_options and client_encryption_options are read from yaml. " +
                "You can override options read from cassandra.yaml with corresponding command line options.";
        new HelpFormatter().printHelp(usage, header, options, footer);
    }
}
