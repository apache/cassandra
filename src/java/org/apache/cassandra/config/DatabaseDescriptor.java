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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.AuthConfig;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.ICIDRAuthorizer;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.auth.INetworkAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.Config.PaxosOnLinearizabilityViolation;
import org.apache.cassandra.config.Config.PaxosStatePurging;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.io.util.SpinningDiskOptimizationStrategy;
import org.apache.cassandra.io.util.SsdDiskOptimizationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.security.AbstractCryptoProvider;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.JREProvider;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.CacheService.CacheType;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOCATE_TOKENS_FOR_KEYSPACE;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS;
import static org.apache.cassandra.config.CassandraRelevantProperties.AUTO_BOOTSTRAP;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONFIG_LOADER;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_STCS_IN_L0;
import static org.apache.cassandra.config.CassandraRelevantProperties.INITIAL_TOKEN;
import static org.apache.cassandra.config.CassandraRelevantProperties.IO_NETTY_TRANSPORT_ESTIMATE_SIZE_ON_SUBMIT;
import static org.apache.cassandra.config.CassandraRelevantProperties.NATIVE_TRANSPORT_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_ARCH;
import static org.apache.cassandra.config.CassandraRelevantProperties.PARTITIONER;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_NODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_TOKEN;
import static org.apache.cassandra.config.CassandraRelevantProperties.SEARCH_CONCURRENCY_FACTOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSL_STORAGE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.STORAGE_DIR;
import static org.apache.cassandra.config.CassandraRelevantProperties.STORAGE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUN_ARCH_DATA_MODEL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_FAIL_MV_LOCKS_COUNT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_JVM_DTEST_DISABLE_SSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_STRICT_RUNTIME_CHECKS;
import static org.apache.cassandra.config.CassandraRelevantProperties.UNSAFE_SYSTEM;
import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.BYTES_PER_SECOND;
import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;
import static org.apache.cassandra.io.util.FileUtils.ONE_GIB;
import static org.apache.cassandra.io.util.FileUtils.ONE_MIB;
import static org.apache.cassandra.utils.Clock.Global.logInitializationOutcome;

public class DatabaseDescriptor
{
    static
    {
        // This static block covers most usages
        FBUtilities.preventIllegalAccessWarnings();
        IO_NETTY_TRANSPORT_ESTIMATE_SIZE_ON_SUBMIT.setBoolean(false);
    }

    private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    /**
     * Tokens are serialized in a Gossip VersionedValue String.  VV are restricted to 64KiB
     * when we send them over the wire, which works out to about 1700 tokens.
     */
    private static final int MAX_NUM_TOKENS = 1536;

    private static Config conf;

    /**
     * Request timeouts can not be less than below defined value (see CASSANDRA-9375)
     */
    static final DurationSpec.LongMillisecondsBound LOWEST_ACCEPTED_TIMEOUT = new DurationSpec.LongMillisecondsBound(10L);

    private static Supplier<IFailureDetector> newFailureDetector;
    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress rpcAddress;
    private static InetAddress broadcastRpcAddress;
    private static SeedProvider seedProvider;
    private static IInternodeAuthenticator internodeAuthenticator = new AllowAllInternodeAuthenticator();

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;
    private static String paritionerName;

    private static DiskAccessMode indexAccessMode;

    private static DiskAccessMode commitLogWriteDiskAccessMode;

    private static AbstractCryptoProvider cryptoProvider;
    private static IAuthenticator authenticator;
    private static IAuthorizer authorizer;
    private static INetworkAuthorizer networkAuthorizer;
    private static ICIDRAuthorizer cidrAuthorizer;

    // Don't initialize the role manager until applying config. The options supported by CassandraRoleManager
    // depend on the configured IAuthenticator, so defer creating it until that's been set.
    private static IRoleManager roleManager;

    private static long preparedStatementsCacheSizeInMiB;

    private static long keyCacheSizeInMiB;
    private static long paxosCacheSizeInMiB;
    private static long counterCacheSizeInMiB;
    private static long indexSummaryCapacityInMiB;

    private static String localDC;
    private static Comparator<Replica> localComparator;
    private static EncryptionContext encryptionContext;
    private static boolean hasLoggedConfig;

    private static DiskOptimizationStrategy diskOptimizationStrategy;

    private static boolean clientInitialized;
    private static boolean toolInitialized;
    private static boolean daemonInitialized;

    private static final int searchConcurrencyFactor = SEARCH_CONCURRENCY_FACTOR.getInt();
    private static DurationSpec.IntSecondsBound autoSnapshoTtl;

    private static volatile boolean disableSTCSInL0 = DISABLE_STCS_IN_L0.getBoolean();
    private static final boolean unsafeSystem = UNSAFE_SYSTEM.getBoolean();

    // turns some warnings into exceptions for testing
    private static final boolean strictRuntimeChecks = TEST_STRICT_RUNTIME_CHECKS.getBoolean();

    public static volatile boolean allowUnlimitedConcurrentValidations = ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS.getBoolean();

    /**
     * The configuration for guardrails.
     */
    private static GuardrailsOptions guardrails;
    private static StartupChecksOptions startupChecksOptions;

    private static ImmutableMap<String, SSTableFormat<?, ?>> sstableFormats;
    private static volatile SSTableFormat<?, ?> selectedSSTableFormat;

    private static Function<CommitLog, AbstractCommitLogSegmentManager> commitLogSegmentMgrProvider = c -> DatabaseDescriptor.isCDCEnabled()
                                                                                                           ? new CommitLogSegmentManagerCDC(c, DatabaseDescriptor.getCommitLogLocation())
                                                                                                           : new CommitLogSegmentManagerStandard(c, DatabaseDescriptor.getCommitLogLocation());

    public static void daemonInitialization() throws ConfigurationException
    {
        daemonInitialization(DatabaseDescriptor::loadConfig);
    }

    public static void daemonInitialization(Supplier<Config> config) throws ConfigurationException
    {
        if (toolInitialized)
            throw new AssertionError("toolInitialization() already called");
        if (clientInitialized)
            throw new AssertionError("clientInitialization() already called");

        // Some unit tests require this :(
        if (daemonInitialized)
            return;
        daemonInitialized = true;

        setConfig(config.get());
        applyAll();
        AuthConfig.applyAuth();
    }

    /**
     * Equivalent to {@link #toolInitialization(boolean) toolInitialization(true)}.
     */
    public static void toolInitialization()
    {
        toolInitialization(true);
    }

    /**
     * Initializes this class as a tool, which means that the configuration is loaded
     * using {@link #loadConfig()} and all non-daemon configuration parts will be setup.
     *
     * @param failIfDaemonOrClient if {@code true} and a call to {@link #daemonInitialization()} or
     *                             {@link #clientInitialization()} has been performed before, an
     *                             {@link AssertionError} will be thrown.
     */
    public static void toolInitialization(boolean failIfDaemonOrClient)
    {
        if (!failIfDaemonOrClient && (daemonInitialized || clientInitialized))
        {
            return;
        }
        else
        {
            if (daemonInitialized)
                throw new AssertionError("daemonInitialization() already called");
            if (clientInitialized)
                throw new AssertionError("clientInitialization() already called");
        }

        if (toolInitialized)
            return;
        toolInitialized = true;

        setConfig(loadConfig());

        applySSTableFormats();

        applySimpleConfig();

        applyPartitioner();

        applySnitch();

        applyEncryptionContext();
    }

    /**
     * Equivalent to {@link #clientInitialization(boolean) clientInitialization(true)}.
     */
    public static void clientInitialization()
    {
        clientInitialization(true);
    }

    /**
     * Equivalent to {@link #clientInitialization(boolean) clientInitialization(true, Config::new)}.
     */
    public static void clientInitialization(boolean failIfDaemonOrTool)
    {
        clientInitialization(failIfDaemonOrTool, Config::new);
    }

    /**
     * Initializes this class as a client, which means that just an empty configuration will
     * be used.
     *
     * @param failIfDaemonOrTool if {@code true} and a call to {@link #daemonInitialization()} or
     *                           {@link #toolInitialization()} has been performed before, an
     *                           {@link AssertionError} will be thrown.
     */
    public static void clientInitialization(boolean failIfDaemonOrTool, Supplier<Config> configSupplier)
    {
        if (!failIfDaemonOrTool && (daemonInitialized || toolInitialized))
        {
            return;
        }
        else
        {
            if (daemonInitialized)
                throw new AssertionError("daemonInitialization() already called");
            if (toolInitialized)
                throw new AssertionError("toolInitialization() already called");
        }

        if (clientInitialized)
            return;
        clientInitialized = true;
        setDefaultFailureDetector();
        Config.setClientMode(true);
        conf = configSupplier.get();
        diskOptimizationStrategy = new SpinningDiskOptimizationStrategy();
        applySSTableFormats();
    }

    public static boolean isClientInitialized()
    {
        return clientInitialized;
    }

    public static boolean isToolInitialized()
    {
        return toolInitialized;
    }

    public static boolean isClientOrToolInitialized()
    {
        return clientInitialized || toolInitialized;
    }

    public static boolean isDaemonInitialized()
    {
        return daemonInitialized;
    }

    public static Config getRawConfig()
    {
        return conf;
    }

    @VisibleForTesting
    public static Config loadConfig() throws ConfigurationException
    {
        if (Config.getOverrideLoadConfig() != null)
            return Config.getOverrideLoadConfig().get();

        String loaderClass = CONFIG_LOADER.getString();
        ConfigurationLoader loader = loaderClass == null
                                     ? new YamlConfigurationLoader()
                                     : FBUtilities.construct(loaderClass, "configuration loading");
        Config config = loader.loadConfig();

        if (!hasLoggedConfig)
        {
            hasLoggedConfig = true;
            Config.log(config);
        }

        return config;
    }

    private static InetAddress getNetworkInterfaceAddress(String intf, String configName, boolean preferIPv6) throws ConfigurationException
    {
        try
        {
            NetworkInterface ni = NetworkInterface.getByName(intf);
            if (ni == null)
                throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" could not be found", false);
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            if (!addrs.hasMoreElements())
                throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" was found, but had no addresses", false);

            /*
             * Try to return the first address of the preferred type, otherwise return the first address
             */
            InetAddress retval = null;
            while (addrs.hasMoreElements())
            {
                InetAddress temp = addrs.nextElement();
                if (preferIPv6 && temp instanceof Inet6Address) return temp;
                if (!preferIPv6 && temp instanceof Inet4Address) return temp;
                if (retval == null) retval = temp;
            }
            return retval;
        }
        catch (SocketException e)
        {
            throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" caused an exception", e);
        }
    }

    @VisibleForTesting
    public static void setConfig(Config config)
    {
        conf = config;
    }

    private static void applyAll() throws ConfigurationException
    {
        //InetAddressAndPort cares that applySimpleConfig runs first
        applySSTableFormats();

        applyCryptoProvider();

        applySimpleConfig();

        applyPartitioner();

        applyAddressConfig();

        applySnitch();

        applyTokensConfig();

        applySeedProvider();

        applyEncryptionContext();

        applySslContext();

        applyGuardrails();

        applyStartupChecks();
    }

    private static void applySimpleConfig()
    {
        //Doing this first before all other things in case other pieces of config want to construct
        //InetAddressAndPort and get the right defaults
        InetAddressAndPort.initializeDefaultPort(getStoragePort());

        validateUpperBoundStreamingConfig();

        if (conf.auto_snapshot_ttl != null)
        {
            try
            {
                autoSnapshoTtl = new DurationSpec.IntSecondsBound(conf.auto_snapshot_ttl);
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException("Invalid value of auto_snapshot_ttl: " + conf.auto_snapshot_ttl, false);
            }
        }

        if (conf.commitlog_sync == null)
        {
            throw new ConfigurationException("Missing required directive CommitLogSync", false);
        }

        if (conf.commitlog_sync == CommitLogSync.batch)
        {
            if (conf.commitlog_sync_period.toMilliseconds() != 0)
            {
                throw new ConfigurationException("Batch sync specified, but commitlog_sync_period found.", false);
            }
            logger.debug("Syncing log with batch mode");
        }
        else if (conf.commitlog_sync == CommitLogSync.group)
        {
            if (conf.commitlog_sync_group_window.toMilliseconds() == 0)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_group_window.", false);
            }
            else if (conf.commitlog_sync_period.toMilliseconds() != 0)
            {
                throw new ConfigurationException("Group sync specified, but commitlog_sync_period found. Only specify commitlog_sync_group_window when using group sync", false);
            }
            logger.debug("Syncing log with a group window of {}", conf.commitlog_sync_period.toString());
        }
        else
        {
            if (conf.commitlog_sync_period.toMilliseconds() == 0)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_period.", false);
            }
            logger.debug("Syncing log with a period of {}", conf.commitlog_sync_period.toString());
        }

        /* evaluate the DiskAccessMode Config directive, which also affects indexAccessMode selection */
        if (conf.disk_access_mode == DiskAccessMode.auto || conf.disk_access_mode == DiskAccessMode.mmap_index_only)
        {
            conf.disk_access_mode = DiskAccessMode.standard;
            indexAccessMode = DiskAccessMode.mmap;
        }
        else if (conf.disk_access_mode == DiskAccessMode.legacy)
        {
            conf.disk_access_mode = hasLargeAddressSpace() ? DiskAccessMode.mmap : DiskAccessMode.standard;
            indexAccessMode = conf.disk_access_mode;
        }
        else if (conf.disk_access_mode == DiskAccessMode.direct)
        {
            throw new ConfigurationException(String.format("DiskAccessMode '%s' is not supported", DiskAccessMode.direct));
        }
        else
        {
            indexAccessMode = conf.disk_access_mode;
        }
        logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);

        /* phi convict threshold for FailureDetector */
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16)
        {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16, but was " + conf.phi_convict_threshold, false);
        }

        /* Thread per pool */
        if (conf.concurrent_reads < 2)
        {
            throw new ConfigurationException("concurrent_reads must be at least 2, but was " + conf.concurrent_reads, false);
        }

        if (conf.concurrent_writes < 2 && TEST_FAIL_MV_LOCKS_COUNT.getString("").isEmpty())
        {
            throw new ConfigurationException("concurrent_writes must be at least 2, but was " + conf.concurrent_writes, false);
        }

        if (conf.concurrent_counter_writes < 2)
            throw new ConfigurationException("concurrent_counter_writes must be at least 2, but was " + conf.concurrent_counter_writes, false);

        if (conf.networking_cache_size == null)
            conf.networking_cache_size = new DataStorageSpec.IntMebibytesBound(Math.min(128, (int) (Runtime.getRuntime().maxMemory() / (16 * 1048576))));

        if (conf.file_cache_size == null)
            conf.file_cache_size = new DataStorageSpec.IntMebibytesBound(Math.min(512, (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576))));

        // round down for SSDs and round up for spinning disks
        if (conf.file_cache_round_up == null)
            conf.file_cache_round_up = conf.disk_optimization_strategy == Config.DiskOptimizationStrategy.spinning;

        if (conf.memtable_offheap_space == null)
            conf.memtable_offheap_space = new DataStorageSpec.IntMebibytesBound((int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)));
        // for the moment, we default to twice as much on-heap space as off-heap, as heap overhead is very large
        if (conf.memtable_heap_space == null)
            conf.memtable_heap_space = new DataStorageSpec.IntMebibytesBound((int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)));
        if (conf.memtable_heap_space.toMebibytes() == 0)
            throw new ConfigurationException("memtable_heap_space must be positive, but was " + conf.memtable_heap_space, false);
        logger.info("Global memtable on-heap threshold is enabled at {}", conf.memtable_heap_space);
        if (conf.memtable_offheap_space.toMebibytes() == 0)
            logger.info("Global memtable off-heap threshold is disabled, HeapAllocator will be used instead");
        else
            logger.info("Global memtable off-heap threshold is enabled at {}", conf.memtable_offheap_space);

        if (conf.repair_session_max_tree_depth != null)
        {
            logger.warn("repair_session_max_tree_depth has been deprecated and should be removed from cassandra.yaml. Use repair_session_space instead");
            if (conf.repair_session_max_tree_depth < 10)
                throw new ConfigurationException("repair_session_max_tree_depth should not be < 10, but was " + conf.repair_session_max_tree_depth);
            if (conf.repair_session_max_tree_depth > 20)
                logger.warn("repair_session_max_tree_depth of " + conf.repair_session_max_tree_depth + " > 20 could lead to excessive memory usage");
        }
        else
        {
            conf.repair_session_max_tree_depth = 20;
        }

        if (conf.repair_session_space == null)
            conf.repair_session_space = new DataStorageSpec.IntMebibytesBound(Math.max(1, (int) (Runtime.getRuntime().maxMemory() / (16 * 1048576))));

        if (conf.repair_session_space.toMebibytes() < 1)
            throw new ConfigurationException("repair_session_space must be > 0, but was " + conf.repair_session_space);
        else if (conf.repair_session_space.toMebibytes() > (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)))
            logger.warn("A repair_session_space of " + conf.repair_session_space+ " mebibytes is likely to cause heap pressure");

        checkForLowestAcceptedTimeouts(conf);

        long valueInBytes = conf.native_transport_max_frame_size.toBytes();
        if (valueInBytes < 0 || valueInBytes > Integer.MAX_VALUE-1)
        {
            throw new ConfigurationException(String.format("native_transport_max_frame_size must be positive value < %dB, but was %dB",
                                                           Integer.MAX_VALUE,
                                                           valueInBytes),
                                             false);
        }

        if (conf.column_index_size != null)
            checkValidForByteConversion(conf.column_index_size, "column_index_size");
        checkValidForByteConversion(conf.column_index_cache_size, "column_index_cache_size");
        checkValidForByteConversion(conf.batch_size_warn_threshold, "batch_size_warn_threshold");

        // if data dirs, commitlog dir, or saved caches dir are set in cassandra.yaml, use that.  Otherwise,
        // use -Dcassandra.storagedir (set in cassandra-env.sh) as the parent dir for data/, commitlog/, and saved_caches/
        if (conf.commitlog_directory == null)
        {
            conf.commitlog_directory = storagedirFor("commitlog");
        }

        initializeCommitLogDiskAccessMode();
        if (commitLogWriteDiskAccessMode != conf.commitlog_disk_access_mode)
            logger.info("commitlog_disk_access_mode resolved to: {}", commitLogWriteDiskAccessMode);

        if (conf.hints_directory == null)
        {
            conf.hints_directory = storagedirFor("hints");
        }

        if (conf.native_transport_max_request_data_in_flight == null)
        {
            conf.native_transport_max_request_data_in_flight = new DataStorageSpec.LongBytesBound(Runtime.getRuntime().maxMemory() / 10);
        }

        if (conf.native_transport_max_request_data_in_flight_per_ip == null)
        {
            conf.native_transport_max_request_data_in_flight_per_ip = new DataStorageSpec.LongBytesBound(Runtime.getRuntime().maxMemory() / 40);
        }

        if (conf.native_transport_rate_limiting_enabled)
            logger.info("Native transport rate-limiting enabled at {} requests/second.", conf.native_transport_max_requests_per_second);
        else
            logger.info("Native transport rate-limiting disabled.");

        if (conf.commitlog_total_space == null)
        {
            final int preferredSizeInMiB = 8192;
            // use 1/4 of available space.  See discussion on #10013 and #10199
            final long totalSpaceInBytes = tryGetSpace(conf.commitlog_directory, FileStore::getTotalSpace);
            int defaultSpaceInMiB = calculateDefaultSpaceInMiB("commitlog",
                                                               conf.commitlog_directory,
                                                               "commitlog_total_space",
                                                               preferredSizeInMiB,
                                                               totalSpaceInBytes, 1, 4);
            conf.commitlog_total_space = new DataStorageSpec.IntMebibytesBound(defaultSpaceInMiB);
        }

        if (conf.cdc_enabled)
        {
            if (conf.cdc_raw_directory == null)
            {
                conf.cdc_raw_directory = storagedirFor("cdc_raw");
            }

            if (conf.cdc_total_space.toMebibytes() == 0)
            {
                final int preferredSizeInMiB = 4096;
                // use 1/8th of available space.  See discussion on #10013 and #10199 on the CL, taking half that for CDC
                final long totalSpaceInBytes = tryGetSpace(conf.cdc_raw_directory, FileStore::getTotalSpace);
                int defaultSpaceInMiB = calculateDefaultSpaceInMiB("cdc",
                                                                   conf.cdc_raw_directory,
                                                                   "cdc_total_space",
                                                                   preferredSizeInMiB,
                                                                   totalSpaceInBytes, 1, 8);
                conf.cdc_total_space = new DataStorageSpec.IntMebibytesBound(defaultSpaceInMiB);
            }

            logger.info("cdc_enabled is true. Starting casssandra node with Change-Data-Capture enabled.");
        }

        if (conf.saved_caches_directory == null)
        {
            conf.saved_caches_directory = storagedirFor("saved_caches");
        }
        if (conf.data_file_directories == null || conf.data_file_directories.length == 0)
        {
            conf.data_file_directories = new String[]{ storagedir("data_file_directories") + File.pathSeparator() + "data" };
        }

        long dataFreeBytes = 0;
        /* data file and commit log directories. they get created later, when they're needed. */
        for (String datadir : conf.data_file_directories)
        {
            if (datadir == null)
                throw new ConfigurationException("data_file_directories must not contain empty entry", false);
            if (datadir.equals(conf.local_system_data_file_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.commitlog_directory))
                throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.hints_directory))
                throw new ConfigurationException("hints_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.saved_caches_directory))
                throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories", false);

            dataFreeBytes = saturatedSum(dataFreeBytes, tryGetSpace(datadir, FileStore::getUnallocatedSpace));
        }
        if (dataFreeBytes < 64 * ONE_GIB) // 64 GB
            logger.warn("Only {} free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots",
                        FBUtilities.prettyPrintMemory(dataFreeBytes));

        if (conf.local_system_data_file_directory != null)
        {
            if (conf.local_system_data_file_directory.equals(conf.commitlog_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the commitlog_directory", false);
            if (conf.local_system_data_file_directory.equals(conf.saved_caches_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the saved_caches_directory", false);
            if (conf.local_system_data_file_directory.equals(conf.hints_directory))
                throw new ConfigurationException("local_system_data_file_directory must not be the same as the hints_directory", false);

            long freeBytes = tryGetSpace(conf.local_system_data_file_directory, FileStore::getUnallocatedSpace);

            if (freeBytes < ONE_GIB)
                logger.warn("Only {} free in the system data volume. Consider adding more capacity or removing obsolete snapshots",
                            FBUtilities.prettyPrintMemory(freeBytes));
        }

        if (conf.commitlog_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory", false);
        if (conf.commitlog_directory.equals(conf.hints_directory))
            throw new ConfigurationException("hints_directory must not be the same as the commitlog_directory", false);
        if (conf.hints_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the hints_directory", false);

        if (conf.memtable_flush_writers == 0)
        {
            conf.memtable_flush_writers = conf.data_file_directories.length == 1 ? 2 : 1;
        }

        if (conf.memtable_flush_writers < 1)
            throw new ConfigurationException("memtable_flush_writers must be at least 1, but was " + conf.memtable_flush_writers, false);

        if (conf.memtable_cleanup_threshold == null)
        {
            conf.memtable_cleanup_threshold = (float) (1.0 / (1 + conf.memtable_flush_writers));
        }
        else
        {
            logger.warn("memtable_cleanup_threshold has been deprecated and should be removed from cassandra.yaml");
        }

        if (conf.memtable_cleanup_threshold < 0.01f)
            throw new ConfigurationException("memtable_cleanup_threshold must be >= 0.01, but was " + conf.memtable_cleanup_threshold, false);
        if (conf.memtable_cleanup_threshold > 0.99f)
            throw new ConfigurationException("memtable_cleanup_threshold must be <= 0.99, but was " + conf.memtable_cleanup_threshold, false);
        if (conf.memtable_cleanup_threshold < 0.1f)
            logger.warn("memtable_cleanup_threshold is set very low [{}], which may cause performance degradation", conf.memtable_cleanup_threshold);

        if (conf.concurrent_compactors == null)
            conf.concurrent_compactors = Math.min(8, Math.max(2, Math.min(FBUtilities.getAvailableProcessors(), conf.data_file_directories.length)));

        if (conf.concurrent_compactors <= 0)
            throw new ConfigurationException("concurrent_compactors should be strictly greater than 0, but was " + conf.concurrent_compactors, false);

        applyConcurrentValidations(conf);
        applyRepairCommandPoolSize(conf);
        applyReadThresholdsValidations(conf);

        if (conf.concurrent_materialized_view_builders <= 0)
            throw new ConfigurationException("concurrent_materialized_view_builders should be strictly greater than 0, but was " + conf.concurrent_materialized_view_builders, false);

        if (conf.num_tokens != null && conf.num_tokens > MAX_NUM_TOKENS)
            throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported", MAX_NUM_TOKENS), false);

        try
        {
            // if prepared_statements_cache_size option was set to "auto" then size of the cache should be "max(1/256 of Heap (in MiB), 10MiB)"
            preparedStatementsCacheSizeInMiB = (conf.prepared_statements_cache_size == null)
                                              ? Math.max(10, (int) (Runtime.getRuntime().maxMemory() / 1024 / 1024 / 256))
                                              : conf.prepared_statements_cache_size.toMebibytes();

            if (preparedStatementsCacheSizeInMiB == 0)
                throw new NumberFormatException(); // to escape duplicating error message

            // we need this assignment for the Settings virtual table - CASSANDRA-17734
            conf.prepared_statements_cache_size = new DataStorageSpec.LongMebibytesBound(preparedStatementsCacheSizeInMiB);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("prepared_statements_cache_size option was set incorrectly to '"
                                             + (conf.prepared_statements_cache_size != null ? conf.prepared_statements_cache_size.toString() : null) + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if key_cache_size option was set to "auto" then size of the cache should be "min(5% of Heap (in MiB), 100MiB)
            keyCacheSizeInMiB = (conf.key_cache_size == null)
                               ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024)), 100)
                               : conf.key_cache_size.toMebibytes();

            if (keyCacheSizeInMiB < 0)
                throw new NumberFormatException(); // to escape duplicating error message

            // we need this assignment for the Settings Virtual Table - CASSANDRA-17734
            conf.key_cache_size = new DataStorageSpec.LongMebibytesBound(keyCacheSizeInMiB);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("key_cache_size option was set incorrectly to '"
                                             + (conf.key_cache_size != null ? conf.key_cache_size.toString() : null) + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if counter_cache_size option was set to "auto" then size of the cache should be "min(2.5% of Heap (in MiB), 50MiB)
            counterCacheSizeInMiB = (conf.counter_cache_size == null)
                                   ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.025 / 1024 / 1024)), 50)
                                   : conf.counter_cache_size.toMebibytes();

            if (counterCacheSizeInMiB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("counter_cache_size option was set incorrectly to '"
                                             + (conf.counter_cache_size !=null ?conf.counter_cache_size.toString() : null) + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if paxosCacheSizeInMiB option was set to "auto" then size of the cache should be "min(1% of Heap (in MB), 50MB)
            paxosCacheSizeInMiB = (conf.paxos_cache_size == null)
                    ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.01 / 1024 / 1024)), 50)
                    : conf.paxos_cache_size.toMebibytes();

            if (paxosCacheSizeInMiB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("paxos_cache_size option was set incorrectly to '"
                    + conf.paxos_cache_size + "', supported values are <integer> >= 0.", false);
        }

        // we need this assignment for the Settings virtual table - CASSANDRA-17735
        conf.counter_cache_size = new DataStorageSpec.LongMebibytesBound(counterCacheSizeInMiB);

        // if set to empty/"auto" then use 5% of Heap size
        indexSummaryCapacityInMiB = (conf.index_summary_capacity == null)
                                   ? Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024))
                                   : conf.index_summary_capacity.toMebibytes();

        if (indexSummaryCapacityInMiB < 0)
            throw new ConfigurationException("index_summary_capacity option was set incorrectly to '"
                                             + conf.index_summary_capacity.toString() + "', it should be a non-negative integer.", false);

        // we need this assignment for the Settings virtual table - CASSANDRA-17735
        conf.index_summary_capacity = new DataStorageSpec.LongMebibytesBound(indexSummaryCapacityInMiB);

        if (conf.user_defined_functions_fail_timeout.toMilliseconds() < conf.user_defined_functions_warn_timeout.toMilliseconds())
            throw new ConfigurationException("user_defined_functions_warn_timeout must less than user_defined_function_fail_timeout", false);

        if (!conf.allow_insecure_udfs && !conf.user_defined_functions_threads_enabled)
            throw new ConfigurationException("To be able to set enable_user_defined_functions_threads: false you need to set allow_insecure_udfs: true - this is an unsafe configuration and is not recommended.");

        if (conf.allow_extra_insecure_udfs)
            logger.warn("Allowing java.lang.System.* access in UDFs is dangerous and not recommended. Set allow_extra_insecure_udfs: false to disable.");

        if(conf.scripted_user_defined_functions_enabled)
            throw new ConfigurationException("JavaScript user-defined functions were removed in CASSANDRA-18252. " +
                                             "Hooks are planned to be introduced as part of CASSANDRA-17280");

        if (conf.commitlog_segment_size.toMebibytes() == 0)
            throw new ConfigurationException("commitlog_segment_size must be positive, but was "
                                             + conf.commitlog_segment_size.toString(), false);
        else if (conf.commitlog_segment_size.toMebibytes() >= 2048)
            throw new ConfigurationException("commitlog_segment_size must be smaller than 2048, but was "
                                             + conf.commitlog_segment_size.toString(), false);

        if (conf.max_mutation_size == null)
            conf.max_mutation_size = new DataStorageSpec.IntKibibytesBound(conf.commitlog_segment_size.toKibibytes() / 2);
        else if (conf.commitlog_segment_size.toKibibytes() < 2 * conf.max_mutation_size.toKibibytes())
            throw new ConfigurationException("commitlog_segment_size must be at least twice the size of max_mutation_size / 1024", false);

        // native transport encryption options
        if (conf.client_encryption_options != null)
        {
            conf.client_encryption_options.applyConfig();

            if (conf.native_transport_port_ssl != null
                && conf.native_transport_port_ssl != conf.native_transport_port
                && conf.client_encryption_options.tlsEncryptionPolicy() == EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
            {
                throw new ConfigurationException("Encryption must be enabled in client_encryption_options for native_transport_port_ssl", false);
            }
        }

        if (conf.snapshot_links_per_second < 0)
            throw new ConfigurationException("snapshot_links_per_second must be >= 0");

        if (conf.max_value_size.toMebibytes() == 0)
            throw new ConfigurationException("max_value_size must be positive", false);
        else if (conf.max_value_size.toMebibytes() >= 2048)
            throw new ConfigurationException("max_value_size must be smaller than 2048, but was "
                    + conf.max_value_size.toString(), false);

        switch (conf.disk_optimization_strategy)
        {
            case ssd:
                diskOptimizationStrategy = new SsdDiskOptimizationStrategy(conf.disk_optimization_page_cross_chance);
                break;
            case spinning:
                diskOptimizationStrategy = new SpinningDiskOptimizationStrategy();
                break;
        }

        if (conf.server_encryption_options != null)
        {
            conf.server_encryption_options.applyConfig();

            if (conf.server_encryption_options.legacy_ssl_storage_port_enabled &&
                conf.server_encryption_options.tlsEncryptionPolicy() == EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
            {
                throw new ConfigurationException("legacy_ssl_storage_port_enabled is true (enabled) with internode encryption disabled (none). Enable encryption or disable the legacy ssl storage port.");
            }
        }

        if (conf.internode_max_message_size != null)
        {
            long maxMessageSize = conf.internode_max_message_size.toBytes();

            if (maxMessageSize > conf.internode_application_receive_queue_reserve_endpoint_capacity.toBytes())
                throw new ConfigurationException("internode_max_message_size must no exceed internode_application_receive_queue_reserve_endpoint_capacity", false);

            if (maxMessageSize > conf.internode_application_receive_queue_reserve_global_capacity.toBytes())
                throw new ConfigurationException("internode_max_message_size must no exceed internode_application_receive_queue_reserve_global_capacity", false);

            if (maxMessageSize > conf.internode_application_send_queue_reserve_endpoint_capacity.toBytes())
                throw new ConfigurationException("internode_max_message_size must no exceed internode_application_send_queue_reserve_endpoint_capacity", false);

            if (maxMessageSize > conf.internode_application_send_queue_reserve_global_capacity.toBytes())
                throw new ConfigurationException("internode_max_message_size must no exceed internode_application_send_queue_reserve_global_capacity", false);
        }
        else
        {
            long maxMessageSizeInBytes =
            Math.min(conf.internode_application_receive_queue_reserve_endpoint_capacity.toBytes(),
                     conf.internode_application_send_queue_reserve_endpoint_capacity.toBytes());

            conf.internode_max_message_size = new DataStorageSpec.IntBytesBound(maxMessageSizeInBytes);
        }

        validateMaxConcurrentAutoUpgradeTasksConf(conf.max_concurrent_automatic_sstable_upgrades);

        if (conf.default_keyspace_rf < conf.minimum_replication_factor_fail_threshold)
        {
            throw new ConfigurationException(String.format("default_keyspace_rf (%d) cannot be less than minimum_replication_factor_fail_threshold (%d)",
                                                           conf.default_keyspace_rf, conf.minimum_replication_factor_fail_threshold));
        }

        if (conf.paxos_repair_parallelism <= 0)
            conf.paxos_repair_parallelism = Math.max(1, conf.concurrent_writes / 8);

        Paxos.setPaxosVariant(conf.paxos_variant);
        if (conf.paxos_state_purging == null)
            conf.paxos_state_purging = PaxosStatePurging.legacy;

        logInitializationOutcome(logger);

        if (conf.max_space_usable_for_compactions_in_percentage < 0 || conf.max_space_usable_for_compactions_in_percentage > 1)
            throw new ConfigurationException("max_space_usable_for_compactions_in_percentage must be between 0 and 1", false);

        if (conf.dump_heap_on_uncaught_exception && DatabaseDescriptor.getHeapDumpPath() == null)
            throw new ConfigurationException(String.format("Invalid configuration. Heap dump is enabled but cannot create heap dump output path: %s.", conf.heap_dump_path != null ? conf.heap_dump_path : "null"));

        conf.sai_options.validate();
    }

    @VisibleForTesting
    static void validateUpperBoundStreamingConfig() throws ConfigurationException
    {
        // below 2 checks are needed in order to match the pre-CASSANDRA-15234 upper bound for those parameters which were still in megabits per second
        if (conf.stream_throughput_outbound.toMegabitsPerSecond() >= Integer.MAX_VALUE)
        {
            throw new ConfigurationException("Invalid value of stream_throughput_outbound: " + conf.stream_throughput_outbound.toString(), false);
        }

        if (conf.inter_dc_stream_throughput_outbound.toMegabitsPerSecond() >= Integer.MAX_VALUE)
        {
            throw new ConfigurationException("Invalid value of inter_dc_stream_throughput_outbound: " + conf.inter_dc_stream_throughput_outbound.toString(), false);
        }

        if (conf.entire_sstable_stream_throughput_outbound.toMebibytesPerSecond() >= Integer.MAX_VALUE)
        {
            throw new ConfigurationException("Invalid value of entire_sstable_stream_throughput_outbound: " + conf.entire_sstable_stream_throughput_outbound.toString(), false);
        }

        if (conf.entire_sstable_inter_dc_stream_throughput_outbound.toMebibytesPerSecond() >= Integer.MAX_VALUE)
        {
            throw new ConfigurationException("Invalid value of entire_sstable_inter_dc_stream_throughput_outbound: " + conf.entire_sstable_inter_dc_stream_throughput_outbound.toString(), false);
        }

        if (conf.compaction_throughput.toMebibytesPerSecond() >= Integer.MAX_VALUE)
        {
            throw new ConfigurationException("Invalid value of compaction_throughput: " + conf.compaction_throughput.toString(), false);
        }
    }

    @VisibleForTesting
    static void applyConcurrentValidations(Config config)
    {
        if (config.concurrent_validations < 1)
        {
            config.concurrent_validations = config.concurrent_compactors;
        }
        else if (config.concurrent_validations > config.concurrent_compactors && !allowUnlimitedConcurrentValidations)
        {
            throw new ConfigurationException("To set concurrent_validations > concurrent_compactors, " +
                                             "set the system property -D" + ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS.getKey() + "=true");
        }
    }

    @VisibleForTesting
    static void applyRepairCommandPoolSize(Config config)
    {
        if (config.repair_command_pool_size < 1)
            config.repair_command_pool_size = config.concurrent_validations;
    }

    @VisibleForTesting
    static void applyReadThresholdsValidations(Config config)
    {
        validateReadThresholds("coordinator_read_size", config.coordinator_read_size_warn_threshold, config.coordinator_read_size_fail_threshold);
        validateReadThresholds("local_read_size", config.local_read_size_warn_threshold, config.local_read_size_fail_threshold);
        validateReadThresholds("row_index_read_size", config.row_index_read_size_warn_threshold, config.row_index_read_size_fail_threshold);
    }

    private static void validateReadThresholds(String name, DataStorageSpec.LongBytesBound warn, DataStorageSpec.LongBytesBound fail)
    {
        if (fail != null && warn != null && fail.toBytes() < warn.toBytes())
            throw new ConfigurationException(String.format("%s (%s) must be greater than or equal to %s (%s)",
                                                           name + "_fail_threshold", fail,
                                                           name + "_warn_threshold", warn));
    }

    public static GuardrailsOptions getGuardrailsConfig()
    {
        return guardrails;
    }

    private static void applyGuardrails()
    {
        try
        {
            guardrails = new GuardrailsOptions(conf);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Invalid guardrails configuration: " + e.getMessage(), e);
        }
    }

    public static StartupChecksOptions getStartupChecksOptions()
    {
        return startupChecksOptions;
    }

    private static void applyStartupChecks()
    {
        startupChecksOptions = new StartupChecksOptions(conf.startup_checks);
    }

    private static String storagedirFor(String type)
    {
        return storagedir(type + "_directory") + File.pathSeparator() + type;
    }

    private static String storagedir(String errMsgType)
    {
        String storagedir = STORAGE_DIR.getString();
        if (storagedir == null)
            throw new ConfigurationException(errMsgType + " is missing and " + STORAGE_DIR.getKey() + " system property is not set", false);
        return storagedir;
    }

    static int calculateDefaultSpaceInMiB(String type, String path, String setting, int preferredSizeInMiB, long totalSpaceInBytes, long totalSpaceNumerator, long totalSpaceDenominator)
    {
        final long totalSizeInMiB = totalSpaceInBytes / ONE_MIB;
        final int minSizeInMiB = Ints.saturatedCast(totalSpaceNumerator * totalSizeInMiB / totalSpaceDenominator);

        if (minSizeInMiB < preferredSizeInMiB)
        {
            logger.warn("Small {} volume detected at '{}'; setting {} to {}.  You can override this in cassandra.yaml",
                        type, path, setting, minSizeInMiB);
            return minSizeInMiB;
        }
        else
        {
            return preferredSizeInMiB;
        }
    }

    public static void applyAddressConfig() throws ConfigurationException
    {
        applyAddressConfig(conf);
    }

    public static void applyAddressConfig(Config config) throws ConfigurationException
    {
        listenAddress = null;
        rpcAddress = null;
        broadcastAddress = null;
        broadcastRpcAddress = null;

        /* Local IP, hostname or interface to bind services to */
        if (config.listen_address != null && config.listen_interface != null)
        {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both", false);
        }
        else if (config.listen_address != null)
        {
            try
            {
                listenAddress = InetAddress.getByName(config.listen_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown listen_address '" + config.listen_address + '\'', false);
            }

            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException("listen_address cannot be a wildcard address (" + config.listen_address + ")!", false);
        }
        else if (config.listen_interface != null)
        {
            listenAddress = getNetworkInterfaceAddress(config.listen_interface, "listen_interface", config.listen_interface_prefer_ipv6);
        }

        /* Gossip Address to broadcast */
        if (config.broadcast_address != null)
        {
            try
            {
                broadcastAddress = InetAddress.getByName(config.broadcast_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_address '" + config.broadcast_address + '\'', false);
            }

            if (broadcastAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_address cannot be a wildcard address (" + config.broadcast_address + ")!", false);
        }

        /* Local IP, hostname or interface to bind RPC server to */
        if (config.rpc_address != null && config.rpc_interface != null)
        {
            throw new ConfigurationException("Set rpc_address OR rpc_interface, not both", false);
        }
        else if (config.rpc_address != null)
        {
            try
            {
                rpcAddress = InetAddress.getByName(config.rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown host in rpc_address " + config.rpc_address, false);
            }
        }
        else if (config.rpc_interface != null)
        {
            rpcAddress = getNetworkInterfaceAddress(config.rpc_interface, "rpc_interface", config.rpc_interface_prefer_ipv6);
        }
        else
        {
            rpcAddress = FBUtilities.getJustLocalAddress();
        }

        /* RPC address to broadcast */
        if (config.broadcast_rpc_address != null)
        {
            try
            {
                broadcastRpcAddress = InetAddress.getByName(config.broadcast_rpc_address);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + config.broadcast_rpc_address + '\'', false);
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_rpc_address cannot be a wildcard address (" + config.broadcast_rpc_address + ")!", false);
        }
        else
        {
            if (rpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("If rpc_address is set to a wildcard address (" + config.rpc_address + "), then " +
                                                 "you must set broadcast_rpc_address to a value other than " + config.rpc_address, false);
        }
    }

    public static void applyEncryptionContext()
    {
        // always attempt to load the cipher factory, as we could be in the situation where the user has disabled encryption,
        // but has existing commitlogs and sstables on disk that are still encrypted (and still need to be read)
        encryptionContext = new EncryptionContext(conf.transparent_data_encryption_options);
    }

    public static void applySslContext()
    {
        if (TEST_JVM_DTEST_DISABLE_SSL.getBoolean())
            return;

        try
        {
            SSLFactory.validateSslContext("Internode messaging", conf.server_encryption_options, true, true);
            SSLFactory.validateSslContext("Native transport", conf.client_encryption_options, conf.client_encryption_options.require_client_auth, true);
            SSLFactory.initHotReloading(conf.server_encryption_options, conf.client_encryption_options, false);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Failed to initialize SSL", e);
        }
    }

    public static void applyCryptoProvider()
    {
        if (TEST_SKIP_CRYPTO_PROVIDER_INSTALLATION.getBoolean())
            return;

        if (conf.crypto_provider == null)
            conf.crypto_provider = new ParameterizedClass(JREProvider.class.getName(), null);

        // properties beat configuration
        String classNameFromSystemProperties = CassandraRelevantProperties.CRYPTO_PROVIDER_CLASS_NAME.getString();
        if (classNameFromSystemProperties != null)
            conf.crypto_provider.class_name = classNameFromSystemProperties;

        if (conf.crypto_provider.class_name == null)
            throw new ConfigurationException("Failed to initialize crypto provider, class_name cannot be null");

        if (conf.crypto_provider.parameters == null)
            conf.crypto_provider.parameters = new HashMap<>();

        Map<String, String> cryptoProviderParameters = new HashMap<>(conf.crypto_provider.parameters);
        cryptoProviderParameters.putIfAbsent(AbstractCryptoProvider.FAIL_ON_MISSING_PROVIDER_KEY, "false");

        try
        {
            cryptoProvider = FBUtilities.newCryptoProvider(conf.crypto_provider.class_name, cryptoProviderParameters);
            cryptoProvider.install();
        }
        catch (Exception e)
        {
            if (e instanceof ConfigurationException)
                throw (ConfigurationException) e;
            else
                throw new ConfigurationException(String.format("Failed to initialize crypto provider %s", conf.crypto_provider.class_name), e);
        }
    }

    public static void applySeedProvider()
    {
        // load the seeds for node contact points
        if (conf.seed_provider == null)
        {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.", false);
        }
        try
        {
            Class<?> seedProviderClass = Class.forName(conf.seed_provider.class_name);
            seedProvider = (SeedProvider)seedProviderClass.getConstructor(Map.class).newInstance(conf.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e)
        {
            throw new ConfigurationException(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.", true);
        }
        if (seedProvider.getSeeds().size() == 0)
            throw new ConfigurationException("The seed provider lists no seeds.", false);
    }

    @VisibleForTesting
    static void checkForLowestAcceptedTimeouts(Config conf)
    {
        if(conf.read_request_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("read_request_timeout", conf.read_request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.read_request_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }

        if(conf.range_request_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("range_request_timeout", conf.range_request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.range_request_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }

        if(conf.request_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("request_timeout", conf.request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.request_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }

        if(conf.write_request_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("write_request_timeout", conf.write_request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.write_request_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }

        if(conf.cas_contention_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("cas_contention_timeout", conf.cas_contention_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.cas_contention_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }

        if(conf.counter_write_request_timeout.toMilliseconds()< LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("counter_write_request_timeout", conf.counter_write_request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.counter_write_request_timeout = new DurationSpec.LongMillisecondsBound("10ms");
        }
        if(conf.truncate_request_timeout.toMilliseconds() < LOWEST_ACCEPTED_TIMEOUT.toMilliseconds())
        {
            logInfo("truncate_request_timeout", conf.truncate_request_timeout, LOWEST_ACCEPTED_TIMEOUT);
            conf.truncate_request_timeout = LOWEST_ACCEPTED_TIMEOUT;
        }
    }

    private static void logInfo(String property, DurationSpec.LongMillisecondsBound actualValue, DurationSpec.LongMillisecondsBound lowestAcceptedValue)
    {
        logger.info("found {}::{} less than lowest acceptable value {}, continuing with {}",
                    property,
                    actualValue.toString(),
                    lowestAcceptedValue.toString(),
                    lowestAcceptedValue);
    }

    public static void applyTokensConfig()
    {
        applyTokensConfig(conf);
    }

    static void applyTokensConfig(Config conf)
    {
        if (conf.initial_token != null)
        {
            Collection<String> tokens = tokensFromString(conf.initial_token);
            if (conf.num_tokens == null)
            {
                if (tokens.size() == 1)
                    conf.num_tokens = 1;
                else
                    throw new ConfigurationException("initial_token was set but num_tokens is not!", false);
            }

            if (tokens.size() != conf.num_tokens)
            {
                throw new ConfigurationException(String.format("The number of initial tokens (by initial_token) specified (%s) is different from num_tokens value (%s)",
                                                               tokens.size(),
                                                               conf.num_tokens),
                                                 false);
            }

            for (String token : tokens)
                partitioner.getTokenFactory().validate(token);
        }
        else if (conf.num_tokens == null)
        {
            conf.num_tokens = 1;
        }
    }

    // definitely not safe for tools + clients - implicitly instantiates StorageService
    public static void applySnitch()
    {
        /* end point snitch */
        if (conf.endpoint_snitch == null)
        {
            throw new ConfigurationException("Missing endpoint_snitch directive", false);
        }
        snitch = createEndpointSnitch(conf.dynamic_snitch, conf.endpoint_snitch);
        EndpointSnitchInfo.create();

        localDC = snitch.getLocalDatacenter();
        localComparator = (replica1, replica2) -> {
            boolean local1 = localDC.equals(snitch.getDatacenter(replica1));
            boolean local2 = localDC.equals(snitch.getDatacenter(replica2));
            if (local1 && !local2)
                return -1;
            if (local2 && !local1)
                return 1;
            return 0;
        };
        newFailureDetector = () -> createFailureDetector(conf.failure_detector);
    }

    // definitely not safe for tools + clients - implicitly instantiates schema
    public static void applyPartitioner()
    {
        applyPartitioner(conf);
    }

    public static void applyPartitioner(Config conf)
    {
        /* Hashing strategy */
        if (conf.partitioner == null)
        {
            throw new ConfigurationException("Missing directive: partitioner", false);
        }
        String name = conf.partitioner;
        try
        {
            name = PARTITIONER.getString(conf.partitioner);
            partitioner = FBUtilities.newPartitioner(name);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid partitioner class " + name, e);
        }

        paritionerName = partitioner.getClass().getCanonicalName();
    }

    private static DiskAccessMode resolveCommitLogWriteDiskAccessMode(DiskAccessMode providedDiskAccessMode)
    {
        boolean compressOrEncrypt = getCommitLogCompression() != null || (getEncryptionContext() != null && getEncryptionContext().isEnabled());
        boolean directIOSupported = false;
        try
        {
            directIOSupported = FileUtils.getBlockSize(new File(getCommitLogLocation())) > 0;
        }
        catch (RuntimeException e)
        {
            logger.warn("Unable to determine block size for commit log directory: {}", e.getMessage());
        }

        if (providedDiskAccessMode == DiskAccessMode.auto)
        {
            if (compressOrEncrypt)
                providedDiskAccessMode = DiskAccessMode.legacy;
            else
            {
                providedDiskAccessMode = directIOSupported && conf.disk_optimization_strategy == Config.DiskOptimizationStrategy.ssd ? DiskAccessMode.direct
                                                                                                                                     : DiskAccessMode.legacy;
            }
        }

        if (providedDiskAccessMode == DiskAccessMode.legacy)
        {
            providedDiskAccessMode = compressOrEncrypt ? DiskAccessMode.standard : DiskAccessMode.mmap;
        }

        return providedDiskAccessMode;
    }

    private static void validateCommitLogWriteDiskAccessMode(DiskAccessMode diskAccessMode) throws ConfigurationException
    {
        boolean compressOrEncrypt = getCommitLogCompression() != null || (getEncryptionContext() != null && getEncryptionContext().isEnabled());

        if (compressOrEncrypt && diskAccessMode != DiskAccessMode.standard)
        {
            throw new ConfigurationException("commitlog_disk_access_mode = " + diskAccessMode + " is not supported with compression or encryption. Please use 'auto' when unsure.", false);
        }
        else if (!compressOrEncrypt && diskAccessMode != DiskAccessMode.mmap && diskAccessMode != DiskAccessMode.direct)
        {
            throw new ConfigurationException("commitlog_disk_access_mode = " + diskAccessMode + " is not supported. Please use 'auto' when unsure.", false);
        }
    }

    private static void validateSSTableFormatFactories(Iterable<SSTableFormat.Factory> factories)
    {
        Map<String, SSTableFormat.Factory> factoryByName = new HashMap<>();
        for (SSTableFormat.Factory factory : factories)
        {
            if (factory.name() == null)
                throw new ConfigurationException(String.format("SSTable format name in %s cannot be null", factory.getClass().getCanonicalName()));

            if (!factory.name().matches("^[a-z]+$"))
                throw new ConfigurationException(String.format("SSTable format name for %s must be non-empty, lower-case letters only string", factory.getClass().getCanonicalName()));

            SSTableFormat.Factory prev = factoryByName.put(factory.name(), factory);
            if (prev != null)
                throw new ConfigurationException(String.format("Multiple sstable format implementations with the same name %s: %s and %s", factory.name(), factory.getClass().getCanonicalName(), prev.getClass().getCanonicalName()));
        }
    }

    private static ImmutableMap<String, Supplier<SSTableFormat<?, ?>>> validateAndMatchSSTableFormatOptions(Iterable<SSTableFormat.Factory> factories, Map<String, Map<String, String>> options)
    {
        ImmutableMap.Builder<String, Supplier<SSTableFormat<?, ?>>> providersBuilder = ImmutableMap.builder();
        if (options == null)
            options = ImmutableMap.of();
        for (SSTableFormat.Factory factory : factories)
        {
            Map<String, String> formatOptions = options.getOrDefault(factory.name(), ImmutableMap.of());
            providersBuilder.put(factory.name(), () -> factory.getInstance(ImmutableMap.copyOf(formatOptions)));
        }
        ImmutableMap<String, Supplier<SSTableFormat<?, ?>>> providers = providersBuilder.build();
        if (options != null)
        {
            Sets.SetView<String> unknownFormatNames = Sets.difference(options.keySet(), providers.keySet());
            if (!unknownFormatNames.isEmpty())
                throw new ConfigurationException(String.format("Configuration contains options of unknown sstable formats: %s", unknownFormatNames));
        }
        return providers;
    }

    private static SSTableFormat<?, ?> getAndValidateWriteFormat(Map<String, SSTableFormat<?, ?>> sstableFormats, String selectedFormatName)
    {
        SSTableFormat<?, ?> selectedFormat;
        if (StringUtils.isBlank(selectedFormatName))
            selectedFormatName = BigFormat.NAME;
        selectedFormat = sstableFormats.get(selectedFormatName);
        if (selectedFormat == null)
            throw new ConfigurationException(String.format("Selected sstable format '%s' is not available.", selectedFormatName));

        getStorageCompatibilityMode().validateSstableFormat(selectedFormat);

        return selectedFormat;
    }

    private static void applySSTableFormats()
    {
        ServiceLoader<SSTableFormat.Factory> loader = ServiceLoader.load(SSTableFormat.Factory.class, DatabaseDescriptor.class.getClassLoader());
        List<SSTableFormat.Factory> factories = Iterables.toList(loader);
        if (factories.isEmpty())
            factories = ImmutableList.of(new BigFormat.BigFormatFactory());
        applySSTableFormats(factories, conf.sstable);
    }

    private static void applySSTableFormats(Iterable<SSTableFormat.Factory> factories, Config.SSTableConfig sstableFormatsConfig)
    {
        if (sstableFormats != null)
            return;

        validateSSTableFormatFactories(factories);
        ImmutableMap<String, Supplier<SSTableFormat<?, ?>>> providers = validateAndMatchSSTableFormatOptions(factories, sstableFormatsConfig.format);

        ImmutableMap.Builder<String, SSTableFormat<?, ?>> sstableFormatsBuilder = ImmutableMap.builder();
        providers.forEach((name, provider) -> {
            try
            {
                sstableFormatsBuilder.put(name, provider.get());
            }
            catch (RuntimeException | Error ex)
            {
                throw new ConfigurationException(String.format("Failed to instantiate sstable format '%s'", name), ex);
            }
        });
        sstableFormats = sstableFormatsBuilder.build();

        selectedSSTableFormat = getAndValidateWriteFormat(sstableFormats, sstableFormatsConfig.selected_format);

        sstableFormats.values().forEach(SSTableFormat::allComponents); // make sure to reach all supported components for a type so that we know all of them are registered
        logger.info("Supported sstable formats are: {}", sstableFormats.values().stream().map(f -> f.name() + " -> " + f.getClass().getName() + " with singleton components: " + f.allComponents()).collect(Collectors.joining(", ")));
    }

    /**
     * Computes the sum of the 2 specified positive values returning {@code Long.MAX_VALUE} if the sum overflow.
     *
     * @param left  the left operand
     * @param right the right operand
     * @return the sum of the 2 specified positive values of {@code Long.MAX_VALUE} if the sum overflow.
     */
    private static long saturatedSum(long left, long right)
    {
        assert left >= 0 && right >= 0;
        long sum = left + right;
        return sum < 0 ? Long.MAX_VALUE : sum;
    }

    private static long tryGetSpace(String dir, PathUtils.IOToLongFunction<FileStore> getSpace)
    {
        return PathUtils.tryGetSpace(new File(dir).toPath(), getSpace, e -> { throw new ConfigurationException("Unable check disk space in '" + dir + "'. Perhaps the Cassandra user does not have the necessary permissions"); });
    }

    public static IEndpointSnitch createEndpointSnitch(boolean dynamic, String snitchClassName) throws ConfigurationException
    {
        if (!snitchClassName.contains("."))
            snitchClassName = "org.apache.cassandra.locator." + snitchClassName;
        IEndpointSnitch snitch = FBUtilities.construct(snitchClassName, "snitch");
        return dynamic ? new DynamicEndpointSnitch(snitch) : snitch;
    }

    private static IFailureDetector createFailureDetector(String detectorClassName) throws ConfigurationException
    {
        if (!detectorClassName.contains("."))
            detectorClassName = "org.apache.cassandra.gms." + detectorClassName;
        IFailureDetector detector = FBUtilities.construct(detectorClassName, "failure detector");
        return detector;
    }

    public static AbstractCryptoProvider getCryptoProvider()
    {
        return cryptoProvider;
    }

    public static void setCryptoProvider(AbstractCryptoProvider cryptoProvider)
    {
        DatabaseDescriptor.cryptoProvider = cryptoProvider;
    }
    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static void setAuthenticator(IAuthenticator authenticator)
    {
        DatabaseDescriptor.authenticator = authenticator;
    }

    public static IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    public static void setAuthorizer(IAuthorizer authorizer)
    {
        DatabaseDescriptor.authorizer = authorizer;
    }

    public static INetworkAuthorizer getNetworkAuthorizer()
    {
        return networkAuthorizer;
    }

    public static void setNetworkAuthorizer(INetworkAuthorizer networkAuthorizer)
    {
        DatabaseDescriptor.networkAuthorizer = networkAuthorizer;
    }

    public static ICIDRAuthorizer getCIDRAuthorizer()
    {
        return cidrAuthorizer;
    }

    public static void setCIDRAuthorizer(ICIDRAuthorizer cidrAuthorizer)
    {
        DatabaseDescriptor.cidrAuthorizer = cidrAuthorizer;
    }

    public static boolean getCidrChecksForSuperusers()
    {
        boolean defaultCidrChecksForSuperusers = false;

        if (conf.cidr_authorizer == null || conf.cidr_authorizer.parameters == null)
            return defaultCidrChecksForSuperusers;

        String value = conf.cidr_authorizer.parameters.get("cidr_checks_for_superusers");
        if (value == null || value.isEmpty())
            return defaultCidrChecksForSuperusers;

        return Boolean.parseBoolean(value);
    }

    public static ICIDRAuthorizer.CIDRAuthorizerMode getCidrAuthorizerMode()
    {
        ICIDRAuthorizer.CIDRAuthorizerMode defaultCidrAuthorizerMode = ICIDRAuthorizer.CIDRAuthorizerMode.MONITOR;

        if (conf.cidr_authorizer == null || conf.cidr_authorizer.parameters == null)
            return defaultCidrAuthorizerMode;

        String cidrAuthorizerMode = conf.cidr_authorizer.parameters.get("cidr_authorizer_mode");
        if (cidrAuthorizerMode == null || cidrAuthorizerMode.isEmpty())
            return defaultCidrAuthorizerMode;

        return ICIDRAuthorizer.CIDRAuthorizerMode.valueOf(cidrAuthorizerMode.toUpperCase());
    }

    public static int getCidrGroupsCacheRefreshInterval()
    {
        int defaultCidrGroupsCacheRefreshInterval = 5; // mins

        if (conf.cidr_authorizer == null  || conf.cidr_authorizer.parameters == null)
            return defaultCidrGroupsCacheRefreshInterval;

        String cidrGroupsCacheRefreshInterval = conf.cidr_authorizer.parameters.get("cidr_groups_cache_refresh_interval");
        if (cidrGroupsCacheRefreshInterval == null || cidrGroupsCacheRefreshInterval.isEmpty())
            return defaultCidrGroupsCacheRefreshInterval;

        return Integer.parseInt(cidrGroupsCacheRefreshInterval);
    }

    public static int getIpCacheMaxSize()
    {
        int defaultIpCacheMaxSize = 100;

        if (conf.cidr_authorizer == null  || conf.cidr_authorizer.parameters == null)
            return defaultIpCacheMaxSize;

        String ipCacheMaxSize = conf.cidr_authorizer.parameters.get("ip_cache_max_size");
        if (ipCacheMaxSize == null || ipCacheMaxSize.isEmpty())
            return defaultIpCacheMaxSize;

        return Integer.parseInt(ipCacheMaxSize);
    }

    public static void setAuthFromRoot(boolean fromRoot)
    {
        conf.traverse_auth_from_root = fromRoot;
    }

    public static boolean getAuthFromRoot()
    {
        return conf.traverse_auth_from_root;
    }

    public static IRoleManager getRoleManager()
    {
        return roleManager;
    }

    public static void setRoleManager(IRoleManager roleManager)
    {
        DatabaseDescriptor.roleManager = roleManager;
    }

    public static int getPermissionsValidity()
    {
        return conf.permissions_validity.toMilliseconds();
    }

    public static void setPermissionsValidity(int timeout)
    {
        conf.permissions_validity = new DurationSpec.IntMillisecondsBound(timeout);
    }

    public static int getPermissionsUpdateInterval()
    {
        return conf.permissions_update_interval == null
             ? conf.permissions_validity.toMilliseconds()
             : conf.permissions_update_interval.toMilliseconds();
    }

    public static void setPermissionsUpdateInterval(int updateInterval)
    {
        if (updateInterval == -1)
            conf.permissions_update_interval = null;
        else
            conf.permissions_update_interval = new DurationSpec.IntMillisecondsBound(updateInterval);
    }

    public static int getPermissionsCacheMaxEntries()
    {
        return conf.permissions_cache_max_entries;
    }

    public static int setPermissionsCacheMaxEntries(int maxEntries)
    {
        return conf.permissions_cache_max_entries = maxEntries;
    }

    public static boolean getPermissionsCacheActiveUpdate()
    {
        return conf.permissions_cache_active_update;
    }

    public static void setPermissionsCacheActiveUpdate(boolean update)
    {
        conf.permissions_cache_active_update = update;
    }

    public static int getRolesValidity()
    {
        return conf.roles_validity.toMilliseconds();
    }

    public static void setRolesValidity(int validity)
    {
        conf.roles_validity = new DurationSpec.IntMillisecondsBound(validity);
    }

    public static int getRolesUpdateInterval()
    {
        return conf.roles_update_interval == null
             ? conf.roles_validity.toMilliseconds()
             : conf.roles_update_interval.toMilliseconds();
    }

    public static void setRolesCacheActiveUpdate(boolean update)
    {
        conf.roles_cache_active_update = update;
    }

    public static boolean getRolesCacheActiveUpdate()
    {
        return conf.roles_cache_active_update;
    }

    public static void setRolesUpdateInterval(int interval)
    {
        if (interval == -1)
            conf.roles_update_interval = null;
        else
            conf.roles_update_interval = new DurationSpec.IntMillisecondsBound(interval);
    }

    public static int getRolesCacheMaxEntries()
    {
        return conf.roles_cache_max_entries;
    }

    public static int setRolesCacheMaxEntries(int maxEntries)
    {
        return conf.roles_cache_max_entries = maxEntries;
    }

    public static int getCredentialsValidity()
    {
        return conf.credentials_validity.toMilliseconds();
    }

    public static void setCredentialsValidity(int timeout)
    {
        conf.credentials_validity = new DurationSpec.IntMillisecondsBound(timeout);
    }

    public static int getCredentialsUpdateInterval()
    {
        return conf.credentials_update_interval == null
               ? conf.credentials_validity.toMilliseconds()
               : conf.credentials_update_interval.toMilliseconds();
    }

    public static void setCredentialsUpdateInterval(int updateInterval)
    {
        if (updateInterval == -1)
            conf.credentials_update_interval = null;
        else
            conf.credentials_update_interval = new DurationSpec.IntMillisecondsBound(updateInterval);
    }

    public static int getCredentialsCacheMaxEntries()
    {
        return conf.credentials_cache_max_entries;
    }

    public static int setCredentialsCacheMaxEntries(int maxEntries)
    {
        return conf.credentials_cache_max_entries = maxEntries;
    }

    public static boolean getCredentialsCacheActiveUpdate()
    {
        return conf.credentials_cache_active_update;
    }

    public static void setCredentialsCacheActiveUpdate(boolean update)
    {
        conf.credentials_cache_active_update = update;
    }

    public static int getMaxValueSize()
    {
        return Ints.saturatedCast(conf.max_value_size.toMebibytes() * 1024L * 1024);
    }

    public static void setMaxValueSize(int maxValueSizeInBytes)
    {
        // the below division is safe as this setter is used only in tests with values that won't lead to precision loss
        conf.max_value_size = new DataStorageSpec.IntMebibytesBound((maxValueSizeInBytes / (1024L * 1024)), MEBIBYTES);
    }

    /**
     * Creates all storage-related directories.
     */
    public static void createAllDirectories()
    {
        try
        {
            if (conf.data_file_directories.length == 0)
                throw new ConfigurationException("At least one DataFileDirectory must be specified", false);

            for (String dataFileDirectory : conf.data_file_directories)
                FileUtils.createDirectory(dataFileDirectory);

            if (conf.local_system_data_file_directory != null)
                FileUtils.createDirectory(conf.local_system_data_file_directory);

            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory must be specified", false);
            FileUtils.createDirectory(conf.commitlog_directory);

            if (conf.hints_directory == null)
                throw new ConfigurationException("hints_directory must be specified", false);
            FileUtils.createDirectory(conf.hints_directory);

            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory must be specified", false);
            FileUtils.createDirectory(conf.saved_caches_directory);

            if (conf.cdc_enabled)
            {
                if (conf.cdc_raw_directory == null)
                    throw new ConfigurationException("cdc_raw_directory must be specified", false);
                FileUtils.createDirectory(conf.cdc_raw_directory);
            }

            boolean created = maybeCreateHeapDumpPath();
            if (!created && conf.dump_heap_on_uncaught_exception)
            {
                logger.error(String.format("cassandra.yaml:dump_heap_on_uncaught_exception is enabled but unable to create heap dump path %s. Disabling.", conf.heap_dump_path != null ? conf.heap_dump_path : "null"));
                conf.dump_heap_on_uncaught_exception = false;
            }
        }
        catch (ConfigurationException e)
        {
            throw new IllegalArgumentException("Bad configuration; unable to start server: "+e.getMessage());
        }
        catch (FSWriteError e)
        {
            throw new IllegalStateException(e.getCause().getMessage() + "; unable to start server");
        }
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public static String getPartitionerName()
    {
        return paritionerName;
    }

    /* For tests ONLY, don't use otherwise or all hell will break loose. Tests should restore value at the end. */
    public static IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner old = partitioner;
        partitioner = newPartitioner;
        return old;
    }

    public static IEndpointSnitch getEndpointSnitch()
    {
        return snitch;
    }
    public static void setEndpointSnitch(IEndpointSnitch eps)
    {
        snitch = eps;
    }

    public static IFailureDetector newFailureDetector()
    {
        return newFailureDetector.get();
    }

    public static void setDefaultFailureDetector()
    {
        newFailureDetector = () -> createFailureDetector("FailureDetector");
    }

    public static int getColumnIndexSize(int defaultValue)
    {
        return conf.column_index_size != null ? conf.column_index_size.toBytes() : defaultValue;
    }

    public static int getColumnIndexSizeInKiB()
    {
        return conf.column_index_size != null ? conf.column_index_size.toKibibytes() : -1;
    }

    public static void setColumnIndexSizeInKiB(int val)
    {
        conf.column_index_size = val != -1 ? createIntKibibyteBoundAndEnsureItIsValidForByteConversion(val,"column_index_size") : null;
    }

    public static int getColumnIndexCacheSize()
    {
        return conf.column_index_cache_size.toBytes();
    }

    public static int getColumnIndexCacheSizeInKiB()
    {
        return conf.column_index_cache_size.toKibibytes();
    }

    public static void setColumnIndexCacheSize(int val)
    {
        conf.column_index_cache_size = createIntKibibyteBoundAndEnsureItIsValidForByteConversion(val,"column_index_cache_size");
    }

    public static int getBatchSizeWarnThreshold()
    {
        return conf.batch_size_warn_threshold.toBytes();
    }

    public static int getBatchSizeWarnThresholdInKiB()
    {
        return conf.batch_size_warn_threshold.toKibibytes();
    }

    public static long getBatchSizeFailThreshold()
    {
        return conf.batch_size_fail_threshold.toBytesInLong();
    }

    public static int getBatchSizeFailThresholdInKiB()
    {
        return conf.batch_size_fail_threshold.toKibibytes();
    }

    public static int getUnloggedBatchAcrossPartitionsWarnThreshold()
    {
        return conf.unlogged_batch_across_partitions_warn_threshold;
    }

    public static void setBatchSizeWarnThresholdInKiB(int threshold)
    {
        conf.batch_size_warn_threshold = createIntKibibyteBoundAndEnsureItIsValidForByteConversion(threshold,"batch_size_warn_threshold");
    }

    public static void setBatchSizeFailThresholdInKiB(int threshold)
    {
        conf.batch_size_fail_threshold = new DataStorageSpec.IntKibibytesBound(threshold);
    }

    public static Collection<String> getInitialTokens()
    {
        return tokensFromString(INITIAL_TOKEN.getString(conf.initial_token));
    }

    public static String getAllocateTokensForKeyspace()
    {
        return ALLOCATE_TOKENS_FOR_KEYSPACE.getString(conf.allocate_tokens_for_keyspace);
    }

    public static Integer getAllocateTokensForLocalRf()
    {
        return conf.allocate_tokens_for_local_replication_factor;
    }

    public static Collection<String> tokensFromString(String tokenString)
    {
        List<String> tokens = new ArrayList<>();
        if (tokenString != null)
            for (String token : StringUtils.split(tokenString, ','))
                tokens.add(token.trim());
        return tokens;
    }

    public static int getNumTokens()
    {
        return conf.num_tokens;
    }

    public static InetAddressAndPort getReplaceAddress()
    {
        try
        {
            String replaceAddress = REPLACE_ADDRESS.getString();
            if (replaceAddress != null)
                return InetAddressAndPort.getByName(replaceAddress);

            String replaceAddressFirsstBoot = REPLACE_ADDRESS_FIRST_BOOT.getString();
            if (replaceAddressFirsstBoot != null)
                return InetAddressAndPort.getByName(replaceAddressFirsstBoot);

            return null;
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Replacement host name could not be resolved or scope_id was specified for a global IPv6 address", e);
        }
    }

    public static Collection<String> getReplaceTokens()
    {
        return tokensFromString(REPLACE_TOKEN.getString());
    }

    public static UUID getReplaceNode()
    {
        try
        {
            return UUID.fromString(REPLACE_NODE.getString());
        } catch (NullPointerException e)
        {
            return null;
        }
    }

    public static String getClusterName()
    {
        return conf.cluster_name;
    }

    public static int getStoragePort()
    {
        return STORAGE_PORT.getInt(conf.storage_port);
    }

    public static int getSSLStoragePort()
    {
        return SSL_STORAGE_PORT.getInt(conf.ssl_storage_port);
    }

    public static long nativeTransportIdleTimeout()
    {
        return conf.native_transport_idle_timeout.toMilliseconds();
    }

    public static void setNativeTransportIdleTimeout(long nativeTransportTimeout)
    {
        conf.native_transport_idle_timeout = new DurationSpec.LongMillisecondsBound(nativeTransportTimeout);
    }

    public static long getRpcTimeout(TimeUnit unit)
    {
        return conf.request_timeout.to(unit);
    }

    public static void setRpcTimeout(long timeOutInMillis)
    {
        conf.request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getReadRpcTimeout(TimeUnit unit)
    {
        return conf.read_request_timeout.to(unit);
    }

    public static void setReadRpcTimeout(long timeOutInMillis)
    {
        conf.read_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getRangeRpcTimeout(TimeUnit unit)
    {
        return conf.range_request_timeout.to(unit);
    }

    public static void setRangeRpcTimeout(long timeOutInMillis)
    {
        conf.range_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getWriteRpcTimeout(TimeUnit unit)
    {
        return conf.write_request_timeout.to(unit);
    }

    public static void setWriteRpcTimeout(long timeOutInMillis)
    {
        conf.write_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getCounterWriteRpcTimeout(TimeUnit unit)
    {
        return conf.counter_write_request_timeout.to(unit);
    }

    public static void setCounterWriteRpcTimeout(long timeOutInMillis)
    {
        conf.counter_write_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getCasContentionTimeout(TimeUnit unit)
    {
        return conf.cas_contention_timeout.to(unit);
    }

    public static void setCasContentionTimeout(long timeOutInMillis)
    {
        conf.cas_contention_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getTruncateRpcTimeout(TimeUnit unit)
    {
        return conf.truncate_request_timeout.to(unit);
    }

    public static void setTruncateRpcTimeout(long timeOutInMillis)
    {
        conf.truncate_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static long getRepairRpcTimeout(TimeUnit unit)
    {
        return conf.repair_request_timeout.to(unit);
    }

    public static void setRepairRpcTimeout(Long timeOutInMillis)
    {
        conf.repair_request_timeout = new DurationSpec.LongMillisecondsBound(timeOutInMillis);
    }

    public static boolean hasCrossNodeTimeout()
    {
        return conf.internode_timeout;
    }

    public static void setCrossNodeTimeout(boolean crossNodeTimeout)
    {
        conf.internode_timeout = crossNodeTimeout;
    }

    public static long getSlowQueryTimeout(TimeUnit unit)
    {
        return conf.slow_query_log_timeout.to(unit);
    }

    /**
     * @return the minimum configured {read, write, range, truncate, misc} timeout
     */
    public static long getMinRpcTimeout(TimeUnit unit)
    {
        return Longs.min(getRpcTimeout(unit),
                         getReadRpcTimeout(unit),
                         getRangeRpcTimeout(unit),
                         getWriteRpcTimeout(unit),
                         getCounterWriteRpcTimeout(unit),
                         getTruncateRpcTimeout(unit));
    }

    public static long getPingTimeout(TimeUnit unit)
    {
        return unit.convert(getBlockForPeersTimeoutInSeconds(), TimeUnit.SECONDS);
    }

    public static double getPhiConvictThreshold()
    {
        return conf.phi_convict_threshold;
    }

    public static void setPhiConvictThreshold(double phiConvictThreshold)
    {
        conf.phi_convict_threshold = phiConvictThreshold;
    }

    public static int getConcurrentReaders()
    {
        return conf.concurrent_reads;
    }

    public static void setConcurrentReaders(int concurrent_reads)
    {
        if (concurrent_reads < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_reads = concurrent_reads;
    }

    public static int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public static void setConcurrentWriters(int concurrent_writers)
    {
        if (concurrent_writers < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_writes = concurrent_writers;
    }

    public static int getConcurrentCounterWriters()
    {
        return conf.concurrent_counter_writes;
    }

    public static void setConcurrentCounterWriters(int concurrent_counter_writes)
    {
        if (concurrent_counter_writes < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_counter_writes = concurrent_counter_writes;
    }

    public static int getConcurrentViewWriters()
    {
        return conf.concurrent_materialized_view_writes;
    }

    public static void setConcurrentViewWriters(int concurrent_materialized_view_writes)
    {
        if (concurrent_materialized_view_writes < 0)
        {
            throw new IllegalArgumentException("Concurrent reads must be non-negative");
        }
        conf.concurrent_materialized_view_writes = concurrent_materialized_view_writes;
    }

    public static int getFlushWriters()
    {
        return conf.memtable_flush_writers;
    }

    public static int getAvailableProcessors()
    {
        return conf == null ? -1 : conf.available_processors;
    }

    public static int getConcurrentCompactors()
    {
        return conf.concurrent_compactors;
    }

    public static void setConcurrentCompactors(int value)
    {
        conf.concurrent_compactors = value;
    }

    public static int getCompactionThroughputMebibytesPerSecAsInt()
    {
        return conf.compaction_throughput.toMebibytesPerSecondAsInt();
    }

    public static double getCompactionThroughputBytesPerSec()
    {
        return conf.compaction_throughput.toBytesPerSecond();
    }

    public static double getCompactionThroughputMebibytesPerSec()
    {
        return conf.compaction_throughput.toMebibytesPerSecond();
    }

    @VisibleForTesting // only for testing!
    public static void setCompactionThroughputBytesPerSec(int value)
    {
        if (BYTES_PER_SECOND.toMebibytesPerSecond(value) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("compaction_throughput: " + value +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in MiB/s");

        conf.compaction_throughput = new DataRateSpec.LongBytesPerSecondBound(value);
    }

    public static void setCompactionThroughputMebibytesPerSec(int value)
    {
        if (value == Integer.MAX_VALUE)
            throw new IllegalArgumentException("compaction_throughput: " + value +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in MiB/s");

        conf.compaction_throughput = new DataRateSpec.LongBytesPerSecondBound(value, MEBIBYTES_PER_SECOND);
    }

    public static int getConcurrentValidations()
    {
        return conf.concurrent_validations;
    }

    public static int getConcurrentIndexBuilders()
    {
        return conf.concurrent_index_builders;
    }

    public static void setConcurrentValidations(int value)
    {
        value = value > 0 ? value : Integer.MAX_VALUE;
        conf.concurrent_validations = value;
    }

    public static int getConcurrentViewBuilders()
    {
        return conf.concurrent_materialized_view_builders;
    }

    public static void setConcurrentViewBuilders(int value)
    {
        conf.concurrent_materialized_view_builders = value;
    }

    public static long getMinFreeSpacePerDriveInMebibytes()
    {
        return conf.min_free_space_per_drive.toMebibytes();
    }

    public static long getMinFreeSpacePerDriveInBytes()
    {
        return conf.min_free_space_per_drive.toBytesInLong();
    }

    @VisibleForTesting
    public static long setMinFreeSpacePerDriveInMebibytes(long mebiBytes)
    {
        conf.min_free_space_per_drive = new DataStorageSpec.IntMebibytesBound(mebiBytes);
        return getMinFreeSpacePerDriveInBytes();
    }

    public static double getMaxSpaceForCompactionsPerDrive()
    {
        return conf.max_space_usable_for_compactions_in_percentage;
    }

    public static void setMaxSpaceForCompactionsPerDrive(double percentage)
    {
        conf.max_space_usable_for_compactions_in_percentage = percentage;
    }

    public static boolean getDisableSTCSInL0()
    {
        return disableSTCSInL0;
    }

    public static void setDisableSTCSInL0(boolean disabled)
    {
        disableSTCSInL0 = disabled;
    }

    public static int getStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.stream_throughput_outbound.toMegabitsPerSecondAsInt();
    }

    public static double getStreamThroughputOutboundMegabitsPerSecAsDouble()
    {
        return conf.stream_throughput_outbound.toMegabitsPerSecond();
    }

    public static double getStreamThroughputOutboundMebibytesPerSec()
    {
        return conf.stream_throughput_outbound.toMebibytesPerSecond();
    }

    public static double getStreamThroughputOutboundBytesPerSec()
    {
        return conf.stream_throughput_outbound.toBytesPerSecond();
    }

    public static int getStreamThroughputOutboundMebibytesPerSecAsInt()
    {
        return conf.stream_throughput_outbound.toMebibytesPerSecondAsInt();
    }

    public static void setStreamThroughputOutboundMebibytesPerSecAsInt(int value)
    {
        if (MEBIBYTES_PER_SECOND.toMegabitsPerSecond(value) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("stream_throughput_outbound: " + value  +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in megabits/s");

        conf.stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, MEBIBYTES_PER_SECOND);
    }

    public static void setStreamThroughputOutboundBytesPerSec(long value)
    {
        conf.stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, BYTES_PER_SECOND);
    }

    public static void setStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.stream_throughput_outbound = DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(value);
    }

    public static double getEntireSSTableStreamThroughputOutboundMebibytesPerSec()
    {
        return conf.entire_sstable_stream_throughput_outbound.toMebibytesPerSecond();
    }

    public static double getEntireSSTableStreamThroughputOutboundBytesPerSec()
    {
        return conf.entire_sstable_stream_throughput_outbound.toBytesPerSecond();
    }

    public static void setEntireSSTableStreamThroughputOutboundMebibytesPerSec(int value)
    {
        if (value == Integer.MAX_VALUE)
            throw new IllegalArgumentException("entire_sstable_stream_throughput_outbound: " + value +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in MiB/s");

        conf.entire_sstable_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, MEBIBYTES_PER_SECOND);
    }

    public static int getInterDCStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound.toMegabitsPerSecondAsInt();
    }

    public static double getInterDCStreamThroughputOutboundMegabitsPerSecAsDouble()
    {
        return conf.inter_dc_stream_throughput_outbound.toMegabitsPerSecond();
    }

    public static double getInterDCStreamThroughputOutboundMebibytesPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound.toMebibytesPerSecond();
    }

    public static double getInterDCStreamThroughputOutboundBytesPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound.toBytesPerSecond();
    }

    public static int getInterDCStreamThroughputOutboundMebibytesPerSecAsInt()
    {
        return conf.inter_dc_stream_throughput_outbound.toMebibytesPerSecondAsInt();
    }

    public static void setInterDCStreamThroughputOutboundMebibytesPerSecAsInt(int value)
    {
        if (MEBIBYTES_PER_SECOND.toMegabitsPerSecond(value) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("inter_dc_stream_throughput_outbound: " + value +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in megabits/s");

        conf.inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, MEBIBYTES_PER_SECOND);
    }

    public static void setInterDCStreamThroughputOutboundBytesPerSec(long value)
    {
        conf.inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, BYTES_PER_SECOND);
    }

    public static void setInterDCStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.inter_dc_stream_throughput_outbound = DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(value);
    }

    public static double getEntireSSTableInterDCStreamThroughputOutboundBytesPerSec()
    {
        return conf.entire_sstable_inter_dc_stream_throughput_outbound.toBytesPerSecond();
    }

    public static double getEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec()
    {
        return conf.entire_sstable_inter_dc_stream_throughput_outbound.toMebibytesPerSecond();
    }

    public static void setEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(int value)
    {
        if (value == Integer.MAX_VALUE)
            throw new IllegalArgumentException("entire_sstable_inter_dc_stream_throughput_outbound: " + value +
                                               " is too large; it should be less than " +
                                               Integer.MAX_VALUE + " in MiB/s");

        conf.entire_sstable_inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(value, MEBIBYTES_PER_SECOND);
    }

    /**
     * Checks if the local system data must be stored in a specific location which supports redundancy.
     *
     * @return {@code true} if the local system keyspaces data must be stored in a different location,
     * {@code false} otherwise.
     */
    public static boolean useSpecificLocationForLocalSystemData()
    {
        return conf.local_system_data_file_directory != null;
    }

    /**
     * Returns the locations where the local system keyspaces data should be stored.
     *
     * <p>If the {@code local_system_data_file_directory} was unspecified, the local system keyspaces data should be stored
     * in the first data directory. This approach guarantees that the server can tolerate the lost of all the disks but the first one.</p>
     *
     * @return the locations where should be stored the local system keyspaces data
     */
    public static String[] getLocalSystemKeyspacesDataFileLocations()
    {
        if (useSpecificLocationForLocalSystemData())
            return new String[] {conf.local_system_data_file_directory};

        return conf.data_file_directories.length == 0  ? conf.data_file_directories
                                                       : new String[] {conf.data_file_directories[0]};
    }

    /**
     * Returns the locations where the non local system keyspaces data should be stored.
     *
     * @return the locations where the non local system keyspaces data should be stored.
     */
    public static String[] getNonLocalSystemKeyspacesDataFileLocations()
    {
        return conf.data_file_directories;
    }

    /**
     * Returns the list of all the directories where the data files can be stored (for local system and non local system keyspaces).
     *
     * @return the list of all the directories where the data files can be stored.
     */
    public static String[] getAllDataFileLocations()
    {
        if (conf.local_system_data_file_directory == null)
            return conf.data_file_directories;

        return ArrayUtils.addFirst(conf.data_file_directories, conf.local_system_data_file_directory);
    }

    public static String getCommitLogLocation()
    {
        return conf.commitlog_directory;
    }

    @VisibleForTesting
    public static void setCommitLogLocation(String value)
    {
        conf.commitlog_directory = value;
    }

    public static ParameterizedClass getCommitLogCompression()
    {
        return conf.commitlog_compression;
    }

    @VisibleForTesting
    public static void setCommitLogCompression(ParameterizedClass compressor)
    {
        conf.commitlog_compression = compressor;
    }

    public static Config.FlushCompression getFlushCompression()
    {
        return conf.flush_compression;
    }

    public static void setFlushCompression(Config.FlushCompression compression)
    {
        conf.flush_compression = compression;
    }

   /**
    * Maximum number of buffers in the compression pool. The default value is 3, it should not be set lower than that
    * (one segment in compression, one written to, one in reserve); delays in compression may cause the log to use
    * more, depending on how soon the sync policy stops all writing threads.
    */
    public static int getCommitLogMaxCompressionBuffersInPool()
    {
        return conf.commitlog_max_compression_buffers_in_pool;
    }

    public static void setCommitLogMaxCompressionBuffersPerPool(int buffers)
    {
        conf.commitlog_max_compression_buffers_in_pool = buffers;
    }

    public static int getMaxMutationSize()
    {
        return conf.max_mutation_size.toBytes();
    }

    public static int getTombstoneWarnThreshold()
    {
        return conf.tombstone_warn_threshold;
    }

    public static void setTombstoneWarnThreshold(int threshold)
    {
        conf.tombstone_warn_threshold = threshold;
    }

    public static int getTombstoneFailureThreshold()
    {
        return conf.tombstone_failure_threshold;
    }

    public static void setTombstoneFailureThreshold(int threshold)
    {
        conf.tombstone_failure_threshold = threshold;
    }

    public static int getCachedReplicaRowsWarnThreshold()
    {
        return conf.replica_filtering_protection.cached_rows_warn_threshold;
    }

    public static void setCachedReplicaRowsWarnThreshold(int threshold)
    {
        conf.replica_filtering_protection.cached_rows_warn_threshold = threshold;
    }

    public static int getCachedReplicaRowsFailThreshold()
    {
        return conf.replica_filtering_protection.cached_rows_fail_threshold;
    }

    public static void setCachedReplicaRowsFailThreshold(int threshold)
    {
        conf.replica_filtering_protection.cached_rows_fail_threshold = threshold;
    }

    /**
     * size of commitlog segments to allocate
     */
    public static int getCommitLogSegmentSize()
    {
        return conf.commitlog_segment_size.toBytes();
    }

    /**
     * Update commitlog_segment_size in the tests.
     * {@link CommitLogSegmentManagerCDC} uses the CommitLogSegmentSize to estimate the file size on allocation.
     * It is important to keep the value unchanged for the estimation to be correct.
     * @param sizeMebibytes
     */
    @VisibleForTesting /* Only for testing */
    public static void setCommitLogSegmentSize(int sizeMebibytes)
    {
        conf.commitlog_segment_size = new DataStorageSpec.IntMebibytesBound(sizeMebibytes);
    }

    /**
     * Return commitlog disk access mode.
     */
    public static DiskAccessMode getCommitLogWriteDiskAccessMode()
    {
        return commitLogWriteDiskAccessMode;
    }

    @VisibleForTesting
    public static void setCommitLogWriteDiskAccessMode(DiskAccessMode diskAccessMode)
    {
        conf.commitlog_disk_access_mode = diskAccessMode;
    }

    @VisibleForTesting
    public static void initializeCommitLogDiskAccessMode()
    {
        DiskAccessMode resolved = resolveCommitLogWriteDiskAccessMode(conf.commitlog_disk_access_mode);
        validateCommitLogWriteDiskAccessMode(resolved);
        commitLogWriteDiskAccessMode = resolved;
    }

    public static String getSavedCachesLocation()
    {
        return conf.saved_caches_directory;
    }

    public static Set<InetAddressAndPort> getSeeds()
    {
        return ImmutableSet.<InetAddressAndPort>builder().addAll(seedProvider.getSeeds()).build();
    }

    public static SeedProvider getSeedProvider()
    {
        return seedProvider;
    }

    public static void setSeedProvider(SeedProvider newSeedProvider)
    {
        seedProvider = newSeedProvider;
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }

    public static void setListenAddress(InetAddress newlistenAddress)
    {
        listenAddress = newlistenAddress;
    }

    public static InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }

    public static boolean shouldListenOnBroadcastAddress()
    {
        return conf.listen_on_broadcast_address;
    }

    public static void setShouldListenOnBroadcastAddress(boolean shouldListenOnBroadcastAddress)
    {
        conf.listen_on_broadcast_address = shouldListenOnBroadcastAddress;
    }

    public static void setListenOnBroadcastAddress(boolean listen_on_broadcast_address)
    {
        conf.listen_on_broadcast_address = listen_on_broadcast_address;
    }

    public static IInternodeAuthenticator getInternodeAuthenticator()
    {
        return internodeAuthenticator;
    }

    public static void setInternodeAuthenticator(IInternodeAuthenticator internodeAuthenticator)
    {
        Preconditions.checkNotNull(internodeAuthenticator);
        DatabaseDescriptor.internodeAuthenticator = internodeAuthenticator;
    }

    public static void setBroadcastAddress(InetAddress broadcastAdd)
    {
        broadcastAddress = broadcastAdd;
    }

    /**
     * This is the address used to bind for the native protocol to communicate with clients. Most usages in the code
     * refer to it as native address although some places still call it RPC address. It's not thrift RPC anymore
     * so native is more appropriate. The address alone is not enough to uniquely identify this instance because
     * multiple instances might use the same interface with different ports.
     */
    public static InetAddress getRpcAddress()
    {
        return rpcAddress;
    }

    public static void setBroadcastRpcAddress(InetAddress broadcastRPCAddr)
    {
        broadcastRpcAddress = broadcastRPCAddr;
    }

    /**
     * This is the address used to reach this instance for the native protocol to communicate with clients. Most usages in the code
     * refer to it as native address although some places still call it RPC address. It's not thrift RPC anymore
     * so native is more appropriate. The address alone is not enough to uniquely identify this instance because
     * multiple instances might use the same interface with different ports.
     *
     * May be null, please use {@link FBUtilities#getBroadcastNativeAddressAndPort()} instead.
     */
    public static InetAddress getBroadcastRpcAddress()
    {
        return broadcastRpcAddress;
    }

    public static boolean getRpcKeepAlive()
    {
        return conf.rpc_keepalive;
    }

    public static int getInternodeSocketSendBufferSizeInBytes()
    {
        return conf.internode_socket_send_buffer_size.toBytes();
    }

    public static int getInternodeSocketReceiveBufferSizeInBytes()
    {
        return conf.internode_socket_receive_buffer_size.toBytes();
    }

    public static int getInternodeApplicationSendQueueCapacityInBytes()
    {
        return conf.internode_application_send_queue_capacity.toBytes();
    }

    public static int getInternodeApplicationSendQueueReserveEndpointCapacityInBytes()
    {
        return conf.internode_application_send_queue_reserve_endpoint_capacity.toBytes();
    }

    public static int getInternodeApplicationSendQueueReserveGlobalCapacityInBytes()
    {
        return conf.internode_application_send_queue_reserve_global_capacity.toBytes();
    }

    public static int getInternodeApplicationReceiveQueueCapacityInBytes()
    {
        return conf.internode_application_receive_queue_capacity.toBytes();
    }

    public static int getInternodeApplicationReceiveQueueReserveEndpointCapacityInBytes()
    {
        return conf.internode_application_receive_queue_reserve_endpoint_capacity.toBytes();
    }

    public static int getInternodeApplicationReceiveQueueReserveGlobalCapacityInBytes()
    {
        return conf.internode_application_receive_queue_reserve_global_capacity.toBytes();
    }

    public static int getInternodeTcpConnectTimeoutInMS()
    {
        return conf.internode_tcp_connect_timeout.toMilliseconds();
    }

    public static void setInternodeTcpConnectTimeoutInMS(int value)
    {
        conf.internode_tcp_connect_timeout = new DurationSpec.IntMillisecondsBound(value);
    }

    public static int getInternodeTcpUserTimeoutInMS()
    {
        return conf.internode_tcp_user_timeout.toMilliseconds();
    }

    public static void setInternodeTcpUserTimeoutInMS(int value)
    {
        conf.internode_tcp_user_timeout = new DurationSpec.IntMillisecondsBound(value);
    }

    public static int getInternodeStreamingTcpUserTimeoutInMS()
    {
        return conf.internode_streaming_tcp_user_timeout.toMilliseconds();
    }

    public static void setInternodeStreamingTcpUserTimeoutInMS(int value)
    {
        conf.internode_streaming_tcp_user_timeout = new DurationSpec.IntMillisecondsBound(value);
    }

    public static int getInternodeMaxMessageSizeInBytes()
    {
        return conf.internode_max_message_size.toBytes();
    }

    @VisibleForTesting
    public static void setInternodeMaxMessageSizeInBytes(int value)
    {
        conf.internode_max_message_size = new DataStorageSpec.IntBytesBound(value);
    }

    public static boolean startNativeTransport()
    {
        return conf.start_native_transport;
    }

    /**
     *  This is the port used with RPC address for the native protocol to communicate with clients. Now that thrift RPC
     *  is no longer in use there is no RPC port.
     */
    public static int getNativeTransportPort()
    {
        return NATIVE_TRANSPORT_PORT.getInt(conf.native_transport_port);
    }

    @VisibleForTesting
    public static void setNativeTransportPort(int port)
    {
        conf.native_transport_port = port;
    }

    public static int getNativeTransportPortSSL()
    {
        return conf.native_transport_port_ssl == null ? getNativeTransportPort() : conf.native_transport_port_ssl;
    }

    @VisibleForTesting
    public static void setNativeTransportPortSSL(Integer port)
    {
        conf.native_transport_port_ssl = port;
    }

    public static int getNativeTransportMaxThreads()
    {
        return conf.native_transport_max_threads;
    }

    public static void setNativeTransportMaxThreads(int max_threads)
    {
        conf.native_transport_max_threads = max_threads;
    }

    public static Integer getNativeTransportMaxAuthThreads()
    {
        return conf.native_transport_max_auth_threads;
    }

    /**
     * If this value is set to <= 0 it will move auth requests to the standard request pool regardless of the current
     * size of the {@link org.apache.cassandra.transport.Dispatcher#authExecutor}'s active size.
     *
     * see {@link org.apache.cassandra.transport.Dispatcher#dispatch} for executor selection
     */
    public static void setNativeTransportMaxAuthThreads(int threads)
    {
        conf.native_transport_max_auth_threads = threads;
    }

    public static int getNativeTransportMaxFrameSize()
    {
        return conf.native_transport_max_frame_size.toBytes();
    }

    public static void setNativeTransportMaxFrameSize(int bytes)
    {
        conf.native_transport_max_frame_size = new DataStorageSpec.IntMebibytesBound(bytes);
    }

    public static long getNativeTransportMaxConcurrentConnections()
    {
        return conf.native_transport_max_concurrent_connections;
    }

    public static void setNativeTransportMaxConcurrentConnections(long nativeTransportMaxConcurrentConnections)
    {
        conf.native_transport_max_concurrent_connections = nativeTransportMaxConcurrentConnections;
    }

    public static long getNativeTransportMaxConcurrentConnectionsPerIp()
    {
        return conf.native_transport_max_concurrent_connections_per_ip;
    }

    public static void setNativeTransportMaxConcurrentConnectionsPerIp(long native_transport_max_concurrent_connections_per_ip)
    {
        conf.native_transport_max_concurrent_connections_per_ip = native_transport_max_concurrent_connections_per_ip;
    }

    public static boolean useNativeTransportLegacyFlusher()
    {
        return conf.native_transport_flush_in_batches_legacy;
    }

    public static boolean getNativeTransportAllowOlderProtocols()
    {
        return conf.native_transport_allow_older_protocols;
    }

    public static void setNativeTransportAllowOlderProtocols(boolean isEnabled)
    {
        conf.native_transport_allow_older_protocols = isEnabled;
    }

    public static long getCommitLogSyncGroupWindow()
    {
        return conf.commitlog_sync_group_window.toMilliseconds();
    }

    public static void setCommitLogSyncGroupWindow(long windowMillis)
    {
        conf.commitlog_sync_group_window = new DurationSpec.IntMillisecondsBound(windowMillis);
    }

    public static int getNativeTransportReceiveQueueCapacityInBytes()
    {
        return conf.native_transport_receive_queue_capacity.toBytes();
    }

    public static void setNativeTransportReceiveQueueCapacityInBytes(int queueSize)
    {
        conf.native_transport_receive_queue_capacity = new DataStorageSpec.IntBytesBound(queueSize);
    }

    public static long getNativeTransportMaxRequestDataInFlightPerIpInBytes()
    {
        return conf.native_transport_max_request_data_in_flight_per_ip.toBytes();
    }

    public static Config.PaxosVariant getPaxosVariant()
    {
        return conf.paxos_variant;
    }

    public static void setPaxosVariant(Config.PaxosVariant variant)
    {
        conf.paxos_variant = variant;
    }

    public static String getPaxosContentionWaitRandomizer()
    {
        return conf.paxos_contention_wait_randomizer;
    }

    public static String getPaxosContentionMinWait()
    {
        return conf.paxos_contention_min_wait;
    }

    public static String getPaxosContentionMaxWait()
    {
        return conf.paxos_contention_max_wait;
    }

    public static String getPaxosContentionMinDelta()
    {
        return conf.paxos_contention_min_delta;
    }

    public static void setPaxosContentionWaitRandomizer(String waitRandomizer)
    {
        conf.paxos_contention_wait_randomizer = waitRandomizer;
    }

    public static void setPaxosContentionMinWait(String minWait)
    {
        conf.paxos_contention_min_wait = minWait;
    }

    public static void setPaxosContentionMaxWait(String maxWait)
    {
        conf.paxos_contention_max_wait = maxWait;
    }

    public static void setPaxosContentionMinDelta(String minDelta)
    {
        conf.paxos_contention_min_delta = minDelta;
    }

    public static boolean skipPaxosRepairOnTopologyChange()
    {
        return conf.skip_paxos_repair_on_topology_change;
    }

    public static void setSkipPaxosRepairOnTopologyChange(boolean value)
    {
        conf.skip_paxos_repair_on_topology_change = value;
    }

    public static long getPaxosPurgeGrace(TimeUnit units)
    {
        return conf.paxos_purge_grace_period.to(units);
    }

    public static void setPaxosPurgeGrace(long seconds)
    {
        conf.paxos_purge_grace_period = new DurationSpec.LongSecondsBound(seconds);
    }

    public static PaxosOnLinearizabilityViolation paxosOnLinearizabilityViolations()
    {
        return conf.paxos_on_linearizability_violations;
    }

    public static void setPaxosOnLinearizabilityViolations(PaxosOnLinearizabilityViolation v)
    {
        conf.paxos_on_linearizability_violations = v;
    }

    public static PaxosStatePurging paxosStatePurging()
    {
        return conf.paxos_state_purging;
    }

    public static void setPaxosStatePurging(PaxosStatePurging v)
    {
        conf.paxos_state_purging = v;
    }

    public static boolean paxosRepairEnabled()
    {
        return conf.paxos_repair_enabled;
    }

    public static void setPaxosRepairEnabled(boolean v)
    {
        conf.paxos_repair_enabled = v;
    }

    public static Set<String> skipPaxosRepairOnTopologyChangeKeyspaces()
    {
        return conf.skip_paxos_repair_on_topology_change_keyspaces;
    }

    public static void setSkipPaxosRepairOnTopologyChangeKeyspaces(String keyspaces)
    {
        conf.skip_paxos_repair_on_topology_change_keyspaces = Config.splitCommaDelimited(keyspaces);
    }

    public static boolean paxoTopologyRepairNoDcChecks()
    {
        return conf.paxos_topology_repair_no_dc_checks;
    }

    public static boolean paxoTopologyRepairStrictEachQuorum()
    {
        return conf.paxos_topology_repair_strict_each_quorum;
    }

    public static void setNativeTransportMaxRequestDataInFlightPerIpInBytes(long maxRequestDataInFlightInBytes)
    {
        if (maxRequestDataInFlightInBytes == -1)
            maxRequestDataInFlightInBytes = Runtime.getRuntime().maxMemory() / 40;

        conf.native_transport_max_request_data_in_flight_per_ip = new DataStorageSpec.LongBytesBound(maxRequestDataInFlightInBytes);
    }

    public static long getNativeTransportMaxRequestDataInFlightInBytes()
    {
        return conf.native_transport_max_request_data_in_flight.toBytes();
    }

    public static void setNativeTransportConcurrentRequestDataInFlightInBytes(long maxRequestDataInFlightInBytes)
    {
        if (maxRequestDataInFlightInBytes == -1)
            maxRequestDataInFlightInBytes = Runtime.getRuntime().maxMemory() / 10;

        conf.native_transport_max_request_data_in_flight = new DataStorageSpec.LongBytesBound(maxRequestDataInFlightInBytes);
    }

    public static int getNativeTransportMaxRequestsPerSecond()
    {
        return conf.native_transport_max_requests_per_second;
    }

    public static void setNativeTransportMaxRequestsPerSecond(int perSecond)
    {
        Preconditions.checkArgument(perSecond > 0, "native_transport_max_requests_per_second must be greater than zero");
        conf.native_transport_max_requests_per_second = perSecond;
    }

    public static void setNativeTransportRateLimitingEnabled(boolean enabled)
    {
        logger.info("native_transport_rate_limiting_enabled set to {}", enabled);
        conf.native_transport_rate_limiting_enabled = enabled;
    }

    public static boolean getNativeTransportRateLimitingEnabled()
    {
        return conf.native_transport_rate_limiting_enabled;
    }

    public static int getCommitLogSyncPeriod()
    {
        return conf.commitlog_sync_period.toMilliseconds();
    }

    public static long getPeriodicCommitLogSyncBlock()
    {
        DurationSpec.IntMillisecondsBound blockMillis = conf.periodic_commitlog_sync_lag_block;
        return blockMillis == null
               ? (long)(getCommitLogSyncPeriod() * 1.5)
               : blockMillis.toMilliseconds();
    }

    public static void setCommitLogSyncPeriod(int periodMillis)
    {
        conf.commitlog_sync_period = new DurationSpec.IntMillisecondsBound(periodMillis);
    }

    public static Config.CommitLogSync getCommitLogSync()
    {
        return conf.commitlog_sync;
    }

    public static void setCommitLogSync(CommitLogSync sync)
    {
        conf.commitlog_sync = sync;
    }

    public static DiskAccessMode getDiskAccessMode()
    {
        return conf.disk_access_mode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setDiskAccessMode(DiskAccessMode mode)
    {
        conf.disk_access_mode = mode;
    }

    public static DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setIndexAccessMode(DiskAccessMode mode)
    {
        indexAccessMode = mode;
    }

    public static void setDiskFailurePolicy(Config.DiskFailurePolicy policy)
    {
        conf.disk_failure_policy = policy;
    }

    public static Config.DiskFailurePolicy getDiskFailurePolicy()
    {
        return conf.disk_failure_policy;
    }

    public static void setCommitFailurePolicy(Config.CommitFailurePolicy policy)
    {
        conf.commit_failure_policy = policy;
    }

    public static Config.CommitFailurePolicy getCommitFailurePolicy()
    {
        return conf.commit_failure_policy;
    }

    public static boolean isSnapshotBeforeCompaction()
    {
        return conf.snapshot_before_compaction;
    }

    public static boolean isAutoSnapshot()
    {
        return conf.auto_snapshot;
    }

    public static DurationSpec.IntSecondsBound getAutoSnapshotTtl()
    {
        return autoSnapshoTtl;
    }

    @VisibleForTesting
    public static void setAutoSnapshotTtl(DurationSpec.IntSecondsBound newTtl)
    {
        autoSnapshoTtl = newTtl;
    }

    @VisibleForTesting
    public static void setAutoSnapshot(boolean autoSnapshot)
    {
        conf.auto_snapshot = autoSnapshot;
    }
    @VisibleForTesting
    public static boolean getAutoSnapshot()
    {
        return conf.auto_snapshot;
    }

    public static long getSnapshotLinksPerSecond()
    {
        return conf.snapshot_links_per_second == 0 ? Long.MAX_VALUE : conf.snapshot_links_per_second;
    }

    public static void setSnapshotLinksPerSecond(long throttle)
    {
        if (throttle < 0)
            throw new IllegalArgumentException("Invalid throttle for snapshot_links_per_second: must be positive");

        conf.snapshot_links_per_second = throttle;
    }

    public static RateLimiter getSnapshotRateLimiter()
    {
        return RateLimiter.create(getSnapshotLinksPerSecond());
    }

    public static boolean isAutoBootstrap()
    {
        return AUTO_BOOTSTRAP.getBoolean(conf.auto_bootstrap);
    }

    public static void setHintedHandoffEnabled(boolean hintedHandoffEnabled)
    {
        conf.hinted_handoff_enabled = hintedHandoffEnabled;
    }

    public static boolean hintedHandoffEnabled()
    {
        return conf.hinted_handoff_enabled;
    }

    public static Set<String> hintedHandoffDisabledDCs()
    {
        return conf.hinted_handoff_disabled_datacenters;
    }

    public static boolean useDeterministicTableID()
    {
        return conf != null && conf.use_deterministic_table_id;
    }

    public static void useDeterministicTableID(boolean value)
    {
        conf.use_deterministic_table_id = value;
    }

    public static void enableHintsForDC(String dc)
    {
        conf.hinted_handoff_disabled_datacenters.remove(dc);
    }

    public static void disableHintsForDC(String dc)
    {
        conf.hinted_handoff_disabled_datacenters.add(dc);
    }

    public static void setMaxHintWindow(int ms)
    {
        conf.max_hint_window = new DurationSpec.IntMillisecondsBound(ms);
    }

    public static int getMaxHintWindow()
    {
        return conf.max_hint_window.toMilliseconds();
    }

    public static void setMaxHintsSizePerHostInMiB(int value)
    {
        conf.max_hints_size_per_host = new DataStorageSpec.LongBytesBound(value, MEBIBYTES);
    }

    public static int getMaxHintsSizePerHostInMiB()
    {
        // Warnings: this conversion rounds down while converting bytes to mebibytes
        return Ints.saturatedCast(conf.max_hints_size_per_host.unit().toMebibytes(conf.max_hints_size_per_host.quantity()));
    }

    public static long getMaxHintsSizePerHost()
    {
        return conf.max_hints_size_per_host.toBytes();
    }

    public static File getHintsDirectory()
    {
        return new File(conf.hints_directory);
    }

    public static boolean hintWindowPersistentEnabled()
    {
        return conf.hint_window_persistent_enabled;
    }

    public static File getSerializedCachePath(CacheType cacheType, String version, String extension)
    {
        String name = cacheType.toString()
                + (version == null ? "" : '-' + version + '.' + extension);
        return new File(conf.saved_caches_directory, name);
    }

    public static int getDynamicUpdateInterval()
    {
        return conf.dynamic_snitch_update_interval.toMilliseconds();
    }
    public static void setDynamicUpdateInterval(int dynamicUpdateInterval)
    {
        conf.dynamic_snitch_update_interval = new DurationSpec.IntMillisecondsBound(dynamicUpdateInterval);
    }

    public static int getDynamicResetInterval()
    {
        return conf.dynamic_snitch_reset_interval.toMilliseconds();
    }
    public static void setDynamicResetInterval(int dynamicResetInterval)
    {
        conf.dynamic_snitch_reset_interval = new DurationSpec.IntMillisecondsBound(dynamicResetInterval);
    }

    public static double getDynamicBadnessThreshold()
    {
        return conf.dynamic_snitch_badness_threshold;
    }

    public static void setDynamicBadnessThreshold(double dynamicBadnessThreshold)
    {
        conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public static EncryptionOptions.ServerEncryptionOptions getInternodeMessagingEncyptionOptions()
    {
        return conf.server_encryption_options;
    }

    public static void setInternodeMessagingEncyptionOptions(EncryptionOptions.ServerEncryptionOptions encryptionOptions)
    {
        conf.server_encryption_options = encryptionOptions;
    }

    public static EncryptionOptions getNativeProtocolEncryptionOptions()
    {
        return conf.client_encryption_options;
    }

    @VisibleForTesting
    public static void updateNativeProtocolEncryptionOptions(Function<EncryptionOptions, EncryptionOptions> update)
    {
        conf.client_encryption_options = update.apply(conf.client_encryption_options);
    }

    public static int getHintedHandoffThrottleInKiB()
    {
        return conf.hinted_handoff_throttle.toKibibytes();
    }

    public static void setHintedHandoffThrottleInKiB(int throttleInKiB)
    {
        conf.hinted_handoff_throttle = new DataStorageSpec.IntKibibytesBound(throttleInKiB);
    }

    public static int getBatchlogReplayThrottleInKiB()
    {
        return conf.batchlog_replay_throttle.toKibibytes();
    }

    public static void setBatchlogReplayThrottleInKiB(int throttleInKiB)
    {
        conf.batchlog_replay_throttle = new DataStorageSpec.IntKibibytesBound(throttleInKiB);
    }

    public static int getMaxHintsDeliveryThreads()
    {
        return conf.max_hints_delivery_threads;
    }

    public static int getHintsFlushPeriodInMS()
    {
        return conf.hints_flush_period.toMilliseconds();
    }

    public static long getMaxHintsFileSize()
    {
        return  conf.max_hints_file_size.toBytesInLong();
    }

    public static ParameterizedClass getHintsCompression()
    {
        return conf.hints_compression;
    }

    public static void setHintsCompression(ParameterizedClass parameterizedClass)
    {
        conf.hints_compression = parameterizedClass;
    }

    public static boolean isAutoHintsCleanupEnabled()
    {
        return conf.auto_hints_cleanup_enabled;
    }

    public static void setAutoHintsCleanupEnabled(boolean value)
    {
        conf.auto_hints_cleanup_enabled = value;
    }

    public static boolean getTransferHintsOnDecommission()
    {
        return conf.transfer_hints_on_decommission;
    }

    public static void setTransferHintsOnDecommission(boolean enabled)
    {
        conf.transfer_hints_on_decommission = enabled;
    }

    public static boolean isIncrementalBackupsEnabled()
    {
        return conf.incremental_backups;
    }

    public static void setIncrementalBackupsEnabled(boolean value)
    {
        conf.incremental_backups = value;
    }

    public static boolean getFileCacheEnabled()
    {
        return conf.file_cache_enabled;
    }

    @VisibleForTesting
    public static void setFileCacheEnabled(boolean enabled)
    {
        conf.file_cache_enabled = enabled;
    }

    public static int getFileCacheSizeInMiB()
    {
        if (conf.file_cache_size == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return 0;
        }

        return conf.file_cache_size.toMebibytes();
    }

    public static int getNetworkingCacheSizeInMiB()
    {
        if (conf.networking_cache_size == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return 0;
        }
        return conf.networking_cache_size.toMebibytes();
    }

    public static boolean getFileCacheRoundUp()
    {
        if (conf.file_cache_round_up == null)
        {
            // In client mode the value is not set.
            assert DatabaseDescriptor.isClientInitialized();
            return false;
        }

        return conf.file_cache_round_up;
    }

    public static DiskOptimizationStrategy getDiskOptimizationStrategy()
    {
        return diskOptimizationStrategy;
    }

    public static double getDiskOptimizationEstimatePercentile()
    {
        return conf.disk_optimization_estimate_percentile;
    }

    public static long getTotalCommitlogSpaceInMiB()
    {
        return conf.commitlog_total_space.toMebibytes();
    }

    public static boolean shouldMigrateKeycacheOnCompaction()
    {
        return conf.key_cache_migrate_during_compaction;
    }

    public static void setMigrateKeycacheOnCompaction(boolean migrateCacheEntry)
    {
        conf.key_cache_migrate_during_compaction = migrateCacheEntry;
    }

    /** This method can return negative number for disabled */
    public static int getSSTablePreemptiveOpenIntervalInMiB()
    {
        if (conf.sstable_preemptive_open_interval == null)
            return -1;
        return conf.sstable_preemptive_open_interval.toMebibytes();
    }

    /** Negative number for disabled */
    public static void setSSTablePreemptiveOpenIntervalInMiB(int mib)
    {
        if (mib < 0)
            conf.sstable_preemptive_open_interval = null;
        else
            conf.sstable_preemptive_open_interval = new DataStorageSpec.IntMebibytesBound(mib);
    }

    public static boolean getTrickleFsync()
    {
        return conf.trickle_fsync;
    }

    public static int getTrickleFsyncIntervalInKiB()
    {
        return conf.trickle_fsync_interval.toKibibytes();
    }

    public static long getKeyCacheSizeInMiB()
    {
        return keyCacheSizeInMiB;
    }

    public static long getIndexSummaryCapacityInMiB()
    {
        return indexSummaryCapacityInMiB;
    }

    public static int getKeyCacheSavePeriod()
    {
        return conf.key_cache_save_period.toSeconds();
    }

    public static void setKeyCacheSavePeriod(int keyCacheSavePeriod)
    {
        conf.key_cache_save_period = new DurationSpec.IntSecondsBound(keyCacheSavePeriod);
    }

    public static int getKeyCacheKeysToSave()
    {
        return conf.key_cache_keys_to_save;
    }

    public static void setKeyCacheKeysToSave(int keyCacheKeysToSave)
    {
        conf.key_cache_keys_to_save = keyCacheKeysToSave;
    }

    public static String getRowCacheClassName()
    {
        return conf.row_cache_class_name;
    }

    public static long getRowCacheSizeInMiB()
    {
        return conf.row_cache_size.toMebibytes();
    }

    @VisibleForTesting
    public static void setRowCacheSizeInMiB(long val)
    {
        conf.row_cache_size = new DataStorageSpec.LongMebibytesBound(val);
    }

    public static int getRowCacheSavePeriod()
    {
        return conf.row_cache_save_period.toSeconds();
    }

    public static void setRowCacheSavePeriod(int rowCacheSavePeriod)
    {
        conf.row_cache_save_period = new DurationSpec.IntSecondsBound(rowCacheSavePeriod);
    }

    public static int getRowCacheKeysToSave()
    {
        return conf.row_cache_keys_to_save;
    }

    public static long getPaxosCacheSizeInMiB()
    {
        return paxosCacheSizeInMiB;
    }

    public static long getCounterCacheSizeInMiB()
    {
        return counterCacheSizeInMiB;
    }

    public static void setRowCacheKeysToSave(int rowCacheKeysToSave)
    {
        conf.row_cache_keys_to_save = rowCacheKeysToSave;
    }

    public static int getCounterCacheSavePeriod()
    {
        return conf.counter_cache_save_period.toSeconds();
    }

    public static void setCounterCacheSavePeriod(int counterCacheSavePeriod)
    {
        conf.counter_cache_save_period = new DurationSpec.IntSecondsBound(counterCacheSavePeriod);
    }

    public static int getCacheLoadTimeout()
    {
        return conf.cache_load_timeout.toSeconds();
    }

    @VisibleForTesting
    public static void setCacheLoadTimeout(int seconds)
    {
        conf.cache_load_timeout = new DurationSpec.IntSecondsBound(seconds);
    }

    public static int getCounterCacheKeysToSave()
    {
        return conf.counter_cache_keys_to_save;
    }

    public static void setCounterCacheKeysToSave(int counterCacheKeysToSave)
    {
        conf.counter_cache_keys_to_save = counterCacheKeysToSave;
    }

    public static int getStreamingKeepAlivePeriod()
    {
        return conf.streaming_keep_alive_period.toSeconds();
    }

    public static int getStreamingConnectionsPerHost()
    {
        return conf.streaming_connections_per_host;
    }

    public static boolean streamEntireSSTables()
    {
        return conf.stream_entire_sstables;
    }

    @VisibleForTesting
    public static boolean setStreamEntireSSTables(boolean value)
    {
        return conf.stream_entire_sstables = value;
    }

    public static DurationSpec.LongMillisecondsBound getStreamTransferTaskTimeout()
    {
        return conf.stream_transfer_task_timeout;
    }

    public static boolean getSkipStreamDiskSpaceCheck()
    {
        return conf.skip_stream_disk_space_check;
    }

    public static void setSkipStreamDiskSpaceCheck(boolean value)
    {
        conf.skip_stream_disk_space_check = value;
    }

    public static String getLocalDataCenter()
    {
        return localDC;
    }

    @VisibleForTesting
    public static void setLocalDataCenter(String value)
    {
        localDC = value;
    }

    public static Comparator<Replica> getLocalComparator()
    {
        return localComparator;
    }

    public static Config.InternodeCompression internodeCompression()
    {
        return conf.internode_compression;
    }

    public static void setInternodeCompression(Config.InternodeCompression compression)
    {
        conf.internode_compression = compression;
    }

    public static boolean getInterDCTcpNoDelay()
    {
        return conf.inter_dc_tcp_nodelay;
    }

    public static long getMemtableHeapSpaceInMiB()
    {
        return conf.memtable_heap_space.toMebibytes();
    }

    public static long getMemtableOffheapSpaceInMiB()
    {
        return conf.memtable_offheap_space.toMebibytes();
    }

    public static Config.MemtableAllocationType getMemtableAllocationType()
    {
        return conf.memtable_allocation_type;
    }

    public static int getRepairSessionMaxTreeDepth()
    {
        return conf.repair_session_max_tree_depth;
    }

    public static void setRepairSessionMaxTreeDepth(int depth)
    {
        if (depth < 10)
            throw new ConfigurationException("Cannot set repair_session_max_tree_depth to " + depth +
                                             " which is < 10, doing nothing");
        else if (depth > 20)
            logger.warn("repair_session_max_tree_depth of " + depth + " > 20 could lead to excessive memory usage");

        conf.repair_session_max_tree_depth = depth;
    }

    public static int getRepairSessionSpaceInMiB()
    {
        return conf.repair_session_space.toMebibytes();
    }

    public static void setRepairSessionSpaceInMiB(int sizeInMiB)
    {
        if (sizeInMiB < 1)
            throw new ConfigurationException("Cannot set repair_session_space to " + sizeInMiB +
                                             " < 1 mebibyte");
        else if (sizeInMiB > (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)))
            logger.warn("A repair_session_space of " + conf.repair_session_space +
                        " is likely to cause heap pressure.");

        conf.repair_session_space = new DataStorageSpec.IntMebibytesBound(sizeInMiB);
    }

    public static int getPaxosRepairParallelism()
    {
        return conf.paxos_repair_parallelism;
    }

    public static void setPaxosRepairParallelism(int v)
    {
        Preconditions.checkArgument(v > 0);
        conf.paxos_repair_parallelism = v;
    }

    public static Float getMemtableCleanupThreshold()
    {
        return conf.memtable_cleanup_threshold;
    }

    public static Map<String, InheritingClass> getMemtableConfigurations()
    {
        if (conf == null || conf.memtable == null)
            return null;
        return conf.memtable.configurations;
    }

    public static int getIndexSummaryResizeIntervalInMinutes()
    {
        if (conf.index_summary_resize_interval == null)
            return -1;

        return conf.index_summary_resize_interval.toMinutes();
    }

    public static void setIndexSummaryResizeIntervalInMinutes(int value)
    {
        if (value == -1)
            conf.index_summary_resize_interval = null;
        else
            conf.index_summary_resize_interval = new DurationSpec.IntMinutesBound(value);
    }

    public static boolean hasLargeAddressSpace()
    {
        // currently we just check if it's a 64bit arch, but any we only really care if the address space is large
        String datamodel = SUN_ARCH_DATA_MODEL.getString();
        if (datamodel != null)
        {
            switch (datamodel)
            {
                case "64": return true;
                case "32": return false;
            }
        }
        String arch = OS_ARCH.getString();
        return arch.contains("64") || arch.contains("sparcv9");
    }

    public static int getTracetypeRepairTTL()
    {
        return conf.trace_type_repair_ttl.toSeconds();
    }

    public static int getTracetypeQueryTTL()
    {
        return conf.trace_type_query_ttl.toSeconds();
    }

    public static long getPreparedStatementsCacheSizeMiB()
    {
        return preparedStatementsCacheSizeInMiB;
    }

    public static boolean enableUserDefinedFunctions()
    {
        return conf.user_defined_functions_enabled;
    }

    public static boolean enableScriptedUserDefinedFunctions()
    {
        return conf.scripted_user_defined_functions_enabled;
    }

    public static boolean enableUserDefinedFunctionsThreads()
    {
        return conf.user_defined_functions_threads_enabled;
    }

    public static long getUserDefinedFunctionWarnTimeout()
    {
        return conf.user_defined_functions_warn_timeout.toMilliseconds();
    }

    public static void setUserDefinedFunctionWarnTimeout(long userDefinedFunctionWarnTimeout)
    {
        conf.user_defined_functions_warn_timeout = new DurationSpec.LongMillisecondsBound(userDefinedFunctionWarnTimeout);
    }

    public static boolean allowInsecureUDFs()
    {
        return conf.allow_insecure_udfs;
    }

    public static boolean allowExtraInsecureUDFs()
    {
        return conf.allow_extra_insecure_udfs;
    }

    public static boolean getMaterializedViewsEnabled()
    {
        return conf.materialized_views_enabled;
    }

    public static void setMaterializedViewsEnabled(boolean enableMaterializedViews)
    {
        conf.materialized_views_enabled = enableMaterializedViews;
    }

    public static boolean getSASIIndexesEnabled()
    {
        return conf.sasi_indexes_enabled;
    }

    public static void setSASIIndexesEnabled(boolean enableSASIIndexes)
    {
        conf.sasi_indexes_enabled = enableSASIIndexes;
    }

    public static String getDefaultSecondaryIndex()
    {
        return conf.default_secondary_index;
    }

    public static void setDefaultSecondaryIndex(String name)
    {
        conf.default_secondary_index = name;
    }

    public static boolean getDefaultSecondaryIndexEnabled()
    {
        return conf.default_secondary_index_enabled;
    }

    public static void setDefaultSecondaryIndexEnabled(boolean enabled)
    {
        conf.default_secondary_index_enabled = enabled;
    }

    public static boolean isTransientReplicationEnabled()
    {
        return conf.transient_replication_enabled;
    }

    public static void setTransientReplicationEnabledUnsafe(boolean enabled)
    {
        conf.transient_replication_enabled = enabled;
    }

    public static boolean enableDropCompactStorage()
    {
        return conf.drop_compact_storage_enabled;
    }

    @VisibleForTesting
    public static void setEnableDropCompactStorage(boolean enableDropCompactStorage)
    {
        conf.drop_compact_storage_enabled = enableDropCompactStorage;
    }

    public static long getUserDefinedFunctionFailTimeout()
    {
        return conf.user_defined_functions_fail_timeout.toMilliseconds();
    }

    public static void setUserDefinedFunctionFailTimeout(long userDefinedFunctionFailTimeout)
    {
        conf.user_defined_functions_fail_timeout = new DurationSpec.LongMillisecondsBound(userDefinedFunctionFailTimeout);
    }

    public static Config.UserFunctionTimeoutPolicy getUserFunctionTimeoutPolicy()
    {
        return conf.user_function_timeout_policy;
    }

    public static void setUserFunctionTimeoutPolicy(Config.UserFunctionTimeoutPolicy userFunctionTimeoutPolicy)
    {
        conf.user_function_timeout_policy = userFunctionTimeoutPolicy;
    }

    public static long getGCLogThreshold()
    {
        return conf.gc_log_threshold.toMilliseconds();
    }

    public static void setGCLogThreshold(int gcLogThreshold)
    {
        conf.gc_log_threshold = new DurationSpec.IntMillisecondsBound(gcLogThreshold);
    }

    public static EncryptionContext getEncryptionContext()
    {
        return encryptionContext;
    }

    public static long getGCWarnThreshold()
    {
        return conf.gc_warn_threshold.toMilliseconds();
    }

    public static void setGCWarnThreshold(int threshold)
    {
        conf.gc_warn_threshold = new DurationSpec.IntMillisecondsBound(threshold);
    }

    public static boolean isCDCEnabled()
    {
        return conf.cdc_enabled;
    }

    @VisibleForTesting
    public static void setCDCEnabled(boolean cdc_enabled)
    {
        conf.cdc_enabled = cdc_enabled;
    }

    public static boolean getCDCBlockWrites()
    {
        return conf.cdc_block_writes;
    }

    public static void setCDCBlockWrites(boolean val)
    {
        conf.cdc_block_writes = val;
    }

    public static boolean isCDCOnRepairEnabled()
    {
        return conf.cdc_on_repair_enabled;
    }

    public static void setCDCOnRepairEnabled(boolean val)
    {
        conf.cdc_on_repair_enabled = val;
    }

    public static String getCDCLogLocation()
    {
        return conf.cdc_raw_directory;
    }

    public static long getCDCTotalSpace()
    {
        return conf.cdc_total_space.toBytesInLong();
    }

    @VisibleForTesting
    public static void setCDCTotalSpaceInMiB(int mibs)
    {
        conf.cdc_total_space = new DataStorageSpec.IntMebibytesBound(mibs);
    }

    public static int getCDCDiskCheckInterval()
    {
        return conf.cdc_free_space_check_interval.toMilliseconds();
    }

    @VisibleForTesting
    public static void setEncryptionContext(EncryptionContext ec)
    {
        encryptionContext = ec;
    }

    public static int searchConcurrencyFactor()
    {
        return searchConcurrencyFactor;
    }

    public static boolean isUnsafeSystem()
    {
        return unsafeSystem;
    }

    public static boolean diagnosticEventsEnabled()
    {
        return conf.diagnostic_events_enabled;
    }

    public static void setDiagnosticEventsEnabled(boolean enabled)
    {
        conf.diagnostic_events_enabled = enabled;
    }

    public static ConsistencyLevel getIdealConsistencyLevel()
    {
        return conf.ideal_consistency_level;
    }

    public static void setIdealConsistencyLevel(ConsistencyLevel cl)
    {
        conf.ideal_consistency_level = cl;
    }

    public static int getRepairCommandPoolSize()
    {
        return conf.repair_command_pool_size;
    }

    public static Config.RepairCommandPoolFullStrategy getRepairCommandPoolFullStrategy()
    {
        return conf.repair_command_pool_full_strategy;
    }

    public static FullQueryLoggerOptions getFullQueryLogOptions()
    {
        return  conf.full_query_logging_options;
    }

    public static void setFullQueryLogOptions(FullQueryLoggerOptions options)
    {
        conf.full_query_logging_options = options;
    }

    public static boolean getBlockForPeersInRemoteDatacenters()
    {
        return conf.block_for_peers_in_remote_dcs;
    }

    public static int getBlockForPeersTimeoutInSeconds()
    {
        return conf.block_for_peers_timeout_in_secs;
    }

    public static boolean automaticSSTableUpgrade()
    {
        return conf.automatic_sstable_upgrade;
    }

    public static void setAutomaticSSTableUpgradeEnabled(boolean enabled)
    {
        if (conf.automatic_sstable_upgrade != enabled)
            logger.debug("Changing automatic_sstable_upgrade to {}", enabled);
        conf.automatic_sstable_upgrade = enabled;
    }

    public static int maxConcurrentAutoUpgradeTasks()
    {
        return conf.max_concurrent_automatic_sstable_upgrades;
    }

    public static void setMaxConcurrentAutoUpgradeTasks(int value)
    {
        if (conf.max_concurrent_automatic_sstable_upgrades != value)
            logger.debug("Changing max_concurrent_automatic_sstable_upgrades to {}", value);
        validateMaxConcurrentAutoUpgradeTasksConf(value);
        conf.max_concurrent_automatic_sstable_upgrades = value;
    }

    private static void validateMaxConcurrentAutoUpgradeTasksConf(int value)
    {
        if (value < 0)
            throw new ConfigurationException("max_concurrent_automatic_sstable_upgrades can't be negative");
        if (value > getConcurrentCompactors())
            logger.warn("max_concurrent_automatic_sstable_upgrades ({}) is larger than concurrent_compactors ({})", value, getConcurrentCompactors());
    }

    public static AuditLogOptions getAuditLoggingOptions()
    {
        return conf.audit_logging_options;
    }

    public static void setAuditLoggingOptions(AuditLogOptions auditLoggingOptions)
    {
        conf.audit_logging_options = new AuditLogOptions.Builder(auditLoggingOptions).build();
    }

    public static Config.CorruptedTombstoneStrategy getCorruptedTombstoneStrategy()
    {
        return conf.corrupted_tombstone_strategy;
    }

    public static void setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy strategy)
    {
        conf.corrupted_tombstone_strategy = strategy;
    }

    public static boolean getRepairedDataTrackingForRangeReadsEnabled()
    {
        return conf.repaired_data_tracking_for_range_reads_enabled;
    }

    public static void setRepairedDataTrackingForRangeReadsEnabled(boolean enabled)
    {
        conf.repaired_data_tracking_for_range_reads_enabled = enabled;
    }

    public static boolean getRepairedDataTrackingForPartitionReadsEnabled()
    {
        return conf.repaired_data_tracking_for_partition_reads_enabled;
    }

    public static void setRepairedDataTrackingForPartitionReadsEnabled(boolean enabled)
    {
        conf.repaired_data_tracking_for_partition_reads_enabled = enabled;
    }

    public static boolean snapshotOnRepairedDataMismatch()
    {
        return conf.snapshot_on_repaired_data_mismatch;
    }

    public static void setSnapshotOnRepairedDataMismatch(boolean enabled)
    {
        conf.snapshot_on_repaired_data_mismatch = enabled;
    }

    public static boolean snapshotOnDuplicateRowDetection()
    {
        return conf.snapshot_on_duplicate_row_detection;
    }

    public static void setSnapshotOnDuplicateRowDetection(boolean enabled)
    {
        conf.snapshot_on_duplicate_row_detection = enabled;
    }

    public static boolean reportUnconfirmedRepairedDataMismatches()
    {
        return conf.report_unconfirmed_repaired_data_mismatches;
    }

    public static void reportUnconfirmedRepairedDataMismatches(boolean enabled)
    {
        conf.report_unconfirmed_repaired_data_mismatches = enabled;
    }

    public static boolean strictRuntimeChecks()
    {
        return strictRuntimeChecks;
    }

    public static boolean useOffheapMerkleTrees()
    {
        return conf.use_offheap_merkle_trees;
    }

    public static void useOffheapMerkleTrees(boolean value)
    {
        logger.info("Setting use_offheap_merkle_trees to {}", value);
        conf.use_offheap_merkle_trees = value;
    }

    public static Function<CommitLog, AbstractCommitLogSegmentManager> getCommitLogSegmentMgrProvider()
    {
        return commitLogSegmentMgrProvider;
    }

    public static void setCommitLogSegmentMgrProvider(Function<CommitLog, AbstractCommitLogSegmentManager> provider)
    {
        commitLogSegmentMgrProvider = provider;
    }

    private static DataStorageSpec.IntKibibytesBound createIntKibibyteBoundAndEnsureItIsValidForByteConversion(int kibibytes, String propertyName)
    {
        DataStorageSpec.IntKibibytesBound intKibibytesBound = new DataStorageSpec.IntKibibytesBound(kibibytes);
        checkValidForByteConversion(intKibibytesBound, propertyName);
        return intKibibytesBound;
    }

    /**
     * Ensures passed in configuration value is positive and will not overflow when converted to Bytes
     */
    private static void checkValidForByteConversion(final DataStorageSpec.IntKibibytesBound value, String name)
    {
        long valueInBytes = value.toBytesInLong();
        if (valueInBytes < 0 || valueInBytes > Integer.MAX_VALUE - 1)
        {
            throw new ConfigurationException(String.format("%s must be positive value <= %dB, but was %dB",
                                                           name,
                                                           Integer.MAX_VALUE - 1,
                                                           valueInBytes),
                                             false);
        }
    }

    public static int getValidationPreviewPurgeHeadStartInSec()
    {
        return conf.validation_preview_purge_head_start.toSeconds();
    }

    public static boolean checkForDuplicateRowsDuringReads()
    {
        return conf.check_for_duplicate_rows_during_reads;
    }

    public static void setCheckForDuplicateRowsDuringReads(boolean enabled)
    {
        conf.check_for_duplicate_rows_during_reads = enabled;
    }

    public static boolean checkForDuplicateRowsDuringCompaction()
    {
        return conf.check_for_duplicate_rows_during_compaction;
    }

    public static void setCheckForDuplicateRowsDuringCompaction(boolean enabled)
    {
        conf.check_for_duplicate_rows_during_compaction = enabled;
    }

    public static int getRepairPendingCompactionRejectThreshold()
    {
        return conf.reject_repair_compaction_threshold;
    }

    public static void setRepairPendingCompactionRejectThreshold(int value)
    {
        conf.reject_repair_compaction_threshold = value;
    }

    public static int getInitialRangeTombstoneListAllocationSize()
    {
        return conf.initial_range_tombstone_list_allocation_size;
    }

    public static void setInitialRangeTombstoneListAllocationSize(int size)
    {
        conf.initial_range_tombstone_list_allocation_size = size;
    }

    public static double getRangeTombstoneListGrowthFactor()
    {
        return conf.range_tombstone_list_growth_factor;
    }

    public static void setRangeTombstoneListGrowthFactor(double resizeFactor)
    {
        conf.range_tombstone_list_growth_factor = resizeFactor;
    }

    public static boolean getAutocompactionOnStartupEnabled()
    {
        return conf.autocompaction_on_startup_enabled;
    }

    public static boolean autoOptimiseIncRepairStreams()
    {
        return conf.auto_optimise_inc_repair_streams;
    }

    public static void setAutoOptimiseIncRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_inc_repair_streams)
            logger.info("Changing auto_optimise_inc_repair_streams from {} to {}", conf.auto_optimise_inc_repair_streams, enabled);
        conf.auto_optimise_inc_repair_streams = enabled;
    }

    public static boolean autoOptimiseFullRepairStreams()
    {
        return conf.auto_optimise_full_repair_streams;
    }

    public static void setAutoOptimiseFullRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_full_repair_streams)
            logger.info("Changing auto_optimise_full_repair_streams from {} to {}", conf.auto_optimise_full_repair_streams, enabled);
        conf.auto_optimise_full_repair_streams = enabled;
    }

    public static boolean autoOptimisePreviewRepairStreams()
    {
        return conf.auto_optimise_preview_repair_streams;
    }

    public static void setAutoOptimisePreviewRepairStreams(boolean enabled)
    {
        if (enabled != conf.auto_optimise_preview_repair_streams)
            logger.info("Changing auto_optimise_preview_repair_streams from {} to {}", conf.auto_optimise_preview_repair_streams, enabled);
        conf.auto_optimise_preview_repair_streams = enabled;
    }

    /** @deprecated See CASSANDRA-17195 */
    @Deprecated(since = "4.1") // this warning threshold will be replaced by an equivalent guardrail
    public static ConsistencyLevel getAuthWriteConsistencyLevel()
    {
        return ConsistencyLevel.valueOf(conf.auth_write_consistency_level);
    }

    public static ConsistencyLevel getAuthReadConsistencyLevel()
    {
        return ConsistencyLevel.valueOf(conf.auth_read_consistency_level);
    }

    public static void setAuthWriteConsistencyLevel(ConsistencyLevel cl)
    {
        conf.auth_write_consistency_level = cl.toString();
    }

    public static void setAuthReadConsistencyLevel(ConsistencyLevel cl)
    {
        conf.auth_read_consistency_level = cl.toString();
    }

    public static int getConsecutiveMessageErrorsThreshold()
    {
        return conf.consecutive_message_errors_threshold;
    }

    public static void setConsecutiveMessageErrorsThreshold(int value)
    {
        conf.consecutive_message_errors_threshold = value;
    }

    public static boolean getPartitionDenylistEnabled()
    {
        return conf.partition_denylist_enabled;
    }

    public static void setPartitionDenylistEnabled(boolean enabled)
    {
        conf.partition_denylist_enabled = enabled;
    }

    public static boolean getDenylistWritesEnabled()
    {
        return conf.denylist_writes_enabled;
    }

    public static void setDenylistWritesEnabled(boolean enabled)
    {
        conf.denylist_writes_enabled = enabled;
    }

    public static boolean getDenylistReadsEnabled()
    {
        return conf.denylist_reads_enabled;
    }

    public static void setDenylistReadsEnabled(boolean enabled)
    {
        conf.denylist_reads_enabled = enabled;
    }

    public static boolean getDenylistRangeReadsEnabled()
    {
        return conf.denylist_range_reads_enabled;
    }

    public static void setDenylistRangeReadsEnabled(boolean enabled)
    {
        conf.denylist_range_reads_enabled = enabled;
    }

    public static int getDenylistRefreshSeconds()
    {
        return conf.denylist_refresh.toSeconds();
    }

    public static void setDenylistRefreshSeconds(int seconds)
    {
        if (seconds <= 0)
            throw new IllegalArgumentException("denylist_refresh must be a positive integer.");

        conf.denylist_refresh = new DurationSpec.IntSecondsBound(seconds);
    }

    public static int getDenylistInitialLoadRetrySeconds()
    {
        return conf.denylist_initial_load_retry.toSeconds();
    }

    public static void setDenylistInitialLoadRetrySeconds(int seconds)
    {
        if (seconds <= 0)
            throw new IllegalArgumentException("denylist_initial_load_retry must be a positive integer.");

        conf.denylist_initial_load_retry = new DurationSpec.IntSecondsBound(seconds);
    }

    public static ConsistencyLevel getDenylistConsistencyLevel()
    {
        return conf.denylist_consistency_level;
    }

    public static void setDenylistConsistencyLevel(ConsistencyLevel cl)
    {
        conf.denylist_consistency_level = cl;
    }

    public static int getDenylistMaxKeysPerTable()
    {
        return conf.denylist_max_keys_per_table;
    }

    public static void setDenylistMaxKeysPerTable(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("denylist_max_keys_per_table must be a positive integer.");
        conf.denylist_max_keys_per_table = value;
    }

    public static int getDenylistMaxKeysTotal()
    {
        return conf.denylist_max_keys_total;
    }

    public static void setDenylistMaxKeysTotal(int value)
    {
        if (value <= 0)
            throw new IllegalArgumentException("denylist_max_keys_total must be a positive integer.");
        conf.denylist_max_keys_total = value;
    }

    public static boolean getAuthCacheWarmingEnabled()
    {
        return conf.auth_cache_warming_enabled;
    }

    public static SubnetGroups getClientErrorReportingExclusions()
    {
        return conf.client_error_reporting_exclusions;
    }

    public static SubnetGroups getInternodeErrorReportingExclusions()
    {
        return conf.internode_error_reporting_exclusions;
    }

    public static boolean getReadThresholdsEnabled()
    {
        return conf.read_thresholds_enabled;
    }

    public static void setReadThresholdsEnabled(boolean value)
    {
        if (conf.read_thresholds_enabled != value)
        {
            conf.read_thresholds_enabled = value;
            logger.info("updated read_thresholds_enabled to {}", value);
        }
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getCoordinatorReadSizeWarnThreshold()
    {
        return conf.coordinator_read_size_warn_threshold;
    }

    public static void setCoordinatorReadSizeWarnThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  coordinator_read_size_warn_threshold to {}", value);
        conf.coordinator_read_size_warn_threshold = value;
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getCoordinatorReadSizeFailThreshold()
    {
        return conf.coordinator_read_size_fail_threshold;
    }

    public static void setCoordinatorReadSizeFailThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  coordinator_read_size_fail_threshold to {}", value);
        conf.coordinator_read_size_fail_threshold = value;
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getLocalReadSizeWarnThreshold()
    {
        return conf.local_read_size_warn_threshold;
    }

    public static void setLocalReadSizeWarnThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  local_read_size_warn_threshold to {}", value);
        conf.local_read_size_warn_threshold = value;
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getLocalReadSizeFailThreshold()
    {
        return conf.local_read_size_fail_threshold;
    }

    public static void setLocalReadSizeFailThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  local_read_size_fail_threshold to {}", value);
        conf.local_read_size_fail_threshold = value;
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getRowIndexReadSizeWarnThreshold()
    {
        return conf.row_index_read_size_warn_threshold;
    }

    public static void setRowIndexReadSizeWarnThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  row_index_size_warn_threshold to {}", value);
        conf.row_index_read_size_warn_threshold = value;
    }

    @Nullable
    public static DataStorageSpec.LongBytesBound getRowIndexReadSizeFailThreshold()
    {
        return conf.row_index_read_size_fail_threshold;
    }

    public static void setRowIndexReadSizeFailThreshold(@Nullable DataStorageSpec.LongBytesBound value)
    {
        logger.info("updating  row_index_read_size_fail_threshold to {}", value);
        conf.row_index_read_size_fail_threshold = value;
    }

    public static int getDefaultKeyspaceRF() { return conf.default_keyspace_rf; }

    public static void setDefaultKeyspaceRF(int value) throws IllegalArgumentException
    {
        if (value < 1)
        {
            throw new IllegalArgumentException("default_keyspace_rf cannot be less than 1");
        }

        if (value < guardrails.getMinimumReplicationFactorFailThreshold())
        {
            throw new IllegalArgumentException(String.format("default_keyspace_rf to be set (%d) cannot be less than minimum_replication_factor_fail_threshold (%d)", value, guardrails.getMinimumReplicationFactorFailThreshold()));
        }

        if (guardrails.getMaximumReplicationFactorFailThreshold() != -1 && value > guardrails.getMaximumReplicationFactorFailThreshold())
        {
            throw new IllegalArgumentException(String.format("default_keyspace_rf to be set (%d) cannot be greater than maximum_replication_factor_fail_threshold (%d)", value, guardrails.getMaximumReplicationFactorFailThreshold()));
        }

        conf.default_keyspace_rf = value;
    }


    public static boolean getUseStatementsEnabled()
    {
        return conf.use_statements_enabled;
    }

    public static void setUseStatementsEnabled(boolean enabled)
    {
        if (enabled != conf.use_statements_enabled)
        {
            logger.info("Setting use_statements_enabled to {}", enabled);
            conf.use_statements_enabled = enabled;
        }
    }

    public static boolean getForceNewPreparedStatementBehaviour()
    {
        return conf.force_new_prepared_statement_behaviour;
    }

    public static void setForceNewPreparedStatementBehaviour(boolean value)
    {
        if (value != conf.force_new_prepared_statement_behaviour)
        {
            logger.info("Setting force_new_prepared_statement_behaviour to {}", value);
            conf.force_new_prepared_statement_behaviour = value;
        }
    }

    public static DurationSpec.LongNanosecondsBound getStreamingStateExpires()
    {
        return conf.streaming_state_expires;
    }

    public static void setStreamingStateExpires(DurationSpec.LongNanosecondsBound duration)
    {
        if (!conf.streaming_state_expires.equals(Objects.requireNonNull(duration, "duration")))
        {
            logger.info("Setting streaming_state_expires to {}", duration);
            conf.streaming_state_expires = duration;
        }
    }

    public static DataStorageSpec.LongBytesBound getStreamingStateSize()
    {
        return conf.streaming_state_size;
    }

    public static void setStreamingStateSize(DataStorageSpec.LongBytesBound duration)
    {
        if (!conf.streaming_state_size.equals(Objects.requireNonNull(duration, "duration")))
        {
            logger.info("Setting streaming_state_size to {}", duration);
            conf.streaming_state_size = duration;
        }
    }

    public static boolean getStreamingStatsEnabled()
    {
        return conf.streaming_stats_enabled;
    }

    public static void setStreamingStatsEnabled(boolean streamingStatsEnabled)
    {
        if (conf.streaming_stats_enabled != streamingStatsEnabled)
        {
            logger.info("Setting streaming_stats_enabled to {}", streamingStatsEnabled);
            conf.streaming_stats_enabled = streamingStatsEnabled;
        }
    }

    public static DurationSpec.IntSecondsBound getStreamingSlowEventsLogTimeout() {
        return conf.streaming_slow_events_log_timeout;
    }

    public static void setStreamingSlowEventsLogTimeout(String value) {
        DurationSpec.IntSecondsBound next = new DurationSpec.IntSecondsBound(value);
        if (!conf.streaming_slow_events_log_timeout.equals(next))
        {
            logger.info("Setting streaming_slow_events_log to " + value);
            conf.streaming_slow_events_log_timeout = next;
        }
    }

    public static boolean isUUIDSSTableIdentifiersEnabled()
    {
        return conf.uuid_sstable_identifiers_enabled;
    }

    public static DurationSpec.LongNanosecondsBound getRepairStateExpires()
    {
        return conf.repair_state_expires;
    }

    public static void setRepairStateExpires(DurationSpec.LongNanosecondsBound duration)
    {
        if (!conf.repair_state_expires.equals(Objects.requireNonNull(duration, "duration")))
        {
            logger.info("Setting repair_state_expires to {}", duration);
            conf.repair_state_expires = duration;
        }
    }

    public static int getRepairStateSize()
    {
        return conf.repair_state_size;
    }

    public static void setRepairStateSize(int size)
    {
        if (conf.repair_state_size != size)
        {
            logger.info("Setting repair_state_size to {}", size);
            conf.repair_state_size = size;
        }
    }

    public static boolean topPartitionsEnabled()
    {
        return conf.top_partitions_enabled;
    }

    public static int getMaxTopSizePartitionCount()
    {
        return conf.max_top_size_partition_count;
    }

    public static void setMaxTopSizePartitionCount(int value)
    {
        conf.max_top_size_partition_count = value;
    }

    public static int getMaxTopTombstonePartitionCount()
    {
        return conf.max_top_tombstone_partition_count;
    }

    public static void setMaxTopTombstonePartitionCount(int value)
    {
        conf.max_top_tombstone_partition_count = value;
    }

    public static DataStorageSpec.LongBytesBound getMinTrackedPartitionSizeInBytes()
    {
        return conf.min_tracked_partition_size;
    }

    public static void setMinTrackedPartitionSizeInBytes(DataStorageSpec.LongBytesBound spec)
    {
        conf.min_tracked_partition_size = spec;
    }

    public static long getMinTrackedPartitionTombstoneCount()
    {
        return conf.min_tracked_partition_tombstone_count;
    }

    public static void setMinTrackedPartitionTombstoneCount(long value)
    {
        conf.min_tracked_partition_tombstone_count = value;
    }

    public static boolean getDumpHeapOnUncaughtException()
    {
        return conf.dump_heap_on_uncaught_exception;
    }

    /**
     * @return Whether the path exists (be it created now or already prior)
     */
    private static boolean maybeCreateHeapDumpPath()
    {
        if (!conf.dump_heap_on_uncaught_exception)
            return false;

        Path heap_dump_path = getHeapDumpPath();
        if (heap_dump_path == null)
        {
            logger.warn("Neither -XX:HeapDumpPath nor cassandra.yaml:heap_dump_path are set; unable to create a directory to hold the output.");
            return false;
        }
        if (PathUtils.exists(File.getPath(conf.heap_dump_path)))
            return true;
        return PathUtils.createDirectoryIfNotExists(File.getPath(conf.heap_dump_path));
    }

    /**
     * As this is at its heart a debug operation (getting a one-shot heapdump from an uncaught exception), we support
     * both the more evolved cassandra.yaml approach but also the -XX param to override it on a one-off basis so you don't
     * have to change the full config of a node or a cluster in order to get a heap dump from a single node that's
     * misbehaving.
     * @return the absolute path of the -XX param if provided, else the heap_dump_path in cassandra.yaml
     */
    public static Path getHeapDumpPath()
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        Optional<String> pathArg = runtimeMxBean.getInputArguments().stream().filter(s -> s.startsWith("-XX:HeapDumpPath=")).findFirst();

        if (pathArg.isPresent())
        {
            Pattern HEAP_DUMP_PATH_SPLITTER = Pattern.compile("HeapDumpPath=");
            String fullHeapPathString = HEAP_DUMP_PATH_SPLITTER.split(pathArg.get())[1];
            Path absolutePath = File.getPath(fullHeapPathString).toAbsolutePath();
            Path basePath = fullHeapPathString.endsWith(".hprof") ? absolutePath.subpath(0, absolutePath.getNameCount() - 1) : absolutePath;
            return File.getPath("/").resolve(basePath);
        }
        if (conf.heap_dump_path == null)
            throw new ConfigurationException("Attempted to get heap dump path without -XX:HeapDumpPath or cassandra.yaml:heap_dump_path set.");
        return File.getPath(conf.heap_dump_path);
    }

    public static void setDumpHeapOnUncaughtException(boolean enabled)
    {
        conf.dump_heap_on_uncaught_exception = enabled;
        boolean pathExists = maybeCreateHeapDumpPath();

        if (enabled && !pathExists)
        {
            logger.error("Attempted to enable heap dump but cannot create the requested path. Disabling.");
            conf.dump_heap_on_uncaught_exception = false;
        }
        else
            logger.info("Setting dump_heap_on_uncaught_exception to {}", enabled);
    }

    public static boolean getSStableReadRatePersistenceEnabled()
    {
        return conf.sstable_read_rate_persistence_enabled;
    }

    public static void setSStableReadRatePersistenceEnabled(boolean enabled)
    {
        if (enabled != conf.sstable_read_rate_persistence_enabled)
        {
            logger.info("Setting sstable_read_rate_persistence_enabled to {}", enabled);
            conf.sstable_read_rate_persistence_enabled = enabled;
        }
    }

    public static boolean getClientRequestSizeMetricsEnabled()
    {
        return conf.client_request_size_metrics_enabled;
    }

    public static void setClientRequestSizeMetricsEnabled(boolean enabled)
    {
        conf.client_request_size_metrics_enabled = enabled;
    }

    @VisibleForTesting
    public static void resetSSTableFormats(Iterable<SSTableFormat.Factory> factories, Config.SSTableConfig config)
    {
        sstableFormats = null;
        selectedSSTableFormat = null;
        applySSTableFormats(factories, config);
    }

    public static ImmutableMap<String, SSTableFormat<?, ?>> getSSTableFormats()
    {
        return Objects.requireNonNull(sstableFormats, "Forgot to initialize DatabaseDescriptor?");
    }

    public static SSTableFormat<?, ?> getSelectedSSTableFormat()
    {
        return Objects.requireNonNull(selectedSSTableFormat, "Forgot to initialize DatabaseDescriptor?");
    }

    @VisibleForTesting
    public static void setSelectedSSTableFormat(SSTableFormat<?, ?> format)
    {
        selectedSSTableFormat = Objects.requireNonNull(format);
    }

    public static boolean getDynamicDataMaskingEnabled()
    {
        return conf.dynamic_data_masking_enabled;
    }

    public static void setDynamicDataMaskingEnabled(boolean enabled)
    {
        if (enabled != conf.dynamic_data_masking_enabled)
        {
            logger.info("Setting dynamic_data_masking_enabled to {}", enabled);
            conf.dynamic_data_masking_enabled = enabled;
        }
    }

    public static OptionalDouble getSeverityDuringDecommission()
    {
        return conf.severity_during_decommission > 0 ?
               OptionalDouble.of(conf.severity_during_decommission) :
               OptionalDouble.empty();
    }

    public static StorageCompatibilityMode getStorageCompatibilityMode()
    {
        // Config is null for junits that don't load the config. Get from env var that CI/build.xml sets
        if (conf == null)
            return CassandraRelevantProperties.JUNIT_STORAGE_COMPATIBILITY_MODE.getEnum(StorageCompatibilityMode.CASSANDRA_4);
        else
            return conf.storage_compatibility_mode;
    }

    public static ParameterizedClass getDefaultCompaction()
    {
        return conf != null ? conf.default_compaction : null;
    }

    public static DataStorageSpec.IntMebibytesBound getSAISegmentWriteBufferSpace()
    {
        return conf.sai_options.segment_write_buffer_size;
    }

    public static RepairRetrySpec getRepairRetrySpec()
    {
        return conf == null ? new RepairRetrySpec() : conf.repair.retries;
    }
}
