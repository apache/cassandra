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

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.scheduler.NoScheduler;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.*;
import org.apache.commons.lang3.StringUtils;

public class DatabaseDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    /**
     * Tokens are serialized in a Gossip VersionedValue String.  VV are restricted to 64KB
     * when we send them over the wire, which works out to about 1700 tokens.
     */
    private static final int MAX_NUM_TOKENS = 1536;

    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress rpcAddress;
    private static InetAddress broadcastRpcAddress;
    private static SeedProvider seedProvider;
    private static IInternodeAuthenticator internodeAuthenticator;

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;
    private static String paritionerName;

    private static Config.DiskAccessMode indexAccessMode;

    private static Config conf;

    private static SSTableFormat.Type sstable_format = SSTableFormat.Type.BIG;

    private static IAuthenticator authenticator = new AllowAllAuthenticator();
    private static IAuthorizer authorizer = new AllowAllAuthorizer();
    // Don't initialize the role manager until applying config. The options supported by CassandraRoleManager
    // depend on the configured IAuthenticator, so defer creating it until that's been set.
    private static IRoleManager roleManager;

    private static IRequestScheduler requestScheduler;
    private static RequestSchedulerId requestSchedulerId;
    private static RequestSchedulerOptions requestSchedulerOptions;

    private static long keyCacheSizeInMB;
    private static long counterCacheSizeInMB;
    private static long indexSummaryCapacityInMB;

    private static String localDC;
    private static Comparator<InetAddress> localComparator;
    private static EncryptionContext encryptionContext;
    private static boolean hasLoggedConfig;

    public static void forceStaticInitialization() {}
    static
    {
        // In client mode, we use a default configuration. Note that the fields of this class will be
        // left unconfigured however (the partitioner or localDC will be null for instance) so this
        // should be used with care.
        try
        {
            if (Config.isClientMode())
            {
                conf = new Config();
            }
            else
            {
                applyConfig(loadConfig());
            }
        }
        catch (Exception e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    @VisibleForTesting
    public static Config loadConfig() throws ConfigurationException
    {
        String loaderClass = System.getProperty("cassandra.config.loader");
        ConfigurationLoader loader = loaderClass == null
                                   ? new YamlConfigurationLoader()
                                   : FBUtilities.<ConfigurationLoader>construct(loaderClass, "configuration loading");
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
    static void applyAddressConfig(Config config) throws ConfigurationException
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
                throw new ConfigurationException("Unknown listen_address '" + config.listen_address + "'", false);
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
                throw new ConfigurationException("Unknown broadcast_address '" + config.broadcast_address + "'", false);
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
            rpcAddress = FBUtilities.getLocalAddress();
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
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + config.broadcast_rpc_address + "'", false);
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_rpc_address cannot be a wildcard address (" + config.broadcast_rpc_address + ")!", false);
        }
        else
        {
            if (rpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("If rpc_address is set to a wildcard address (" + config.rpc_address + "), then " +
                                                 "you must set broadcast_rpc_address to a value other than " + config.rpc_address, false);
            broadcastRpcAddress = rpcAddress;
        }
    }

    public static void applyConfig(Config config) throws ConfigurationException
    {
        conf = config;

        if (conf.commitlog_sync == null)
        {
            throw new ConfigurationException("Missing required directive CommitLogSync", false);
        }

        if (conf.commitlog_sync == Config.CommitLogSync.batch)
        {
            if (conf.commitlog_sync_batch_window_in_ms == null)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_batch_window_in_ms: Double expected.", false);
            }
            else if (conf.commitlog_sync_period_in_ms != null)
            {
                throw new ConfigurationException("Batch sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_batch_window_in_ms when using batch sync", false);
            }
            logger.debug("Syncing log with a batch window of {}", conf.commitlog_sync_batch_window_in_ms);
        }
        else
        {
            if (conf.commitlog_sync_period_in_ms == null)
            {
                throw new ConfigurationException("Missing value for commitlog_sync_period_in_ms: Integer expected", false);
            }
            else if (conf.commitlog_sync_batch_window_in_ms != null)
            {
                throw new ConfigurationException("commitlog_sync_period_in_ms specified, but commitlog_sync_batch_window_in_ms found.  Only specify commitlog_sync_period_in_ms when using periodic sync.", false);
            }
            logger.debug("Syncing log with a period of {}", conf.commitlog_sync_period_in_ms);
        }

        /* evaluate the DiskAccessMode Config directive, which also affects indexAccessMode selection */
        if (conf.disk_access_mode == Config.DiskAccessMode.auto)
        {
            conf.disk_access_mode = hasLargeAddressSpace() ? Config.DiskAccessMode.mmap : Config.DiskAccessMode.standard;
            indexAccessMode = conf.disk_access_mode;
            logger.info("DiskAccessMode 'auto' determined to be {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }
        else if (conf.disk_access_mode == Config.DiskAccessMode.mmap_index_only)
        {
            conf.disk_access_mode = Config.DiskAccessMode.standard;
            indexAccessMode = Config.DiskAccessMode.mmap;
            logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }
        else
        {
            indexAccessMode = conf.disk_access_mode;
            logger.info("DiskAccessMode is {}, indexAccessMode is {}", conf.disk_access_mode, indexAccessMode);
        }

        /* Authentication, authorization and role management backend, implementing IAuthenticator, IAuthorizer & IRoleMapper*/
        if (conf.authenticator != null)
            authenticator = FBUtilities.newAuthenticator(conf.authenticator);

        // the configuration options regarding credentials caching are only guaranteed to
        // work with PasswordAuthenticator, so log a message if some other authenticator
        // is in use and non-default values are detected
        if (!(authenticator instanceof PasswordAuthenticator)
            && (conf.credentials_update_interval_in_ms != -1
                || conf.credentials_validity_in_ms != 2000
                || conf.credentials_cache_max_entries != 1000))
        {
            logger.info("Configuration options credentials_update_interval_in_ms, credentials_validity_in_ms and " +
                        "credentials_cache_max_entries may not be applicable for the configured authenticator ({})",
                        authenticator.getClass().getName());
        }

        if (conf.authorizer != null)
            authorizer = FBUtilities.newAuthorizer(conf.authorizer);

        if (!authenticator.requireAuthentication() && authorizer.requireAuthorization())
            throw new ConfigurationException(conf.authenticator + " can't be used with " +  conf.authorizer, false);

        if (conf.role_manager != null)
            roleManager = FBUtilities.newRoleManager(conf.role_manager);
        else
            roleManager = new CassandraRoleManager();

        if (authenticator instanceof PasswordAuthenticator && !(roleManager instanceof CassandraRoleManager))
            throw new ConfigurationException("CassandraRoleManager must be used with PasswordAuthenticator", false);

        if (conf.internode_authenticator != null)
            internodeAuthenticator = FBUtilities.construct(conf.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        roleManager.validateConfiguration();
        internodeAuthenticator.validateConfiguration();

        /* Hashing strategy */
        if (conf.partitioner == null)
        {
            throw new ConfigurationException("Missing directive: partitioner", false);
        }
        try
        {
            partitioner = FBUtilities.newPartitioner(System.getProperty("cassandra.partitioner", conf.partitioner));
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid partitioner class " + conf.partitioner, false);
        }
        paritionerName = partitioner.getClass().getCanonicalName();

        if (conf.gc_warn_threshold_in_ms < 0)
        {
            throw new ConfigurationException("gc_warn_threshold_in_ms must be a positive integer");
        }

        if (conf.max_hint_window_in_ms == null)
        {
            throw new ConfigurationException("max_hint_window_in_ms cannot be set to null", false);
        }

        /* phi convict threshold for FailureDetector */
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16)
        {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16, but was " + conf.phi_convict_threshold, false);
        }

        /* Thread per pool */
        if (conf.concurrent_reads != null && conf.concurrent_reads < 2)
        {
            throw new ConfigurationException("concurrent_reads must be at least 2, but was " + conf.concurrent_reads, false);
        }

        if (conf.concurrent_writes != null && conf.concurrent_writes < 2)
        {
            throw new ConfigurationException("concurrent_writes must be at least 2, but was " + conf.concurrent_writes, false);
        }

        if (conf.concurrent_counter_writes != null && conf.concurrent_counter_writes < 2)
            throw new ConfigurationException("concurrent_counter_writes must be at least 2, but was " + conf.concurrent_counter_writes, false);

        if (conf.concurrent_replicates != null)
            logger.warn("concurrent_replicates has been deprecated and should be removed from cassandra.yaml");

        if (conf.file_cache_size_in_mb == null)
            conf.file_cache_size_in_mb = Math.min(512, (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576)));

        if (conf.memtable_offheap_space_in_mb == null)
            conf.memtable_offheap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_offheap_space_in_mb < 0)
            throw new ConfigurationException("memtable_offheap_space_in_mb must be positive, but was " + conf.memtable_offheap_space_in_mb, false);
        // for the moment, we default to twice as much on-heap space as off-heap, as heap overhead is very large
        if (conf.memtable_heap_space_in_mb == null)
            conf.memtable_heap_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (4 * 1048576));
        if (conf.memtable_heap_space_in_mb <= 0)
            throw new ConfigurationException("memtable_heap_space_in_mb must be positive, but was " + conf.memtable_heap_space_in_mb, false);
        logger.info("Global memtable on-heap threshold is enabled at {}MB", conf.memtable_heap_space_in_mb);
        if (conf.memtable_offheap_space_in_mb == 0)
            logger.info("Global memtable off-heap threshold is disabled, HeapAllocator will be used instead");
        else
            logger.info("Global memtable off-heap threshold is enabled at {}MB", conf.memtable_offheap_space_in_mb);

        applyAddressConfig(config);

        if (conf.thrift_framed_transport_size_in_mb <= 0)
            throw new ConfigurationException("thrift_framed_transport_size_in_mb must be positive, but was " + conf.thrift_framed_transport_size_in_mb, false);

        if (conf.native_transport_max_frame_size_in_mb <= 0)
            throw new ConfigurationException("native_transport_max_frame_size_in_mb must be positive, but was " + conf.native_transport_max_frame_size_in_mb, false);

        // fail early instead of OOMing (see CASSANDRA-8116)
        if (ThriftServer.HSHA.equals(conf.rpc_server_type) && conf.rpc_max_threads == Integer.MAX_VALUE)
            throw new ConfigurationException("The hsha rpc_server_type is not compatible with an rpc_max_threads " +
                                             "setting of 'unlimited'.  Please see the comments in cassandra.yaml " +
                                             "for rpc_server_type and rpc_max_threads.",
                                             false);
        if (ThriftServer.HSHA.equals(conf.rpc_server_type) && conf.rpc_max_threads > (FBUtilities.getAvailableProcessors() * 2 + 1024))
            logger.warn("rpc_max_threads setting of {} may be too high for the hsha server and cause unnecessary thread contention, reducing performance", conf.rpc_max_threads);

        /* end point snitch */
        if (conf.endpoint_snitch == null)
        {
            throw new ConfigurationException("Missing endpoint_snitch directive", false);
        }
        snitch = createEndpointSnitch(conf.endpoint_snitch);
        EndpointSnitchInfo.create();

        localDC = snitch.getDatacenter(FBUtilities.getBroadcastAddress());
        localComparator = new Comparator<InetAddress>()
        {
            public int compare(InetAddress endpoint1, InetAddress endpoint2)
            {
                boolean local1 = localDC.equals(snitch.getDatacenter(endpoint1));
                boolean local2 = localDC.equals(snitch.getDatacenter(endpoint2));
                if (local1 && !local2)
                    return -1;
                if (local2 && !local1)
                    return 1;
                return 0;
            }
        };

        /* Request Scheduler setup */
        requestSchedulerOptions = conf.request_scheduler_options;
        if (conf.request_scheduler != null)
        {
            try
            {
                if (requestSchedulerOptions == null)
                {
                    requestSchedulerOptions = new RequestSchedulerOptions();
                }
                Class<?> cls = Class.forName(conf.request_scheduler);
                requestScheduler = (IRequestScheduler) cls.getConstructor(RequestSchedulerOptions.class).newInstance(requestSchedulerOptions);
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException("Invalid Request Scheduler class " + conf.request_scheduler, false);
            }
            catch (Exception e)
            {
                throw new ConfigurationException("Unable to instantiate request scheduler", e);
            }
        }
        else
        {
            requestScheduler = new NoScheduler();
        }

        if (conf.request_scheduler_id == RequestSchedulerId.keyspace)
        {
            requestSchedulerId = conf.request_scheduler_id;
        }
        else
        {
            // Default to Keyspace
            requestSchedulerId = RequestSchedulerId.keyspace;
        }

        // if data dirs, commitlog dir, or saved caches dir are set in cassandra.yaml, use that.  Otherwise,
        // use -Dcassandra.storagedir (set in cassandra-env.sh) as the parent dir for data/, commitlog/, and saved_caches/
        if (conf.commitlog_directory == null)
        {
            conf.commitlog_directory = System.getProperty("cassandra.storagedir", null);
            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory is missing and -Dcassandra.storagedir is not set", false);
            conf.commitlog_directory += File.separator + "commitlog";
        }

        if (conf.hints_directory == null)
        {
            conf.hints_directory = System.getProperty("cassandra.storagedir", null);
            if (conf.hints_directory == null)
                throw new ConfigurationException("hints_directory is missing and -Dcassandra.storagedir is not set", false);
            conf.hints_directory += File.separator + "hints";
        }

        if (conf.commitlog_total_space_in_mb == null)
        {
            int preferredSize = 8192;
            int minSize = 0;
            try
            {
                // use 1/4 of available space.  See discussion on #10013 and #10199
                minSize = Ints.checkedCast((guessFileStore(conf.commitlog_directory).getTotalSpace() / 1048576) / 4);
            }
            catch (IOException e)
            {
                logger.debug("Error checking disk space", e);
                throw new ConfigurationException(String.format("Unable to check disk space available to %s. Perhaps the Cassandra user does not have the necessary permissions",
                                                               conf.commitlog_directory), e);
            }
            if (minSize < preferredSize)
            {
                logger.warn("Small commitlog volume detected at {}; setting commitlog_total_space_in_mb to {}.  You can override this in cassandra.yaml",
                            conf.commitlog_directory, minSize);
                conf.commitlog_total_space_in_mb = minSize;
            }
            else
            {
                conf.commitlog_total_space_in_mb = preferredSize;
            }
        }

        if (conf.saved_caches_directory == null)
        {
            conf.saved_caches_directory = System.getProperty("cassandra.storagedir", null);
            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory is missing and -Dcassandra.storagedir is not set", false);
            conf.saved_caches_directory += File.separator + "saved_caches";
        }
        if (conf.data_file_directories == null || conf.data_file_directories.length == 0)
        {
            String defaultDataDir = System.getProperty("cassandra.storagedir", null);
            if (defaultDataDir == null)
                throw new ConfigurationException("data_file_directories is not missing and -Dcassandra.storagedir is not set", false);
            conf.data_file_directories = new String[]{ defaultDataDir + File.separator + "data" };
        }

        long dataFreeBytes = 0;
        /* data file and commit log directories. they get created later, when they're needed. */
        for (String datadir : conf.data_file_directories)
        {
            if (datadir.equals(conf.commitlog_directory))
                throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.hints_directory))
                throw new ConfigurationException("hints_directory must not be the same as any data_file_directories", false);
            if (datadir.equals(conf.saved_caches_directory))
                throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories", false);

            try
            {
                dataFreeBytes += guessFileStore(datadir).getUnallocatedSpace();
            }
            catch (IOException e)
            {
                logger.debug("Error checking disk space", e);
                throw new ConfigurationException(String.format("Unable to check disk space available to %s. Perhaps the Cassandra user does not have the necessary permissions",
                                                               datadir), e);
            }
        }
        if (dataFreeBytes < 64L * 1024 * 1048576) // 64 GB
            logger.warn("Only {} free across all data volumes. Consider adding more capacity to your cluster or removing obsolete snapshots",
                        FBUtilities.prettyPrintMemory(dataFreeBytes));


        if (conf.commitlog_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory", false);
        if (conf.commitlog_directory.equals(conf.hints_directory))
            throw new ConfigurationException("hints_directory must not be the same as the commitlog_directory", false);
        if (conf.hints_directory.equals(conf.saved_caches_directory))
            throw new ConfigurationException("saved_caches_directory must not be the same as the hints_directory", false);

        if (conf.memtable_flush_writers < 1)
            throw new ConfigurationException("memtable_flush_writers must be at least 1, but was " + conf.memtable_flush_writers, false);

        if (conf.memtable_cleanup_threshold == null)
            conf.memtable_cleanup_threshold = (float) (1.0 / (1 + conf.memtable_flush_writers));

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

        if (conf.num_tokens == null)
            conf.num_tokens = 1;
        else if (conf.num_tokens > MAX_NUM_TOKENS)
            throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported", MAX_NUM_TOKENS), false);

        if (conf.initial_token != null)
        {
            Collection<String> tokens = tokensFromString(conf.initial_token);
            if (tokens.size() != conf.num_tokens)
                throw new ConfigurationException("The number of initial tokens (by initial_token) specified is different from num_tokens value", false);

            for (String token : tokens)
                partitioner.getTokenFactory().validate(token);
        }


        try
        {
            // if key_cache_size_in_mb option was set to "auto" then size of the cache should be "min(5% of Heap (in MB), 100MB)
            keyCacheSizeInMB = (conf.key_cache_size_in_mb == null)
                ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024)), 100)
                : conf.key_cache_size_in_mb;

            if (keyCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("key_cache_size_in_mb option was set incorrectly to '"
                    + conf.key_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
        }

        try
        {
            // if counter_cache_size_in_mb option was set to "auto" then size of the cache should be "min(2.5% of Heap (in MB), 50MB)
            counterCacheSizeInMB = (conf.counter_cache_size_in_mb == null)
                    ? Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.025 / 1024 / 1024)), 50)
                    : conf.counter_cache_size_in_mb;

            if (counterCacheSizeInMB < 0)
                throw new NumberFormatException(); // to escape duplicating error message
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException("counter_cache_size_in_mb option was set incorrectly to '"
                    + conf.counter_cache_size_in_mb + "', supported values are <integer> >= 0.", false);
        }

        // if set to empty/"auto" then use 5% of Heap size
        indexSummaryCapacityInMB = (conf.index_summary_capacity_in_mb == null)
            ? Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.05 / 1024 / 1024))
            : conf.index_summary_capacity_in_mb;

        if (indexSummaryCapacityInMB < 0)
            throw new ConfigurationException("index_summary_capacity_in_mb option was set incorrectly to '"
                    + conf.index_summary_capacity_in_mb + "', it should be a non-negative integer.", false);

        if(conf.encryption_options != null)
        {
            logger.warn("Please rename encryption_options as server_encryption_options in the yaml");
            //operate under the assumption that server_encryption_options is not set in yaml rather than both
            conf.server_encryption_options = conf.encryption_options;
        }

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

        if (conf.user_defined_function_fail_timeout < 0)
            throw new ConfigurationException("user_defined_function_fail_timeout must not be negative", false);
        if (conf.user_defined_function_warn_timeout < 0)
            throw new ConfigurationException("user_defined_function_warn_timeout must not be negative", false);

        if (conf.user_defined_function_fail_timeout < conf.user_defined_function_warn_timeout)
            throw new ConfigurationException("user_defined_function_warn_timeout must less than user_defined_function_fail_timeout", false);

        // always attempt to load the cipher factory, as we could be in the situation where the user has disabled encryption,
        // but has existing commitlogs and sstables on disk that are still encrypted (and still need to be read)
        encryptionContext = new EncryptionContext(config.transparent_data_encryption_options);

        if (conf.max_mutation_size_in_kb == null)
            conf.max_mutation_size_in_kb = conf.commitlog_segment_size_in_mb * 1024 / 2;
        else if (conf.commitlog_segment_size_in_mb * 1024 < 2 * conf.max_mutation_size_in_kb)
            throw new ConfigurationException("commitlog_segment_size_in_mb must be at least twice the size of max_mutation_size_in_kb / 1024", false);

        // native transport encryption options
        if (conf.native_transport_port_ssl != null
            && conf.native_transport_port_ssl.intValue() != conf.native_transport_port.intValue()
            && !conf.client_encryption_options.enabled)
        {
            throw new ConfigurationException("Encryption must be enabled in client_encryption_options for native_transport_port_ssl", false);
        }
    }

    private static FileStore guessFileStore(String dir) throws IOException
    {
        Path path = Paths.get(dir);
        while (true)
        {
            try
            {
                return Files.getFileStore(path);
            }
            catch (IOException e)
            {
                if (e instanceof NoSuchFileException)
                    path = path.getParent();
                else
                    throw e;
            }
        }
    }

    private static IEndpointSnitch createEndpointSnitch(String snitchClassName) throws ConfigurationException
    {
        if (!snitchClassName.contains("."))
            snitchClassName = "org.apache.cassandra.locator." + snitchClassName;
        IEndpointSnitch snitch = FBUtilities.construct(snitchClassName, "snitch");
        return conf.dynamic_snitch ? new DynamicEndpointSnitch(snitch) : snitch;
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    public static IRoleManager getRoleManager()
    {
        return roleManager;
    }

    public static int getPermissionsValidity()
    {
        return conf.permissions_validity_in_ms;
    }

    public static void setPermissionsValidity(int timeout)
    {
        conf.permissions_validity_in_ms = timeout;
    }

    public static int getPermissionsUpdateInterval()
    {
        return conf.permissions_update_interval_in_ms == -1
             ? conf.permissions_validity_in_ms
             : conf.permissions_update_interval_in_ms;
    }

    public static void setPermissionsUpdateInterval(int updateInterval)
    {
        conf.permissions_update_interval_in_ms = updateInterval;
    }

    public static int getPermissionsCacheMaxEntries()
    {
        return conf.permissions_cache_max_entries;
    }

    public static int setPermissionsCacheMaxEntries(int maxEntries)
    {
        return conf.permissions_cache_max_entries = maxEntries;
    }

    public static int getRolesValidity()
    {
        return conf.roles_validity_in_ms;
    }

    public static void setRolesValidity(int validity)
    {
        conf.roles_validity_in_ms = validity;
    }

    public static int getRolesUpdateInterval()
    {
        return conf.roles_update_interval_in_ms == -1
             ? conf.roles_validity_in_ms
             : conf.roles_update_interval_in_ms;
    }

    public static void setRolesUpdateInterval(int interval)
    {
        conf.roles_update_interval_in_ms = interval;
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
        return conf.credentials_validity_in_ms;
    }

    public static void setCredentialsValidity(int timeout)
    {
        conf.credentials_validity_in_ms = timeout;
    }

    public static int getCredentialsUpdateInterval()
    {
        return conf.credentials_update_interval_in_ms == -1
               ? conf.credentials_validity_in_ms
               : conf.credentials_update_interval_in_ms;
    }

    public static void setCredentialsUpdateInterval(int updateInterval)
    {
        conf.credentials_update_interval_in_ms = updateInterval;
    }

    public static int getCredentialsCacheMaxEntries()
    {
        return conf.credentials_cache_max_entries;
    }

    public static int setCredentialsCacheMaxEntries(int maxEntries)
    {
        return conf.credentials_cache_max_entries = maxEntries;
    }

    public static int getThriftFramedTransportSize()
    {
        return conf.thrift_framed_transport_size_in_mb * 1024 * 1024;
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

            if (conf.commitlog_directory == null)
                throw new ConfigurationException("commitlog_directory must be specified", false);
            FileUtils.createDirectory(conf.commitlog_directory);

            if (conf.hints_directory == null)
                throw new ConfigurationException("hints_directory must be specified", false);
            FileUtils.createDirectory(conf.hints_directory);

            if (conf.saved_caches_directory == null)
                throw new ConfigurationException("saved_caches_directory must be specified", false);
            FileUtils.createDirectory(conf.saved_caches_directory);
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

    public static IRequestScheduler getRequestScheduler()
    {
        return requestScheduler;
    }

    public static RequestSchedulerOptions getRequestSchedulerOptions()
    {
        return requestSchedulerOptions;
    }

    public static RequestSchedulerId getRequestSchedulerId()
    {
        return requestSchedulerId;
    }

    public static int getColumnIndexSize()
    {
        return conf.column_index_size_in_kb * 1024;
    }

    @VisibleForTesting
    public static void setColumnIndexSize(int val)
    {
        conf.column_index_size_in_kb = val;
    }

    public static int getBatchSizeWarnThreshold()
    {
        return conf.batch_size_warn_threshold_in_kb * 1024;
    }

    public static long getBatchSizeFailThreshold()
    {
        return conf.batch_size_fail_threshold_in_kb * 1024L;
    }

    public static int getBatchSizeFailThresholdInKB()
    {
        return conf.batch_size_fail_threshold_in_kb;
    }

    public static int getUnloggedBatchAcrossPartitionsWarnThreshold()
    {
        return conf.unlogged_batch_across_partitions_warn_threshold;
    }

    public static void setBatchSizeWarnThresholdInKB(int threshold)
    {
        conf.batch_size_warn_threshold_in_kb = threshold;
    }

    public static void setBatchSizeFailThresholdInKB(int threshold)
    {
        conf.batch_size_fail_threshold_in_kb = threshold;
    }

    public static Collection<String> getInitialTokens()
    {
        return tokensFromString(System.getProperty("cassandra.initial_token", conf.initial_token));
    }

    public static String getAllocateTokensForKeyspace()
    {
        return System.getProperty("cassandra.allocate_tokens_for_keyspace", conf.allocate_tokens_for_keyspace);
    }

    public static Collection<String> tokensFromString(String tokenString)
    {
        List<String> tokens = new ArrayList<String>();
        if (tokenString != null)
            for (String token : StringUtils.split(tokenString, ','))
                tokens.add(token.trim());
        return tokens;
    }

    public static Integer getNumTokens()
    {
        return conf.num_tokens;
    }

    public static InetAddress getReplaceAddress()
    {
        try
        {
            if (System.getProperty("cassandra.replace_address", null) != null)
                return InetAddress.getByName(System.getProperty("cassandra.replace_address", null));
            else if (System.getProperty("cassandra.replace_address_first_boot", null) != null)
                return InetAddress.getByName(System.getProperty("cassandra.replace_address_first_boot", null));
            return null;
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("Replacement host name could not be resolved or scope_id was specified for a global IPv6 address", e);
        }
    }

    public static Collection<String> getReplaceTokens()
    {
        return tokensFromString(System.getProperty("cassandra.replace_token", null));
    }

    public static UUID getReplaceNode()
    {
        try
        {
            return UUID.fromString(System.getProperty("cassandra.replace_node", null));
        } catch (NullPointerException e)
        {
            return null;
        }
    }

    public static boolean isReplacing()
    {
        if (System.getProperty("cassandra.replace_address_first_boot", null) != null && SystemKeyspace.bootstrapComplete())
        {
            logger.info("Replace address on first boot requested; this node is already bootstrapped");
            return false;
        }
        return getReplaceAddress() != null;
    }

    public static String getClusterName()
    {
        return conf.cluster_name;
    }

    public static int getMaxStreamingRetries()
    {
        return conf.max_streaming_retries;
    }

    public static int getStoragePort()
    {
        return Integer.parseInt(System.getProperty("cassandra.storage_port", conf.storage_port.toString()));
    }

    public static int getSSLStoragePort()
    {
        return Integer.parseInt(System.getProperty("cassandra.ssl_storage_port", conf.ssl_storage_port.toString()));
    }

    public static int getRpcPort()
    {
        return Integer.parseInt(System.getProperty("cassandra.rpc_port", conf.rpc_port.toString()));
    }

    public static int getRpcListenBacklog()
    {
        return conf.rpc_listen_backlog;
    }

    public static long getRpcTimeout()
    {
        return conf.request_timeout_in_ms;
    }

    public static void setRpcTimeout(Long timeOutInMillis)
    {
        conf.request_timeout_in_ms = timeOutInMillis;
    }

    public static long getReadRpcTimeout()
    {
        return conf.read_request_timeout_in_ms;
    }

    public static void setReadRpcTimeout(Long timeOutInMillis)
    {
        conf.read_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getRangeRpcTimeout()
    {
        return conf.range_request_timeout_in_ms;
    }

    public static void setRangeRpcTimeout(Long timeOutInMillis)
    {
        conf.range_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getWriteRpcTimeout()
    {
        return conf.write_request_timeout_in_ms;
    }

    public static void setWriteRpcTimeout(Long timeOutInMillis)
    {
        conf.write_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getCounterWriteRpcTimeout()
    {
        return conf.counter_write_request_timeout_in_ms;
    }

    public static void setCounterWriteRpcTimeout(Long timeOutInMillis)
    {
        conf.counter_write_request_timeout_in_ms = timeOutInMillis;
    }

    public static long getCasContentionTimeout()
    {
        return conf.cas_contention_timeout_in_ms;
    }

    public static void setCasContentionTimeout(Long timeOutInMillis)
    {
        conf.cas_contention_timeout_in_ms = timeOutInMillis;
    }

    public static long getTruncateRpcTimeout()
    {
        return conf.truncate_request_timeout_in_ms;
    }

    public static void setTruncateRpcTimeout(Long timeOutInMillis)
    {
        conf.truncate_request_timeout_in_ms = timeOutInMillis;
    }

    public static boolean hasCrossNodeTimeout()
    {
        return conf.cross_node_timeout;
    }

    // not part of the Verb enum so we can change timeouts easily via JMX
    public static long getTimeout(MessagingService.Verb verb)
    {
        switch (verb)
        {
            case READ:
                return getReadRpcTimeout();
            case RANGE_SLICE:
                return getRangeRpcTimeout();
            case TRUNCATE:
                return getTruncateRpcTimeout();
            case READ_REPAIR:
            case MUTATION:
            case PAXOS_COMMIT:
            case PAXOS_PREPARE:
            case PAXOS_PROPOSE:
            case HINT:
            case BATCH_STORE:
            case BATCH_REMOVE:
                return getWriteRpcTimeout();
            case COUNTER_MUTATION:
                return getCounterWriteRpcTimeout();
            default:
                return getRpcTimeout();
        }
    }

    /**
     * @return the minimum configured {read, write, range, truncate, misc} timeout
     */
    public static long getMinRpcTimeout()
    {
        return Longs.min(getRpcTimeout(),
                         getReadRpcTimeout(),
                         getRangeRpcTimeout(),
                         getWriteRpcTimeout(),
                         getCounterWriteRpcTimeout(),
                         getTruncateRpcTimeout());
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

    public static int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public static int getConcurrentCounterWriters()
    {
        return conf.concurrent_counter_writes;
    }

    public static int getConcurrentViewWriters()
    {
        return conf.concurrent_materialized_view_writes;
    }

    public static int getFlushWriters()
    {
            return conf.memtable_flush_writers;
    }

    public static int getConcurrentCompactors()
    {
        return conf.concurrent_compactors;
    }

    public static int getCompactionThroughputMbPerSec()
    {
        return conf.compaction_throughput_mb_per_sec;
    }

    public static void setCompactionThroughputMbPerSec(int value)
    {
        conf.compaction_throughput_mb_per_sec = value;
    }

    public static int getCompactionLargePartitionWarningThreshold() { return conf.compaction_large_partition_warning_threshold_mb * 1024 * 1024; }

    public static boolean getDisableSTCSInL0()
    {
        return Boolean.getBoolean("cassandra.disable_stcs_in_l0");
    }

    public static int getStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.stream_throughput_outbound_megabits_per_sec;
    }

    public static void setStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getInterDCStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.inter_dc_stream_throughput_outbound_megabits_per_sec;
    }

    public static void setInterDCStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.inter_dc_stream_throughput_outbound_megabits_per_sec = value;
    }

    public static String[] getAllDataFileLocations()
    {
        return conf.data_file_directories;
    }

    public static String getCommitLogLocation()
    {
        return conf.commitlog_directory;
    }

    public static ParameterizedClass getCommitLogCompression()
    {
        return conf.commitlog_compression;
    }

    public static void setCommitLogCompression(ParameterizedClass compressor)
    {
        conf.commitlog_compression = compressor;
    }

    public static int getCommitLogMaxCompressionBuffersInPool()
    {
        return conf.commitlog_max_compression_buffers_in_pool;
    }

    public static int getMaxMutationSize()
    {
        return conf.max_mutation_size_in_kb * 1024;
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

    /**
     * size of commitlog segments to allocate
     */
    public static int getCommitLogSegmentSize()
    {
        return conf.commitlog_segment_size_in_mb * 1024 * 1024;
    }

    public static void setCommitLogSegmentSize(int sizeMegabytes)
    {
        conf.commitlog_segment_size_in_mb = sizeMegabytes;
    }

    public static String getSavedCachesLocation()
    {
        return conf.saved_caches_directory;
    }

    public static Set<InetAddress> getSeeds()
    {
        return ImmutableSet.<InetAddress>builder().addAll(seedProvider.getSeeds()).build();
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }

    public static InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }

    public static Boolean shouldListenOnBroadcastAddress()
    {
        return conf.listen_on_broadcast_address;
    }

    public static IInternodeAuthenticator getInternodeAuthenticator()
    {
        return internodeAuthenticator;
    }

    public static void setBroadcastAddress(InetAddress broadcastAdd)
    {
        broadcastAddress = broadcastAdd;
    }

    public static boolean startRpc()
    {
        return conf.start_rpc;
    }

    public static InetAddress getRpcAddress()
    {
        return rpcAddress;
    }

    public static void setBroadcastRpcAddress(InetAddress broadcastRPCAddr)
    {
        broadcastRpcAddress = broadcastRPCAddr;
    }

    public static InetAddress getBroadcastRpcAddress()
    {
        return broadcastRpcAddress;
    }

    public static String getRpcServerType()
    {
        return conf.rpc_server_type;
    }

    public static boolean getRpcKeepAlive()
    {
        return conf.rpc_keepalive;
    }

    public static Integer getRpcMinThreads()
    {
        return conf.rpc_min_threads;
    }

    public static Integer getRpcMaxThreads()
    {
        return conf.rpc_max_threads;
    }

    public static Integer getRpcSendBufferSize()
    {
        return conf.rpc_send_buff_size_in_bytes;
    }

    public static Integer getRpcRecvBufferSize()
    {
        return conf.rpc_recv_buff_size_in_bytes;
    }

    public static Integer getInternodeSendBufferSize()
    {
        return conf.internode_send_buff_size_in_bytes;
    }

    public static Integer getInternodeRecvBufferSize()
    {
        return conf.internode_recv_buff_size_in_bytes;
    }

    public static boolean startNativeTransport()
    {
        return conf.start_native_transport;
    }

    public static int getNativeTransportPort()
    {
        return Integer.parseInt(System.getProperty("cassandra.native_transport_port", conf.native_transport_port.toString()));
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

    public static Integer getNativeTransportMaxThreads()
    {
        return conf.native_transport_max_threads;
    }

    public static int getNativeTransportMaxFrameSize()
    {
        return conf.native_transport_max_frame_size_in_mb * 1024 * 1024;
    }

    public static Long getNativeTransportMaxConcurrentConnections()
    {
        return conf.native_transport_max_concurrent_connections;
    }

    public static void setNativeTransportMaxConcurrentConnections(long nativeTransportMaxConcurrentConnections)
    {
        conf.native_transport_max_concurrent_connections = nativeTransportMaxConcurrentConnections;
    }

    public static Long getNativeTransportMaxConcurrentConnectionsPerIp() {
        return conf.native_transport_max_concurrent_connections_per_ip;
    }

    public static void setNativeTransportMaxConcurrentConnectionsPerIp(long native_transport_max_concurrent_connections_per_ip)
    {
        conf.native_transport_max_concurrent_connections_per_ip = native_transport_max_concurrent_connections_per_ip;
    }

    public static double getCommitLogSyncBatchWindow()
    {
        return conf.commitlog_sync_batch_window_in_ms;
    }

    public static void setCommitLogSyncBatchWindow(double windowMillis)
    {
        conf.commitlog_sync_batch_window_in_ms = windowMillis;
    }

    public static int getCommitLogSyncPeriod()
    {
        return conf.commitlog_sync_period_in_ms;
    }

    public static void setCommitLogSyncPeriod(int periodMillis)
    {
        conf.commitlog_sync_period_in_ms = periodMillis;
    }

    public static Config.CommitLogSync getCommitLogSync()
    {
        return conf.commitlog_sync;
    }

    public static void setCommitLogSync(CommitLogSync sync)
    {
        conf.commitlog_sync = sync;
    }

    public static Config.DiskAccessMode getDiskAccessMode()
    {
        return conf.disk_access_mode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setDiskAccessMode(Config.DiskAccessMode mode)
    {
        conf.disk_access_mode = mode;
    }

    public static Config.DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    // Do not use outside unit tests.
    @VisibleForTesting
    public static void setIndexAccessMode(Config.DiskAccessMode mode)
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

    public static boolean isAutoSnapshot() {
        return conf.auto_snapshot;
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

    public static boolean isAutoBootstrap()
    {
        return Boolean.parseBoolean(System.getProperty("cassandra.auto_bootstrap", conf.auto_bootstrap.toString()));
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
        conf.max_hint_window_in_ms = ms;
    }

    public static int getMaxHintWindow()
    {
        return conf.max_hint_window_in_ms;
    }

    public static File getHintsDirectory()
    {
        return new File(conf.hints_directory);
    }

    public static File getSerializedCachePath(CacheService.CacheType cacheType, String version, String extension)
    {
        String name = cacheType.toString()
                + (version == null ? "" : "-" + version + "." + extension);
        return new File(conf.saved_caches_directory, name);
    }

    public static int getDynamicUpdateInterval()
    {
        return conf.dynamic_snitch_update_interval_in_ms;
    }
    public static void setDynamicUpdateInterval(Integer dynamicUpdateInterval)
    {
        conf.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
    }

    public static int getDynamicResetInterval()
    {
        return conf.dynamic_snitch_reset_interval_in_ms;
    }
    public static void setDynamicResetInterval(Integer dynamicResetInterval)
    {
        conf.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
    }

    public static double getDynamicBadnessThreshold()
    {
        return conf.dynamic_snitch_badness_threshold;
    }

    public static void setDynamicBadnessThreshold(Double dynamicBadnessThreshold)
    {
        conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public static ServerEncryptionOptions getServerEncryptionOptions()
    {
        return conf.server_encryption_options;
    }

    public static ClientEncryptionOptions getClientEncryptionOptions()
    {
        return conf.client_encryption_options;
    }

    public static int getHintedHandoffThrottleInKB()
    {
        return conf.hinted_handoff_throttle_in_kb;
    }

    public static int getBatchlogReplayThrottleInKB()
    {
        return conf.batchlog_replay_throttle_in_kb;
    }

    public static void setHintedHandoffThrottleInKB(Integer throttleInKB)
    {
        conf.hinted_handoff_throttle_in_kb = throttleInKB;
    }

    public static int getMaxHintsDeliveryThreads()
    {
        return conf.max_hints_delivery_threads;
    }

    public static int getHintsFlushPeriodInMS()
    {
        return conf.hints_flush_period_in_ms;
    }

    public static long getMaxHintsFileSize()
    {
        return conf.max_hints_file_size_in_mb * 1024L * 1024L;
    }

    public static ParameterizedClass getHintsCompression()
    {
        return conf.hints_compression;
    }

    public static void setHintsCompression(ParameterizedClass parameterizedClass)
    {
        conf.hints_compression = parameterizedClass;
    }

    public static boolean isIncrementalBackupsEnabled()
    {
        return conf.incremental_backups;
    }

    public static void setIncrementalBackupsEnabled(boolean value)
    {
        conf.incremental_backups = value;
    }

    public static int getFileCacheSizeInMB()
    {
        return conf.file_cache_size_in_mb;
    }

    public static boolean getBufferPoolUseHeapIfExhausted()
    {
        return conf.buffer_pool_use_heap_if_exhausted;
    }

    public static Config.DiskOptimizationStrategy getDiskOptimizationStrategy()
    {
        return conf.disk_optimization_strategy;
    }

    @VisibleForTesting
    public static void setDiskOptimizationStrategy(Config.DiskOptimizationStrategy strategy)
    {
        conf.disk_optimization_strategy = strategy;
    }

    public static double getDiskOptimizationEstimatePercentile()
    {
        return conf.disk_optimization_estimate_percentile;
    }

    public static double getDiskOptimizationPageCrossChance()
    {
        return conf.disk_optimization_page_cross_chance;
    }

    @VisibleForTesting
    public static void setDiskOptimizationPageCrossChance(double chance)
    {
        conf.disk_optimization_page_cross_chance = chance;
    }

    public static long getTotalCommitlogSpaceInMB()
    {
        return conf.commitlog_total_space_in_mb;
    }

    public static int getSSTablePreempiveOpenIntervalInMB()
    {
        return FBUtilities.isWindows() ? -1 : conf.sstable_preemptive_open_interval_in_mb;
    }

    public static boolean getTrickleFsync()
    {
        return conf.trickle_fsync;
    }

    public static int getTrickleFsyncIntervalInKb()
    {
        return conf.trickle_fsync_interval_in_kb;
    }

    public static long getKeyCacheSizeInMB()
    {
        return keyCacheSizeInMB;
    }

    public static long getIndexSummaryCapacityInMB()
    {
        return indexSummaryCapacityInMB;
    }

    public static int getKeyCacheSavePeriod()
    {
        return conf.key_cache_save_period;
    }

    public static void setKeyCacheSavePeriod(int keyCacheSavePeriod)
    {
        conf.key_cache_save_period = keyCacheSavePeriod;
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

    public static long getRowCacheSizeInMB()
    {
        return conf.row_cache_size_in_mb;
    }

    @VisibleForTesting
    public static void setRowCacheSizeInMB(long val)
    {
        conf.row_cache_size_in_mb = val;
    }

    public static int getRowCacheSavePeriod()
    {
        return conf.row_cache_save_period;
    }

    public static void setRowCacheSavePeriod(int rowCacheSavePeriod)
    {
        conf.row_cache_save_period = rowCacheSavePeriod;
    }

    public static int getRowCacheKeysToSave()
    {
        return conf.row_cache_keys_to_save;
    }

    public static long getCounterCacheSizeInMB()
    {
        return counterCacheSizeInMB;
    }

    public static void setRowCacheKeysToSave(int rowCacheKeysToSave)
    {
        conf.row_cache_keys_to_save = rowCacheKeysToSave;
    }

    public static int getCounterCacheSavePeriod()
    {
        return conf.counter_cache_save_period;
    }

    public static void setCounterCacheSavePeriod(int counterCacheSavePeriod)
    {
        conf.counter_cache_save_period = counterCacheSavePeriod;
    }

    public static int getCounterCacheKeysToSave()
    {
        return conf.counter_cache_keys_to_save;
    }

    public static void setCounterCacheKeysToSave(int counterCacheKeysToSave)
    {
        conf.counter_cache_keys_to_save = counterCacheKeysToSave;
    }

    public static void setStreamingSocketTimeout(int value)
    {
        conf.streaming_socket_timeout_in_ms = value;
    }

    public static int getStreamingSocketTimeout()
    {
        return conf.streaming_socket_timeout_in_ms;
    }

    public static String getLocalDataCenter()
    {
        return localDC;
    }

    public static Comparator<InetAddress> getLocalComparator()
    {
        return localComparator;
    }

    public static Config.InternodeCompression internodeCompression()
    {
        return conf.internode_compression;
    }

    public static boolean getInterDCTcpNoDelay()
    {
        return conf.inter_dc_tcp_nodelay;
    }


    public static SSTableFormat.Type getSSTableFormat()
    {
        return sstable_format;
    }

    public static MemtablePool getMemtableAllocatorPool()
    {
        long heapLimit = ((long) conf.memtable_heap_space_in_mb) << 20;
        long offHeapLimit = ((long) conf.memtable_offheap_space_in_mb) << 20;
        switch (conf.memtable_allocation_type)
        {
            case unslabbed_heap_buffers:
                return new HeapPool(heapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case heap_buffers:
                return new SlabPool(heapLimit, 0, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_buffers:
                if (!FileUtils.isCleanerAvailable())
                {
                    throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
                }
                return new SlabPool(heapLimit, offHeapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            case offheap_objects:
                return new NativePool(heapLimit, offHeapLimit, conf.memtable_cleanup_threshold, new ColumnFamilyStore.FlushLargestColumnFamily());
            default:
                throw new AssertionError();
        }
    }

    public static int getIndexSummaryResizeIntervalInMinutes()
    {
        return conf.index_summary_resize_interval_in_minutes;
    }

    public static boolean hasLargeAddressSpace()
    {
        // currently we just check if it's a 64bit arch, but any we only really care if the address space is large
        String datamodel = System.getProperty("sun.arch.data.model");
        if (datamodel != null)
        {
            switch (datamodel)
            {
                case "64": return true;
                case "32": return false;
            }
        }
        String arch = System.getProperty("os.arch");
        return arch.contains("64") || arch.contains("sparcv9");
    }

    public static int getTracetypeRepairTTL()
    {
        return conf.tracetype_repair_ttl;
    }

    public static int getTracetypeQueryTTL()
    {
        return conf.tracetype_query_ttl;
    }

    public static String getOtcCoalescingStrategy()
    {
        return conf.otc_coalescing_strategy;
    }

    public static int getOtcCoalescingWindow()
    {
        return conf.otc_coalescing_window_us;
    }

    public static int getWindowsTimerInterval()
    {
        return conf.windows_timer_interval;
    }

    public static boolean enableUserDefinedFunctions()
    {
        return conf.enable_user_defined_functions;
    }

    public static boolean enableScriptedUserDefinedFunctions()
    {
        return conf.enable_scripted_user_defined_functions;
    }

    public static void enableScriptedUserDefinedFunctions(boolean enableScriptedUserDefinedFunctions)
    {
        conf.enable_scripted_user_defined_functions = enableScriptedUserDefinedFunctions;
    }

    public static boolean enableUserDefinedFunctionsThreads()
    {
        return conf.enable_user_defined_functions_threads;
    }

    public static long getUserDefinedFunctionWarnTimeout()
    {
        return conf.user_defined_function_warn_timeout;
    }

    public static void setUserDefinedFunctionWarnTimeout(long userDefinedFunctionWarnTimeout)
    {
        conf.user_defined_function_warn_timeout = userDefinedFunctionWarnTimeout;
    }

    public static long getUserDefinedFunctionFailTimeout()
    {
        return conf.user_defined_function_fail_timeout;
    }

    public static void setUserDefinedFunctionFailTimeout(long userDefinedFunctionFailTimeout)
    {
        conf.user_defined_function_fail_timeout = userDefinedFunctionFailTimeout;
    }

    public static Config.UserFunctionTimeoutPolicy getUserFunctionTimeoutPolicy()
    {
        return conf.user_function_timeout_policy;
    }

    public static void setUserFunctionTimeoutPolicy(Config.UserFunctionTimeoutPolicy userFunctionTimeoutPolicy)
    {
        conf.user_function_timeout_policy = userFunctionTimeoutPolicy;
    }

    public static EncryptionContext getEncryptionContext()
    {
        return encryptionContext;
    }
    
    public static long getGCWarnThreshold()
    {
        return conf.gc_warn_threshold_in_ms;
    }

    @VisibleForTesting
    public static void setEncryptionContext(EncryptionContext ec)
    {
        encryptionContext = ec;
    }

    public static int searchConcurrencyFactor()
    {
        return Integer.valueOf(System.getProperty("cassandra.search_concurrency_factor", "1"));
    }
}
