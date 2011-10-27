/**
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
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthority;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthority;
import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.scheduler.NoScheduler;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.cassandra.utils.FBUtilities;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

public class DatabaseDescriptor
{
    private static Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress rpcAddress;
    private static SeedProvider seedProvider;
    /* Current index into the above list of directories */
    private static int currentIndex = 0;

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;

    private static Config.DiskAccessMode indexAccessMode;
    
    private static Config conf;

    private static IAuthenticator authenticator = new AllowAllAuthenticator();
    private static IAuthority authority = new AllowAllAuthority();

    private final static String DEFAULT_CONFIGURATION = "cassandra.yaml";

    private static IRequestScheduler requestScheduler;
    private static RequestSchedulerId requestSchedulerId;
    private static RequestSchedulerOptions requestSchedulerOptions;

    /**
     * Inspect the classpath to find storage configuration file
     */
    static URL getStorageConfigURL() throws ConfigurationException
    {
        String configUrl = System.getProperty("cassandra.config");
        if (configUrl == null)
            configUrl = DEFAULT_CONFIGURATION;

        URL url;
        try
        {
            url = new URL(configUrl);
            url.openStream().close(); // catches well-formed but bogus URLs
        }
        catch (Exception e)
        {
            ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
            url = loader.getResource(configUrl);
            if (url == null)
                throw new ConfigurationException("Cannot locate " + configUrl);
        }

        return url;
    }
    
    static
    {
        try
        {
            URL url = getStorageConfigURL();
            logger.info("Loading settings from " + url);

            InputStream input = null;
            try
            {
                input = url.openStream();
            }
            catch (IOException e)
            {
                // getStorageConfigURL should have ruled this out
                throw new AssertionError(e);
            }
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(Config.class);
            TypeDescription seedDesc = new TypeDescription(SeedProviderDef.class);
            seedDesc.putMapPropertyType("parameters", String.class, String.class);
            constructor.addTypeDescription(seedDesc);
            Yaml yaml = new Yaml(new Loader(constructor));
            conf = (Config)yaml.load(input);
            
            if (conf.commitlog_sync == null)
            {
                throw new ConfigurationException("Missing required directive CommitLogSync");
            }

            if (conf.commitlog_sync == Config.CommitLogSync.batch)
            {
                if (conf.commitlog_sync_batch_window_in_ms == null)
                {
                    throw new ConfigurationException("Missing value for commitlog_sync_batch_window_in_ms: Double expected.");
                } 
                else if (conf.commitlog_sync_period_in_ms != null)
                {
                    throw new ConfigurationException("Batch sync specified, but commitlog_sync_period_in_ms found. Only specify commitlog_sync_batch_window_in_ms when using batch sync");
                }
                logger.debug("Syncing log with a batch window of " + conf.commitlog_sync_batch_window_in_ms);
            }
            else
            {
                if (conf.commitlog_sync_period_in_ms == null)
                {
                    throw new ConfigurationException("Missing value for commitlog_sync_period_in_ms: Integer expected");
                }
                else if (conf.commitlog_sync_batch_window_in_ms != null)
                {
                    throw new ConfigurationException("commitlog_sync_period_in_ms specified, but commitlog_sync_batch_window_in_ms found.  Only specify commitlog_sync_period_in_ms when using periodic sync.");
                }
                logger.debug("Syncing log with a period of " + conf.commitlog_sync_period_in_ms);
            }

            /* evaluate the DiskAccessMode Config directive, which also affects indexAccessMode selection */           
            if (conf.disk_access_mode == Config.DiskAccessMode.auto)
            {
                conf.disk_access_mode = System.getProperty("os.arch").contains("64") ? Config.DiskAccessMode.mmap : Config.DiskAccessMode.standard;
                indexAccessMode = conf.disk_access_mode;
                logger.info("DiskAccessMode 'auto' determined to be " + conf.disk_access_mode + ", indexAccessMode is " + indexAccessMode );
            }
            else if (conf.disk_access_mode == Config.DiskAccessMode.mmap_index_only)
            {
                conf.disk_access_mode = Config.DiskAccessMode.standard;
                indexAccessMode = Config.DiskAccessMode.mmap;
                logger.info("DiskAccessMode is " + conf.disk_access_mode + ", indexAccessMode is " + indexAccessMode );
            }
            else
            {
                indexAccessMode = conf.disk_access_mode;
                logger.info("DiskAccessMode is " + conf.disk_access_mode + ", indexAccessMode is " + indexAccessMode );
            }
            // We could enable cleaner for index only mmap but it probably doesn't matter much
            if (conf.disk_access_mode == Config.DiskAccessMode.mmap)
                MmappedSegmentedFile.initCleaner();

            /* Authentication and authorization backend, implementing IAuthenticator and IAuthority */
            if (conf.authenticator != null)
                authenticator = FBUtilities.<IAuthenticator>construct(conf.authenticator, "authenticator");
            if (conf.authority != null)
                authority = FBUtilities.<IAuthority>construct(conf.authority, "authority");
            authenticator.validateConfiguration();
            authority.validateConfiguration();
            
            /* Hashing strategy */
            if (conf.partitioner == null)
            {
                throw new ConfigurationException("Missing directive: partitioner");
            }
            try
            {
                partitioner = FBUtilities.newPartitioner(conf.partitioner);
            }
            catch (Exception e)
            {
                throw new ConfigurationException("Invalid partitioner class " + conf.partitioner);
            }

            /* phi convict threshold for FailureDetector */
            if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16)
            {
                throw new ConfigurationException("phi_convict_threshold must be between 5 and 16");
            }
            
            /* Thread per pool */
            if (conf.concurrent_reads != null && conf.concurrent_reads < 2) 
            {
                throw new ConfigurationException("concurrent_reads must be at least 2");
            }

            if (conf.concurrent_writes != null && conf.concurrent_writes < 2)
            {
                throw new ConfigurationException("concurrent_writes must be at least 2");
            }

            if (conf.concurrent_replicates != null && conf.concurrent_replicates < 2)
            {
                throw new ConfigurationException("concurrent_replicates must be at least 2");
            }

            if (conf.memtable_total_space_in_mb == null)
                conf.memtable_total_space_in_mb = (int) (Runtime.getRuntime().maxMemory() / (3 * 1048576));
            if (conf.memtable_total_space_in_mb <= 0)
                throw new ConfigurationException("memtable_total_space_in_mb must be positive");
            logger.info("Global memtable threshold is enabled at {}MB", conf.memtable_total_space_in_mb);

            /* Memtable flush writer threads */
            if (conf.memtable_flush_writers != null && conf.memtable_flush_writers < 1)
            {
                throw new ConfigurationException("memtable_flush_writers must be at least 1");
            }
            else if (conf.memtable_flush_writers == null)
            {
                conf.memtable_flush_writers = conf.data_file_directories.length;
            }

            /* Local IP or hostname to bind services to */
            if (conf.listen_address != null)
            {
                try
                {
                    listenAddress = InetAddress.getByName(conf.listen_address);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown listen_address '" + conf.listen_address + "'");
                }
            }

            /* Gossip Address to broadcast */
            if (conf.broadcast_address != null)
            {
                if (conf.broadcast_address.equals("0.0.0.0"))
                {
                    throw new ConfigurationException("broadcast_address cannot be 0.0.0.0!");
                }
                
                try
                {
                    broadcastAddress = InetAddress.getByName(conf.broadcast_address);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown broadcast_address '" + conf.broadcast_address + "'");
                }
            }
            
            /* Local IP or hostname to bind RPC server to */
            if (conf.rpc_address != null)
            {
                try
                {
                    rpcAddress = InetAddress.getByName(conf.rpc_address);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown host in rpc_address " + conf.rpc_address);
                }
            }
            else
            {
                rpcAddress = FBUtilities.getLocalAddress();
            }

            if (conf.thrift_framed_transport_size_in_mb <= 0)
                throw new ConfigurationException("thrift_framed_transport_size_in_mb must be positive");

            if (conf.thrift_framed_transport_size_in_mb > 0 && conf.thrift_max_message_length_in_mb < conf.thrift_framed_transport_size_in_mb)
            {
                throw new ConfigurationException("thrift_max_message_length_in_mb must be greater than thrift_framed_transport_size_in_mb when using TFramedTransport");
            }

            /* end point snitch */
            if (conf.endpoint_snitch == null)
            {
                throw new ConfigurationException("Missing endpoint_snitch directive");
            }
            snitch = createEndpointSnitch(conf.endpoint_snitch);
            EndpointSnitchInfo.create();

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
                    Class cls = Class.forName(conf.request_scheduler);
                    requestScheduler = (IRequestScheduler) cls.getConstructor(RequestSchedulerOptions.class).newInstance(requestSchedulerOptions);
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Invalid Request Scheduler class " + conf.request_scheduler);
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

            if (logger.isDebugEnabled() && conf.auto_bootstrap != null)
            {
                logger.debug("setting auto_bootstrap to " + conf.auto_bootstrap);
            }
            
           if (conf.in_memory_compaction_limit_in_mb != null && conf.in_memory_compaction_limit_in_mb <= 0)
            {
                throw new ConfigurationException("in_memory_compaction_limit_in_mb must be a positive integer");
            }

            if (conf.concurrent_compactors == null)
                conf.concurrent_compactors = Runtime.getRuntime().availableProcessors();

            if (conf.concurrent_compactors <= 0)
                throw new ConfigurationException("concurrent_compactors should be strictly greater than 0");

            if (conf.compaction_throughput_mb_per_sec == null)
                conf.compaction_throughput_mb_per_sec = 16;

            if (conf.stream_throughput_outbound_megabits_per_sec == null)
                conf.stream_throughput_outbound_megabits_per_sec = 400;

            if (!CassandraDaemon.rpc_server_types.contains(conf.rpc_server_type.toLowerCase()))
                throw new ConfigurationException("Unknown rpc_server_type: " + conf.rpc_server_type);
            if (conf.rpc_min_threads == null)
                conf.rpc_min_threads = conf.rpc_server_type.toLowerCase().equals("hsha")
                                     ? Runtime.getRuntime().availableProcessors() * 4
                                     : 16;
            if (conf.rpc_max_threads == null)
                conf.rpc_max_threads = conf.rpc_server_type.toLowerCase().equals("hsha")
                                     ? Runtime.getRuntime().availableProcessors() * 4
                                     : Integer.MAX_VALUE;

            /* data file and commit log directories. they get created later, when they're needed. */
            if (conf.commitlog_directory != null && conf.data_file_directories != null && conf.saved_caches_directory != null)
            {
                for (String datadir : conf.data_file_directories)
                {
                    if (datadir.equals(conf.commitlog_directory))
                        throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories");
                    if (datadir.equals(conf.saved_caches_directory))
                        throw new ConfigurationException("saved_caches_directory must not be the same as any data_file_directories");
                }

                if (conf.commitlog_directory.equals(conf.saved_caches_directory))
                    throw new ConfigurationException("saved_caches_directory must not be the same as the commitlog_directory");
            }
            else
            {
                if (conf.commitlog_directory == null)
                    throw new ConfigurationException("commitlog_directory missing");
                if (conf.data_file_directories == null)
                    throw new ConfigurationException("data_file_directories missing; at least one data directory must be specified");
                if (conf.saved_caches_directory == null)
                    throw new ConfigurationException("saved_caches_directory missing");
            }

            if (conf.initial_token != null)
                partitioner.getTokenFactory().validate(conf.initial_token);

            // Hardcoded system tables
            KSMetaData systemMeta = KSMetaData.systemKeyspace();
            Schema.instance.load(CFMetaData.StatusCf);
            Schema.instance.load(CFMetaData.HintsCf);
            Schema.instance.load(CFMetaData.MigrationsCf);
            Schema.instance.load(CFMetaData.SchemaCf);
            Schema.instance.load(CFMetaData.IndexCf);
            Schema.instance.load(CFMetaData.NodeIdCf);
            Schema.instance.load(CFMetaData.VersionCf);

            Schema.instance.addSystemTable(systemMeta);

            /* Load the seeds for node contact points */
            if (conf.seed_provider == null)
            {
                throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.");
            }
            try 
            {
                Class seedProviderClass = Class.forName(conf.seed_provider.class_name);
                seedProvider = (SeedProvider)seedProviderClass.getConstructor(Map.class).newInstance(conf.seed_provider.parameters);
            }
            // there are about 5 checked exceptions that could be thrown here.
            catch (Exception e)
            {
                logger.error("Fatal configuration error", e);
                System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
                System.exit(1);
            }
            if (seedProvider.getSeeds().size() == 0)
                throw new ConfigurationException("The seed provider lists no seeds.");
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }
        catch (YAMLException e)
        {
            logger.error("Fatal configuration error error", e);
            System.err.println(e.getMessage() + "\nInvalid yaml; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }
    }

    private static IEndpointSnitch createEndpointSnitch(String endpointSnitchClassName) throws ConfigurationException
    {
        IEndpointSnitch snitch = FBUtilities.construct(endpointSnitchClassName, "snitch");
        return conf.dynamic_snitch ? new DynamicEndpointSnitch(snitch) : snitch;
    }

    /** load keyspace (table) definitions, but do not initialize the table instances. */
    public static void loadSchemas() throws IOException                         
    {
        // we can load tables from local storage if a version is set in the system table and that acutally maps to
        // real data in the definitions table.  If we do end up loading from xml, store the defintions so that we
        // don't load from xml anymore.
        UUID uuid = Migration.getLastMigrationId();
        if (uuid == null)
        {
            logger.info("Couldn't detect any schema definitions in local storage.");
            // peek around the data directories to see if anything is there.
            boolean hasExistingTables = false;
            for (String dataDir : getAllDataFileLocations())
            {
                File dataPath = new File(dataDir);
                if (dataPath.exists() && dataPath.isDirectory())
                {
                    // see if there are other directories present.
                    int dirCount = dataPath.listFiles(new FileFilter()
                    {
                        public boolean accept(File pathname)
                        {
                            return pathname.isDirectory();
                        }
                    }).length;
                    if (dirCount > 0)
                        hasExistingTables = true;
                }
                if (hasExistingTables)
                {
                    break;
                }
            }
            
            if (hasExistingTables)
                logger.info("Found table data in data directories. Consider using the CLI to define your schema.");
            else
                logger.info("To create keyspaces and column families, see 'help create keyspace' in the CLI, or set up a schema using the thrift system_* calls.");
        }
        else
        {
            logger.info("Loading schema version " + uuid.toString());
            Collection<KSMetaData> tableDefs = DefsTable.loadFromStorage(uuid);   

            // happens when someone manually deletes all tables and restarts.
            if (tableDefs.size() == 0)
            {
                logger.warn("No schema definitions were found in local storage.");
                // set version so that migrations leading up to emptiness aren't replayed.
                Schema.instance.setVersion(uuid);
            }
            else // if non-system tables where found, trying to load them
            {
                Schema.instance.load(tableDefs, uuid);
            }
        }

        Schema.instance.fixCFMaxId();
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static IAuthority getAuthority()
    {
        return authority;
    }

    public static int getThriftMaxMessageLength()
    {
        return conf.thrift_max_message_length_in_mb * 1024 * 1024;
    }
    
    public static int getThriftFramedTransportSize() 
    {
        return conf.thrift_framed_transport_size_in_mb * 1024 * 1024;
    }

    /**
     * Creates all storage-related directories.
     * @throws IOException when a disk problem is encountered.
     */
    public static void createAllDirectories() throws IOException
    {
        try {
            if (conf.data_file_directories.length == 0)
            {
                throw new ConfigurationException("At least one DataFileDirectory must be specified");
            }
            for ( String dataFileDirectory : conf.data_file_directories )
                FileUtils.createDirectory(dataFileDirectory);
            if (conf.commitlog_directory == null)
            {
                throw new ConfigurationException("commitlog_directory must be specified");
            }
            FileUtils.createDirectory(conf.commitlog_directory);
            if (conf.saved_caches_directory == null)
            {
                throw new ConfigurationException("saved_caches_directory must be specified");
            }
            FileUtils.createDirectory(conf.saved_caches_directory);
        }
        catch (ConfigurationException ex) {
            logger.error("Fatal error: " + ex.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
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

    public static String getJobTrackerAddress()
    {
        return conf.job_tracker_host;
    }
    
    public static int getColumnIndexSize()
    {
    	return conf.column_index_size_in_kb * 1024;
    }

    public static String getInitialToken()
    {
        return System.getProperty("cassandra.initial_token", conf.initial_token);
    }

    public static String getReplaceToken()
    {
        return System.getProperty("cassandra.replace_token", null);
    }

   public static String getClusterName()
    {
        return conf.cluster_name;
    }

    public static String getJobJarLocation()
    {
        return conf.job_jar_file_location;
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

    public static long getRpcTimeout()
    {
        return conf.rpc_timeout_in_ms;
    }

    public static int getPhiConvictThreshold()
    {
        return conf.phi_convict_threshold;
    }

    public static int getConcurrentReaders()
    {
        return conf.concurrent_reads;
    }

    public static int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public static int getConcurrentReplicators()
    {
        return conf.concurrent_replicates;
    }

    public static int getFlushWriters()
    {
            return conf.memtable_flush_writers;
    }

    public static int getInMemoryCompactionLimit()
    {
        return conf.in_memory_compaction_limit_in_mb * 1024 * 1024;
    }

    public static void setInMemoryCompactionLimit(int sizeInMB)
    {
        conf.in_memory_compaction_limit_in_mb = sizeInMB;
    }

    public static int getConcurrentCompactors()
    {
        return conf.concurrent_compactors;
    }

    public static boolean isMultithreadedCompaction()
    {
        return conf.multithreaded_compaction;
    }

    public static int getCompactionThroughputMbPerSec()
    {
        return conf.compaction_throughput_mb_per_sec;
    }

    public static void setCompactionThroughputMbPerSec(int value)
    {
        conf.compaction_throughput_mb_per_sec = value;
    }

    public static int getStreamThroughputOutboundMegabitsPerSec()
    {
        return conf.stream_throughput_outbound_megabits_per_sec;
    }

    public static void setStreamThroughputOutboundMegabitsPerSec(int value)
    {
        conf.stream_throughput_outbound_megabits_per_sec = value;
    }

    public static String[] getAllDataFileLocations()
    {
        return conf.data_file_directories;
    }

    /**
     * Get a list of data directories for a given table
     * 
     * @param table name of the table.
     * 
     * @return an array of path to the data directories. 
     */
    public static String[] getAllDataFileLocationsForTable(String table)
    {
        String[] tableLocations = new String[conf.data_file_directories.length];

        for (int i = 0; i < conf.data_file_directories.length; i++)
        {
            tableLocations[i] = conf.data_file_directories[i] + File.separator + table;
        }

        return tableLocations;
    }

    public synchronized static String getNextAvailableDataLocation()
    {
        String dataFileDirectory = conf.data_file_directories[currentIndex];
        currentIndex = (currentIndex + 1) % conf.data_file_directories.length;
        return dataFileDirectory;
    }

    public static String getCommitLogLocation()
    {
        return conf.commitlog_directory;
    }

    public static String getSavedCachesLocation()
    {
        return conf.saved_caches_directory;
    }
    
    public static Set<InetAddress> getSeeds()
    {
        return Collections.unmodifiableSet(new HashSet(seedProvider.getSeeds()));
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public static String getDataFileLocationForTable(String table, long expectedCompactedFileSize)
    {
      long maxFreeDisk = 0;
      int maxDiskIndex = 0;
      String dataFileDirectory = null;
      String[] dataDirectoryForTable = getAllDataFileLocationsForTable(table);

      for ( int i = 0 ; i < dataDirectoryForTable.length ; i++ )
      {
        File f = new File(dataDirectoryForTable[i]);
        if( maxFreeDisk < f.getUsableSpace())
        {
          maxFreeDisk = f.getUsableSpace();
          maxDiskIndex = i;
        }
      }
        logger.debug("expected data files size is {}; largest free partition has {} bytes free",
                     expectedCompactedFileSize, maxFreeDisk);
      // Load factor of 0.9 we do not want to use the entire disk that is too risky.
      maxFreeDisk = (long)(0.9 * maxFreeDisk);
      if( expectedCompactedFileSize < maxFreeDisk )
      {
        dataFileDirectory = dataDirectoryForTable[maxDiskIndex];
        currentIndex = (maxDiskIndex + 1 )%dataDirectoryForTable.length ;
      }
      else
      {
        currentIndex = maxDiskIndex;
      }
        return dataFileDirectory;
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }
    
    public static InetAddress getBroadcastAddress()
    {
        return broadcastAddress;
    }
    
    public static void setBroadcastAddress(InetAddress broadcastAdd)
    {
        broadcastAddress = broadcastAdd;
    }
    
    public static InetAddress getRpcAddress()
    {
        return rpcAddress;
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

    public static double getCommitLogSyncBatchWindow()
    {
        return conf.commitlog_sync_batch_window_in_ms;
    }

    public static int getCommitLogSyncPeriod() {
        return conf.commitlog_sync_period_in_ms;
    }

    public static Config.CommitLogSync getCommitLogSync()
    {
        return conf.commitlog_sync;
    }

    public static Config.DiskAccessMode getDiskAccessMode()
    {
        return conf.disk_access_mode;
    }

    public static Config.DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    public static int getIndexedReadBufferSizeInKB()
    {
        return conf.column_index_size_in_kb;
    }

    public static int getSlicedReadBufferSizeInKB()
    {
        return conf.sliced_buffer_size_in_kb;
    }

    public static boolean isSnapshotBeforeCompaction()
    {
        return conf.snapshot_before_compaction;
    }

    public static boolean isAutoBootstrap()
    {
        return conf.auto_bootstrap;
    }

    public static boolean hintedHandoffEnabled()
    {
        return conf.hinted_handoff_enabled;
    }

    public static int getMaxHintWindow()
    {
        return conf.max_hint_window_in_ms;
    }

    public static Integer getIndexInterval()
    {
        return conf.index_interval;
    }

    public static File getSerializedCachePath(String ksName, String cfName, ColumnFamilyStore.CacheType cacheType)
    {
        return new File(conf.saved_caches_directory + File.separator + ksName + "-" + cfName + "-" + cacheType);
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

    public static EncryptionOptions getEncryptionOptions()
    {
        return conf.encryption_options;
    }

    public static double getFlushLargestMemtablesAt()
    {
        return conf.flush_largest_memtables_at;
    }

    public static double getReduceCacheSizesAt()
    {
        return conf.reduce_cache_sizes_at;
    }

    public static double getReduceCacheCapacityTo()
    {
        return conf.reduce_cache_capacity_to;
    }

    public static int getHintedHandoffThrottleDelay()
    {
        return conf.hinted_handoff_throttle_delay_in_ms;
    }

    public static boolean getPreheatKeyCache()
    {
        return conf.compaction_preheat_key_cache;
    }

    public static void validateMemtableThroughput(int sizeInMB) throws ConfigurationException
    {
        if (sizeInMB <= 0)
            throw new ConfigurationException("memtable_throughput_in_mb must be greater than 0.");
    }

    public static void validateMemtableOperations(double operationsInMillions) throws ConfigurationException
    {
        if (operationsInMillions <= 0)
            throw new ConfigurationException("memtable_operations_in_millions must be greater than 0.0.");
        if (operationsInMillions > Long.MAX_VALUE / 1024 * 1024)
            throw new ConfigurationException("memtable_operations_in_millions must be less than " + Long.MAX_VALUE / 1024 * 1024);
    }

    public static boolean incrementalBackupsEnabled()
    {
        return conf.incremental_backups;
    }

    public static int getFlushQueueSize()
    {
        return conf.memtable_flush_queue_size;
    }

    public static int getTotalMemtableSpaceInMB()
    {
        // should only be called if estimatesRealMemtableSize() is true
        assert conf.memtable_total_space_in_mb > 0;
        return conf.memtable_total_space_in_mb;
    }

    public static long getTotalCommitlogSpaceInMB()
    {
        return conf.commitlog_total_space_in_mb;
    }
}
