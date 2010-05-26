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

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.locator.IEndpointSnitch;

import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URL;

public class DatabaseDescriptor
{
    private static Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    public static final String random = "RANDOM";
    public static final String ophf = "OPHF";
    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress rpcAddress;
    private static Set<InetAddress> seeds = new HashSet<InetAddress>();
    /* Current index into the above list of directories */
    private static int currentIndex = 0;
    private static int consistencyThreads = 4; // not configurable


    static Map<String, KSMetaData> tables = new HashMap<String, KSMetaData>();

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;

    // the path qualified config file (cassandra.yaml) name
    private static String configFileName;

    private static Config.DiskAccessMode indexAccessMode;
    
    private static Config conf;

    private static IAuthenticator authenticator = new AllowAllAuthenticator();

    private final static String STORAGE_CONF_FILE = "cassandra.yaml";

    public static final UUID INITIAL_VERSION = new UUID(4096, 0); // has type nibble set to 1, everything else to zero.
    private static UUID defsVersion = INITIAL_VERSION;

    /**
     * Inspect the classpath to find STORAGE_CONF_FILE.
     */
    static String getStorageConfigPath() throws ConfigurationException
    {
        ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
        URL scpurl = loader.getResource(STORAGE_CONF_FILE);
        if (scpurl != null)
            return scpurl.getFile();
        throw new ConfigurationException("Cannot locate " + STORAGE_CONF_FILE + " on the classpath");
    }

    private static int stageQueueSize_ = 4096;

    static
    {
        try
        {
            
            configFileName = getStorageConfigPath();
            
            if (logger.isDebugEnabled())
                logger.debug("Loading settings from " + configFileName);
            
            InputStream input = new FileInputStream(new File(configFileName));
            org.yaml.snakeyaml.constructor.Constructor constructor = new org.yaml.snakeyaml.constructor.Constructor(Config.class);
            TypeDescription desc = new TypeDescription(Config.class);
            TypeDescription ksDesc = new TypeDescription(Keyspace.class);
            ksDesc.putListPropertyType("column_families", ColumnFamily.class);
            desc.putListPropertyType("keyspaces", Keyspace.class);
            constructor.addTypeDescription(desc);
            constructor.addTypeDescription(ksDesc);
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
            
            if (conf.disk_access_mode == Config.DiskAccessMode.auto)
            {
                conf.disk_access_mode = System.getProperty("os.arch").contains("64") ? Config.DiskAccessMode.mmap : Config.DiskAccessMode.standard;
                indexAccessMode = conf.disk_access_mode;
                logger.info("Auto DiskAccessMode determined to be " + conf.disk_access_mode);
            }
            else if (conf.disk_access_mode == Config.DiskAccessMode.mmap_index_only)
            {
                conf.disk_access_mode = Config.DiskAccessMode.standard;
                indexAccessMode = Config.DiskAccessMode.mmap;
            }
            else
            {
                indexAccessMode = conf.disk_access_mode;
            }

            /* Authentication and authorization backend, implementing IAuthenticator */
            if (conf.authenticator != null)
            {
                try
                {
                    Class cls = Class.forName(conf.authenticator);
                    authenticator = (IAuthenticator) cls.getConstructor().newInstance();
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Invalid authenticator class " + conf.authenticator);
                }
            }

            authenticator.validateConfiguration();
            
            /* Hashing strategy */
            if (conf.partitioner == null)
            {
                throw new ConfigurationException("Missing directive: partitioner");
            }
            try
            {
                Class cls = Class.forName(conf.partitioner);
                partitioner = (IPartitioner) cls.getConstructor().newInstance();
            }
            catch (ClassNotFoundException e)
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
            
            /* Local IP or hostname to bind services to */
            if (conf.listen_address != null)
            {
                if (conf.listen_address.equals("0.0.0.0"))
                {
                    throw new ConfigurationException("listen_address must be a single interface.  See http://wiki.apache.org/cassandra/FAQ#cant_listen_on_ip_any");
                }
                
                try
                {
                    listenAddress = InetAddress.getByName(conf.listen_address);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown listen_address '" + conf.listen_address + "'");
                }
            }
            
            /* Local IP or hostname to bind RPC server to */
            if (conf.rpc_address != null)
                rpcAddress = InetAddress.getByName(conf.rpc_address);
            
            /* end point snitch */
            if (conf.endpoint_snitch == null)
            {
                throw new ConfigurationException("Missing endpoint_snitch directive");
            }
            snitch = createEndpointSnitch(conf.endpoint_snitch);
            
            if (logger.isDebugEnabled() && conf.auto_bootstrap != null)
            {
                logger.debug("setting auto_bootstrap to " + conf.auto_bootstrap);
            }
            
            /* Number of objects in millions in the memtable before it is dumped */
            if (conf.memtable_operations_in_millions != null && conf.memtable_operations_in_millions <= 0)
            {
                throw new ConfigurationException("memtable_operations_in_millions must be a positive double");
            }
            
            if (conf.row_warning_threshold_in_mb != null && conf.row_warning_threshold_in_mb <= 0)
            {
                throw new ConfigurationException("row_warning_threshold_in_mb must be a positive integer");
            }
            
            /* data file and commit log directories. they get created later, when they're needed. */
            if (conf.commitlog_directory != null && conf.data_file_directories != null)
            {
                for (String datadir : conf.data_file_directories)
                {
                    if (datadir.equals(conf.commitlog_directory))
                        throw new ConfigurationException("commitlog_directory must not be the same as any data_file_directories");
                }
            }
            else
            {
                if (conf.commitlog_directory == null)
                    throw new ConfigurationException("commitlog_directory missing");
                if (conf.data_file_directories == null)
                    throw new ConfigurationException("data_file_directories missing; at least one data directory must be specified");
            }

            /* threshold after which commit log should be rotated. */
            if (conf.commitlog_rotation_threshold_in_mb != null)
                CommitLog.setSegmentSize(conf.commitlog_rotation_threshold_in_mb * 1024 * 1024);

            // Hardcoded system tables
            KSMetaData systemMeta = new KSMetaData(Table.SYSTEM_TABLE, null, -1, new CFMetaData[]{CFMetaData.StatusCf,
                                                                                                  CFMetaData.HintsCf,
                                                                                                  CFMetaData.MigrationsCf,
                                                                                                  CFMetaData.SchemaCf
            });
            CFMetaData.map(CFMetaData.StatusCf);
            CFMetaData.map(CFMetaData.HintsCf);
            CFMetaData.map(CFMetaData.MigrationsCf);
            CFMetaData.map(CFMetaData.SchemaCf);
            tables.put(Table.SYSTEM_TABLE, systemMeta);
            
            /* Load the seeds for node contact points */
            if (conf.seeds == null || conf.seeds.length <= 0)
            {
                throw new ConfigurationException("seeds missing; a minimum of one seed is required.");
            }
            for( int i = 0; i < conf.seeds.length; ++i )
            {
                seeds.add(InetAddress.getByName(conf.seeds[i]));
            }
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
        catch (YAMLException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static IEndpointSnitch createEndpointSnitch(String endpointSnitchClassName) throws ConfigurationException
    {
        IEndpointSnitch snitch;
        Class cls;
        try
        {
            cls = Class.forName(endpointSnitchClassName);
        }
        catch (ClassNotFoundException e)
        {
            throw new ConfigurationException("Unable to load endpointsnitch class " + endpointSnitchClassName);
        }
        Constructor ctor;
        try
        {
            ctor = cls.getConstructor();
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException("No default constructor found in " + endpointSnitchClassName);
        }
        try
        {
            snitch = (IEndpointSnitch)ctor.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new ConfigurationException("endpointsnitch class " + endpointSnitchClassName + "is abstract");
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Access to " + endpointSnitchClassName + " constructor was rejected");
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException)e.getCause();
            throw new ConfigurationException("Error instantiating " + endpointSnitchClassName + " " + e.getMessage());
        }
        return snitch;
    }
    
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
            for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
            {
                File dataPath = new File(dataDir);
                if (dataPath.exists() && dataPath.isDirectory())
                {
                    // see if there are other directories present.
                    int dirCount = dataPath.listFiles(new FileFilter()
                    {
                        @Override
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
                logger.info("Found table data in data directories. Consider using JMX to call org.apache.cassandra.service.StorageService.loadSchemaFromYaml().");
            else
                logger.info("Consider using JMX to org.apache.cassandra.service.StorageService.loadSchemaFromYaml() or set up a schema using the system_* calls provided via thrift.");
            
        }
        else
        {
            logger.info("Loading schema version " + uuid.toString());
            Collection<KSMetaData> tableDefs = DefsTable.loadFromStorage(uuid);   
            for (KSMetaData def : tableDefs)
            {
                for (CFMetaData cfm : def.cfMetaData().values())
                {
                    try
                    {
                        CFMetaData.map(cfm);
                    }
                    catch (ConfigurationException ex)
                    {
                        throw new IOError(ex);
                    }
                }
                DatabaseDescriptor.setTableDefinition(def, uuid);
                // this part creates storage and jmx objects.
                Table.open(def.name);
            }
            
            // since we loaded definitions from local storage, log a warning if definitions exist in yaml.
            
            if (conf.keyspaces != null && conf.keyspaces.size() > 0)
                logger.warn("Schema definitions were defined both locally and in " + STORAGE_CONF_FILE +
                    ". Definitions in " + STORAGE_CONF_FILE + " were ignored.");
            
        }
        CFMetaData.fixMaxId();
    }

    /** reads xml. doesn't populate any internal structures. */
    public static Collection<KSMetaData> readTablesFromYaml() throws ConfigurationException
    {
        List<KSMetaData> defs = new ArrayList<KSMetaData>();
        
        
        /* Read the table related stuff from config */
        for (Keyspace keyspace : conf.keyspaces)
        {
            /* parsing out the table name */
            if (keyspace.name == null)
            {
                throw new ConfigurationException("Keyspace name attribute is required");
            }
            
            if (keyspace.name.equalsIgnoreCase(Table.SYSTEM_TABLE))
            {
                throw new ConfigurationException("'system' is a reserved table name for Cassandra internals");
            }
            
            /* See which replica placement strategy to use */
            if (keyspace.replica_placement_strategy == null)
            {
                throw new ConfigurationException("Missing replica_placement_strategy directive for " + keyspace.name);
            }
            Class<? extends AbstractReplicationStrategy> strategyClass = null;
            try
            {
                strategyClass = (Class<? extends AbstractReplicationStrategy>) Class.forName(keyspace.replica_placement_strategy);
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException("Invalid replicaplacementstrategy class " + keyspace.replica_placement_strategy);
            }
            
            /* Data replication factor */
            if (keyspace.replication_factor == null)
            {
                throw new ConfigurationException("Missing replication_factor directory for keyspace " + keyspace.name);
            }
            
            int size2 = keyspace.column_families.length;
            CFMetaData[] cfDefs = new CFMetaData[size2];
            int j = 0;
            for (ColumnFamily cf : keyspace.column_families)
            {
                if (cf.name == null)
                {
                    throw new ConfigurationException("ColumnFamily name attribute is required");
                }
                if (cf.name.contains("-"))
                {
                    throw new ConfigurationException("ColumnFamily names cannot contain hyphens");
                }
                
                // Parse out the column comparator
                AbstractType comparator = getComparator(cf.compare_with);
                AbstractType subcolumnComparator = null;
                ColumnFamilyType cfType = cf.column_type == null ? ColumnFamilyType.Standard : cf.column_type;
                if (cfType == ColumnFamilyType.Super)
                {
                    subcolumnComparator = getComparator(cf.compare_subcolumns_with);
                }
                else if (cf.compare_subcolumns_with != null)
                {
                    throw new ConfigurationException("compare_subcolumns_with is only a valid attribute on super columnfamilies (not regular columnfamily " + cf.name + ")");
                }
                
                if (cf.read_repair_chance < 0.0 || cf.read_repair_chance > 1.0)
                {                        
                    throw new ConfigurationException("read_repair_chance must be between 0.0 and 1.0");
                }
                cfDefs[j++] = new CFMetaData(keyspace.name, cf.name, cfType, ClockType.Timestamp, comparator, subcolumnComparator, cf.comment, cf.rows_cached, cf.preload_row_cache, cf.keys_cached, cf.read_repair_chance);
            }
            defs.add(new KSMetaData(keyspace.name, strategyClass, keyspace.replication_factor, cfDefs));
            
        }

        return defs;
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static boolean isThriftFramed()
    {
        return conf.thrift_framed_transport;
    }

    public static AbstractType getComparator(String compareWith) throws ConfigurationException
//    throws ConfigurationException, TransformerException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException
    {
        Class<? extends AbstractType> typeClass;
        
        if (compareWith == null)
        {
            typeClass = BytesType.class;
        }
        else
        {
            String className = compareWith.contains(".") ? compareWith : "org.apache.cassandra.db.marshal." + compareWith;
            try
            {
                typeClass = (Class<? extends AbstractType>)Class.forName(className);
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException("Unable to load class " + className);
            }
        }
        try
        {
            return typeClass.getConstructor().newInstance();
        }
        catch (InstantiationException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IllegalAccessException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (InvocationTargetException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (NoSuchMethodException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
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
        }
        catch (ConfigurationException ex) {
            logger.error("Fatal error: " + ex.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
    }

    public static int getGcGraceInSeconds()
    {
        return conf.gc_grace_seconds;
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
    }
    
    public static IEndpointSnitch getEndpointSnitch()
    {
        return snitch;
    }

    public static Class<? extends AbstractReplicationStrategy> getReplicaPlacementStrategyClass(String table)
    {
    	KSMetaData meta = tables.get(table);
    	if (meta == null)
            throw new RuntimeException(table + " not found. Failure to call loadSchemas() perhaps?");
        return meta.strategyClass;
    }
    
    public static String getJobTrackerAddress()
    {
        return conf.job_tracker_host;
    }
    
    public static int getColumnIndexSize()
    {
    	return conf.column_index_size_in_kb * 1024;
    }

    public static int getMemtableLifetimeMS()
    {
      return conf.memtable_flush_after_mins * 60 * 1000;
    }

    public static String getInitialToken()
    {
      return conf.initial_token;
    }

    public static int getMemtableThroughput()
    {
      return conf.memtable_throughput_in_mb;
    }

    public static double getMemtableOperations()
    {
      return conf.memtable_operations_in_millions;
    }

    public static String getClusterName()
    {
        return conf.cluster_name;
    }

    public static String getConfigFileName() {
        return configFileName;
    }

    public static String getJobJarLocation()
    {
        return conf.job_jar_file_location;
    }
    
    public static Map<String, CFMetaData> getTableMetaData(String tableName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        assert ksm != null;
        return ksm.cfMetaData();
    }

    /*
     * Given a table name & column family name, get the column family
     * meta data. If the table name or column family name is not valid
     * this function returns null.
     */
    public static CFMetaData getCFMetaData(String tableName, String cfName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        if (ksm == null)
            return null;
        return ksm.cfMetaData().get(cfName);
    }
    
    public static CFMetaData getCFMetaData(int cfid)
    {
        Pair<String,String> cf = CFMetaData.getCF(cfid);
        if (cf == null)
            return null;
        return getCFMetaData(cf.left, cf.right);
    }

    public static ColumnFamilyType getColumnFamilyType(String tableName, String cfName)
    {
        assert tableName != null && cfName != null;
        CFMetaData cfMetaData = getCFMetaData(tableName, cfName);
        
        if (cfMetaData == null)
            return null;
        return cfMetaData.cfType;
    }

    public static ClockType getClockType(String tableName, String cfName)
    {
        assert tableName != null && cfName != null;
        CFMetaData cfMetaData = getCFMetaData(tableName, cfName);

        assert (cfMetaData != null);
        return cfMetaData.clockType;
    }

    public static Set<String> getTables()
    {
        return tables.keySet();
    }

    public static List<String> getNonSystemTables()
    {
        List<String> tableslist = new ArrayList<String>(tables.keySet());
        tableslist.remove(Table.SYSTEM_TABLE);
        return Collections.unmodifiableList(tableslist);
    }

    public static int getStoragePort()
    {
        return conf.storage_port;
    }

    public static int getRpcPort()
    {
        return conf.rpc_port;
    }

    public static int getReplicationFactor(String table)
    {
        return tables.get(table).replicationFactor;
    }

    public static int getQuorum(String table)
    {
        return (tables.get(table).replicationFactor / 2) + 1;
    }

    public static long getRpcTimeout()
    {
        return conf.rpc_timeout_in_ms;
    }

    public static int getPhiConvictThreshold()
    {
        return conf.phi_convict_threshold;
    }

    public static int getConsistencyThreads()
    {
        return consistencyThreads;
    }

    public static int getConcurrentReaders()
    {
        return conf.concurrent_reads;
    }

    public static int getConcurrentWriters()
    {
        return conf.concurrent_writes;
    }

    public static long getRowWarningThreshold()
    {
        return conf.row_warning_threshold_in_mb * 1024 * 1024;
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

    public static String getLogFileLocation()
    {
        return conf.commitlog_directory;
    }

    public static Set<InetAddress> getSeeds()
    {
        return seeds;
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
    
    public static AbstractType getComparator(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfmd = getCFMetaData(tableName, cfName);
        if (cfmd == null)
            throw new IllegalArgumentException("Unknown ColumnFamily " + cfName + " in keyspace " + tableName);
        return cfmd.comparator;
    }

    public static AbstractType getSubComparator(String tableName, String cfName)
    {
        assert tableName != null;
        return getCFMetaData(tableName, cfName).subcolumnComparator;
    }

    public static int getStageQueueSize()
    {
        return stageQueueSize_;
    }

    /**
     * @return The absolute number of keys that should be cached per table.
     */
    public static int getKeysCachedFor(String tableName, String columnFamilyName, long expectedKeys)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cfm.keyCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedKeys), Integer.MAX_VALUE);
    }

    /**
     * @return The absolute number of rows that should be cached for the columnfamily.
     */
    public static int getRowsCachedFor(String tableName, String columnFamilyName, long expectedRows)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cfm.rowCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedRows), Integer.MAX_VALUE);
    }

    public static KSMetaData getTableDefinition(String table)
    {
        return tables.get(table);
    }

    // todo: this is wrong. the definitions need to be moved into their own class that can only be updated by the
    // process of mutating an individual keyspace, rather than setting manually here.
    public static void setTableDefinition(KSMetaData ksm, UUID newVersion)
    {
        tables.put(ksm.name, ksm);
        DatabaseDescriptor.defsVersion = newVersion;
        StorageService.instance.initReplicationStrategy(ksm.name);
    }
    
    public static void clearTableDefinition(KSMetaData ksm, UUID newVersion)
    {
        tables.remove(ksm.name);
        StorageService.instance.clearReplicationStrategy(ksm.name);
        DatabaseDescriptor.defsVersion = newVersion;
    }
    
    public static UUID getDefsVersion()
    {
        return defsVersion;
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }
    
    public static InetAddress getRpcAddress()
    {
        return rpcAddress;
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

    public static double getFlushDataBufferSizeInMB()
    {
        return conf.flush_data_buffer_size_in_mb;
    }

    public static double getFlushIndexBufferSizeInMB()
    {
        return conf.flush_index_buffer_size_in_mb;
    }

    public static int getIndexedReadBufferSizeInKB()
    {
        return conf.column_index_size_in_kb;
    }

    public static int getSlicedReadBufferSizeInKB()
    {
        return conf.sliced_buffer_size_in_kb;
    }

    public static int getBMTThreshold()
    {
        return conf.binary_memtable_throughput_in_mb;
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
}
