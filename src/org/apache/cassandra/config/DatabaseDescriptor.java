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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TypeInfo;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.XMLUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class DatabaseDescriptor
{
    public static final String random_ = "RANDOM";
    public static final String ophf_ = "OPHF";
    private static int storagePort_ = 7000;
    private static int controlPort_ = 7001;
    private static int httpPort_ = 7002;
    private static String clusterName_ = "Test";
    private static int replicationFactor_ = 3;
    private static long rpcTimeoutInMillis_ = 2000;
    private static Set<String> seeds_ = new HashSet<String>();
    private static String metadataDirectory_;
    private static String snapshotDirectory_;
    /* Keeps the list of Ganglia servers to contact */
    private static String[] gangliaServers_ ;
    /* Keeps the list of map output directories */
    private static String[] mapOutputDirectories_;
    /* Keeps the list of data file directories */
    private static String[] dataFileDirectories_;
    /* Current index into the above list of directories */
    private static int currentIndex_ = 0;
    private static String logFileDirectory_;
    private static String bootstrapFileDirectory_;
    private static int logRotationThreshold_ = 128*1024*1024;
    private static boolean fastSync_ = false;
    private static boolean rackAware_ = false;
    private static int threadsPerPool_ = 4;
    private static List<String> tables_ = new ArrayList<String>();
    private static Set<String> applicationColumnFamilies_ = new HashSet<String>();

    // Default descriptive names for use in CQL. The user can override
    // these choices in the config file. These are not case sensitive.
    // Hence, these are stored in UPPER case for easy comparison.
    private static String d_rowKey_           = "ROW_KEY";
    private static String d_superColumnMap_   = "SUPER_COLUMN_MAP";
    private static String d_superColumnKey_   = "SUPER_COLUMN_KEY";
    private static String d_columnMap_        = "COLUMN_MAP";
    private static String d_columnKey_        = "COLUMN_KEY";
    private static String d_columnValue_      = "COLUMN_VALUE";
    private static String d_columnTimestamp_  = "COLUMN_TIMESTAMP";

    /*
     * A map from table names to the set of column families for the table and the
     * corresponding meta data for that column family.
    */
    private static Map<String, Map<String, CFMetaData>> tableToCFMetaDataMap_;
    /* Hashing strategy Random or OPHF */
    private static String hashingStrategy_ = DatabaseDescriptor.random_;
    /* if the size of columns or super-columns are more than this, indexing will kick in */
    private static int columnIndexSizeInKB_;
    /* Size of touch key cache */
    private static int touchKeyCacheSize_ = 1024;
    /* Number of hours to keep a memtable in memory */
    private static int memtableLifetime_ = 6;
    /* Size of the memtable in memory before it is dumped */
    private static int memtableSize_ = 128;
    /* Number of objects in millions in the memtable before it is dumped */
    private static int memtableObjectCount_ = 1;
    /* 
     * This parameter enables or disables consistency checks. 
     * If set to false the read repairs are disable for very
     * high throughput on reads but at the cost of consistency.
    */
    private static boolean doConsistencyCheck_ = true;
    /* Address of ZooKeeper cell */
    private static String zkAddress_;
    /* Callout directories */
    private static String calloutLocation_;
    /* Job Jar Location */
    private static String jobJarFileLocation_;
    /* Address where to run the job tracker */
    private static String jobTrackerHost_;    
    /* Zookeeper session timeout. */
    private static int zkSessionTimeout_ = 30000;
    
    // the path qualified config file (storage-conf.xml) name
    private static String configFileName_;

    static
    {
        try
        {
            String file = System.getProperty("storage-config") + System.getProperty("file.separator") + "storage-conf.xml";
            String os = System.getProperty("os.name");
            XMLUtils xmlUtils = new XMLUtils(file);

            /* Cluster Name */
            clusterName_ = xmlUtils.getNodeValue("/Storage/ClusterName");

            /* Ganglia servers contact list */
            gangliaServers_ = xmlUtils.getNodeValues("/Storage/GangliaServers/GangliaServer");

            /* ZooKeeper's address */
            zkAddress_ = xmlUtils.getNodeValue("/Storage/ZookeeperAddress");

            /* Hashing strategy */
            hashingStrategy_ = xmlUtils.getNodeValue("/Storage/HashingStrategy");
            /* Callout location */
            calloutLocation_ = xmlUtils.getNodeValue("/Storage/CalloutLocation");

            /* JobTracker address */
            jobTrackerHost_ = xmlUtils.getNodeValue("/Storage/JobTrackerHost");

            /* Job Jar file location */
            jobJarFileLocation_ = xmlUtils.getNodeValue("/Storage/JobJarFileLocation");

            /* Zookeeper's session timeout */
            String zkSessionTimeout = xmlUtils.getNodeValue("/Storage/ZookeeperSessionTimeout");
            if ( zkSessionTimeout != null )
                zkSessionTimeout_ = Integer.parseInt(zkSessionTimeout);

            /* Data replication factor */
            String replicationFactor = xmlUtils.getNodeValue("/Storage/ReplicationFactor");
            if ( replicationFactor != null )
                replicationFactor_ = Integer.parseInt(replicationFactor);

            /* RPC Timeout */
            String rpcTimeoutInMillis = xmlUtils.getNodeValue("/Storage/RpcTimeoutInMillis");
            if ( rpcTimeoutInMillis != null )
                rpcTimeoutInMillis_ = Integer.parseInt(rpcTimeoutInMillis);

            /* Thread per pool */
            String threadsPerPool = xmlUtils.getNodeValue("/Storage/ThreadsPerPool");
            if ( threadsPerPool != null )
                threadsPerPool_ = Integer.parseInt(threadsPerPool);

            /* TCP port on which the storage system listens */
            String port = xmlUtils.getNodeValue("/Storage/StoragePort");
            if ( port != null )
                storagePort_ = Integer.parseInt(port);

            /* UDP port for control messages */
            port = xmlUtils.getNodeValue("/Storage/ControlPort");
            if ( port != null )
                controlPort_ = Integer.parseInt(port);

            /* HTTP port for HTTP messages */
            port = xmlUtils.getNodeValue("/Storage/HttpPort");
            if ( port != null )
                httpPort_ = Integer.parseInt(port);

            /* Touch Key Cache Size */
            String touchKeyCacheSize = xmlUtils.getNodeValue("/Storage/TouchKeyCacheSize");
            if ( touchKeyCacheSize != null )
                touchKeyCacheSize_ = Integer.parseInt(touchKeyCacheSize);

            /* Number of days to keep the memtable around w/o flushing */
            String lifetime = xmlUtils.getNodeValue("/Storage/MemtableLifetimeInDays");
            if ( lifetime != null )
                memtableLifetime_ = Integer.parseInt(lifetime);

            /* Size of the memtable in memory in MB before it is dumped */
            String memtableSize = xmlUtils.getNodeValue("/Storage/MemtableSizeInMB");
            if ( memtableSize != null )
                memtableSize_ = Integer.parseInt(memtableSize);
            /* Number of objects in millions in the memtable before it is dumped */
            String memtableObjectCount = xmlUtils.getNodeValue("/Storage/MemtableObjectCountInMillions");
            if ( memtableObjectCount != null )
                memtableObjectCount_ = Integer.parseInt(memtableObjectCount);

            /* This parameter enables or disables consistency checks.
             * If set to false the read repairs are disable for very
             * high throughput on reads but at the cost of consistency.*/
            String doConsistencyCheck = xmlUtils.getNodeValue("/Storage/DoConsistencyChecksBoolean");
            if ( doConsistencyCheck != null )
                doConsistencyCheck_ = Boolean.parseBoolean(doConsistencyCheck);


            /* read the size at which we should do column indexes */
            String columnIndexSizeInKB = xmlUtils.getNodeValue("/Storage/ColumnIndexSizeInKB");
            if(columnIndexSizeInKB == null)
            {
                columnIndexSizeInKB_ = 64;
            }
            else
            {
                columnIndexSizeInKB_ = Integer.parseInt(columnIndexSizeInKB);
            }

            /* metadata directory */
            metadataDirectory_ = xmlUtils.getNodeValue("/Storage/MetadataDirectory");
            if ( metadataDirectory_ != null )
                FileUtils.createDirectory(metadataDirectory_);
            else
            {
                if ( os.equals("Linux") )
                {
                    metadataDirectory_ = "/var/storage/system";
                }
            }

            /* snapshot directory */
            snapshotDirectory_ = xmlUtils.getNodeValue("/Storage/SnapshotDirectory");
            if ( snapshotDirectory_ != null )
                FileUtils.createDirectory(snapshotDirectory_);
            else
            {
                    snapshotDirectory_ = metadataDirectory_ + System.getProperty("file.separator") + "snapshot";
            }

            /* map output directory */
            mapOutputDirectories_ = xmlUtils.getNodeValues("/Storage/MapOutputDirectories/MapOutputDirectory");
            if ( mapOutputDirectories_.length > 0 )
            {
                for ( String mapOutputDirectory : mapOutputDirectories_ )
                    FileUtils.createDirectory(mapOutputDirectory);
            }

            /* data file directory */
            dataFileDirectories_ = xmlUtils.getNodeValues("/Storage/DataFileDirectories/DataFileDirectory");
            if ( dataFileDirectories_.length > 0 )
            {
                for ( String dataFileDirectory : dataFileDirectories_ )
                    FileUtils.createDirectory(dataFileDirectory);
            }
            else
            {
                if ( os.equals("Linux") )
                {
                    dataFileDirectories_ = new String[]{"/var/storage/data"};
                }
            }

            /* bootstrap file directory */
            bootstrapFileDirectory_ = xmlUtils.getNodeValue("/Storage/BootstrapFileDirectory");
            if ( bootstrapFileDirectory_ != null )
                FileUtils.createDirectory(bootstrapFileDirectory_);
            else
            {
                if ( os.equals("Linux") )
                {
                    bootstrapFileDirectory_ = "/var/storage/bootstrap";
                }
            }

            /* commit log directory */
            logFileDirectory_ = xmlUtils.getNodeValue("/Storage/CommitLogDirectory");
            if ( logFileDirectory_ != null )
                FileUtils.createDirectory(logFileDirectory_);
            else
            {
                if ( os.equals("Linux") )
                {
                    logFileDirectory_ = "/var/storage/commitlog";
                }
            }

            /* threshold after which commit log should be rotated. */
            String value = xmlUtils.getNodeValue("/Storage/CommitLogRotationThresholdInMB");
            if ( value != null)
                logRotationThreshold_ = Integer.parseInt(value) * 1024 * 1024;

            /* fast sync option */
            value = xmlUtils.getNodeValue("/Storage/CommitLogFastSync");
            if ( value != null )
                fastSync_ = Boolean.parseBoolean(value);

            tableToCFMetaDataMap_ = new HashMap<String, Map<String, CFMetaData>>();

            /* Rack Aware option */
            value = xmlUtils.getNodeValue("/Storage/RackAware");
            if ( value != null )
                rackAware_ = Boolean.parseBoolean(value);

            /* Read the table related stuff from config */
            NodeList tables = xmlUtils.getRequestedNodeList("/Storage/Tables/Table");
            int size = tables.getLength();
            if (size == 0) {
                throw new UnsupportedOperationException("A Table must be configured");
            }
            for ( int i = 0; i < size; ++i )
            {
                Node table = tables.item(i);

                /* parsing out the table name */
                String tName = XMLUtils.getAttributeValue(table, "Name");
                tables_.add(tName);
                tableToCFMetaDataMap_.put(tName, new HashMap<String, CFMetaData>());

                String xqlTable = "/Storage/Tables/Table[@Name='" + tName + "']/";
                NodeList columnFamilies = xmlUtils.getRequestedNodeList(xqlTable + "ColumnFamily");

                // get name of the rowKey for this table
                String n_rowKey = xmlUtils.getNodeValue(xqlTable + "RowKey");
                if (n_rowKey == null)
                    n_rowKey = d_rowKey_;

                //NodeList columnFamilies = xmlUtils.getRequestedNodeList(table, "ColumnFamily");
                int size2 = columnFamilies.getLength();

                for ( int j = 0; j < size2; ++j )
                {
                    Node columnFamily = columnFamilies.item(j);
                    String cName = XMLUtils.getAttributeValue(columnFamily, "Name");
                    if (cName == null)
                    {
                        throw new IllegalArgumentException("ColumnFamily element missing Name attribute: " + columnFamily);
                    }
                    String xqlCF = xqlTable + "ColumnFamily[@Name='" + cName + "']/";

                    /* squirrel away the application column families */
                    applicationColumnFamilies_.add(cName);

                    // Parse out the column type
                    String columnType = xmlUtils.getAttributeValue(columnFamily, "ColumnType");
                    columnType = ColumnFamily.getColumnType(columnType);

                    // Parse out the column family sorting property for columns
                    String columnIndexProperty = XMLUtils.getAttributeValue(columnFamily, "ColumnSort");
                    String columnIndexType = ColumnFamily.getColumnSortProperty(columnIndexProperty);

                    // Parse out user-specified logical names for the various dimensions
                    // of a the column family from the config.
                    String n_superColumnMap = xmlUtils.getNodeValue(xqlCF + "SuperColumnMap");
                    if (n_superColumnMap == null)
                        n_superColumnMap = d_superColumnMap_;

                    String n_superColumnKey = xmlUtils.getNodeValue(xqlCF + "SuperColumnKey");
                    if (n_superColumnKey == null)
                        n_superColumnKey = d_superColumnKey_;

                    String n_columnMap = xmlUtils.getNodeValue(xqlCF + "ColumnMap");
                    if (n_columnMap == null)
                        n_columnMap = d_columnMap_;

                    String n_columnKey = xmlUtils.getNodeValue(xqlCF + "ColumnKey");
                    if (n_columnKey == null)
                        n_columnKey = d_columnKey_;

                    String n_columnValue = xmlUtils.getNodeValue(xqlCF + "ColumnValue");
                    if (n_columnValue == null)
                        n_columnValue = d_columnValue_;

                    String n_columnTimestamp = xmlUtils.getNodeValue(xqlCF + "ColumnTimestamp");
                    if (n_columnTimestamp == null)
                        n_columnTimestamp = d_columnTimestamp_;

                    // now populate the column family meta data and
                    // insert it into the table dictionary.
                    CFMetaData cfMetaData = new CFMetaData();

                    cfMetaData.tableName = tName;
                    cfMetaData.cfName = cName;

                    cfMetaData.columnType = columnType;
                    cfMetaData.indexProperty_ = columnIndexType;

                    cfMetaData.n_rowKey = n_rowKey;
                    cfMetaData.n_columnMap = n_columnMap;
                    cfMetaData.n_columnKey = n_columnKey;
                    cfMetaData.n_columnValue = n_columnValue;
                    cfMetaData.n_columnTimestamp = n_columnTimestamp;
                    if ("Super".equals(columnType))
                    {
                        cfMetaData.n_superColumnKey = n_superColumnKey;
                        cfMetaData.n_superColumnMap = n_superColumnMap;
                    }

                    tableToCFMetaDataMap_.get(tName).put(cName, cfMetaData);
                }
            }

            /* Load the seeds for node contact points */
            String[] seeds = xmlUtils.getNodeValues("/Storage/Seeds/Seed");
            for( int i = 0; i < seeds.length; ++i )
            {
                seeds_.add( seeds[i] );
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            storeMetadata();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * Create the metadata tables. This table has information about
     * the table name and the column families that make up the table.
     * Each column family also has an associated ID which is an int.
    */
    private static void storeMetadata() throws IOException
    {
        AtomicInteger idGenerator = new AtomicInteger(0);
        Set<String> tables = tableToCFMetaDataMap_.keySet();

        for ( String table : tables )
        {
            Table.TableMetadata tmetadata = Table.TableMetadata.instance();
            if (tmetadata.isEmpty())
            {
                tmetadata = Table.TableMetadata.instance();
                /* Column families associated with this table */
                Map<String, CFMetaData> columnFamilies = tableToCFMetaDataMap_.get(table);

                for (String columnFamily : columnFamilies.keySet())
                {
                    tmetadata.add(columnFamily, idGenerator.getAndIncrement(), DatabaseDescriptor.getColumnType(columnFamily));
                }

                /*
                 * Here we add all the system related column families.
                */
                /* Add the TableMetadata column family to this map. */
                tmetadata.add(Table.TableMetadata.cfName_, idGenerator.getAndIncrement());
                /* Add the LocationInfo column family to this map. */
                tmetadata.add(SystemTable.cfName_, idGenerator.getAndIncrement());
                /* Add the recycle column family to this map. */
                tmetadata.add(Table.recycleBin_, idGenerator.getAndIncrement());
                /* Add the Hints column family to this map. */
                tmetadata.add(Table.hints_, idGenerator.getAndIncrement(), ColumnFamily.getColumnType("Super"));
                tmetadata.apply();
                idGenerator.set(0);
            }
        }
    }


    
    public static String getHashingStrategy()
    {
        return hashingStrategy_;
    }
    
    public static String getZkAddress()
    {
        return zkAddress_;
    }
    
    public static String getCalloutLocation()
    {
        return calloutLocation_;
    }
    
    public static String getJobTrackerAddress()
    {
        return jobTrackerHost_;
    }
    
    public static int getZkSessionTimeout()
    {
        return zkSessionTimeout_;
    }

    public static int getColumnIndexSize()
    {
    	return columnIndexSizeInKB_ * 1024;
    }

   
    public static int getMemtableLifetime()
    {
      return memtableLifetime_;
    }

    public static int getMemtableSize()
    {
      return memtableSize_;
    }

    public static int getMemtableObjectCount()
    {
      return memtableObjectCount_;
    }

    public static boolean getConsistencyCheck()
    {
      return doConsistencyCheck_;
    }

    public static String getClusterName()
    {
        return clusterName_;
    }

    public static String getConfigFileName() {
        return configFileName_;
    }
    
    public static boolean isApplicationColumnFamily(String columnFamily)
    {
        return applicationColumnFamilies_.contains(columnFamily);
    }

    public static int getTouchKeyCacheSize()
    {
        return touchKeyCacheSize_;
    }
    
    public static String getJobJarLocation()
    {
        return jobJarFileLocation_;
    }

    public static String getGangliaServers()
    {
    	StringBuilder sb = new StringBuilder();
    	for ( int i = 0; i < gangliaServers_.length; ++i )
    	{
    		sb.append(gangliaServers_[i]);
    		if ( i != (gangliaServers_.length - 1) )
    			sb.append(", ");
    	}
    	return sb.toString();
    }
    
    public static Map<String, CFMetaData> getTableMetaData(String table)
    {
        return tableToCFMetaDataMap_.get(table);
    }

    /*
     * Given a table name & column family name, get the column family
     * meta data. If the table name or column family name is not valid
     * this function returns null.
     */
    public static CFMetaData getCFMetaData(String table, String cfName)
    {
        Map<String, CFMetaData> cfInfo = tableToCFMetaDataMap_.get(table);
        if (cfInfo == null)
            return null;
        
        return cfInfo.get(cfName);
    }
    
    public static String getColumnType(String cfName)
    {
        String table = getTables().get(0);
        CFMetaData cfMetaData = getCFMetaData(table, cfName);
        
        if (cfMetaData == null)
            return null;
        return cfMetaData.columnType;
    }

    public static boolean isNameSortingEnabled(String cfName)
    {
        String table = getTables().get(0);
        CFMetaData cfMetaData = getCFMetaData(table, cfName);

        if (cfMetaData == null)
            return false;

    	return "Name".equals(cfMetaData.indexProperty_);
    }
    
    public static boolean isTimeSortingEnabled(String cfName)
    {
        String table = getTables().get(0);
        CFMetaData cfMetaData = getCFMetaData(table, cfName);

        if (cfMetaData == null)
            return false;

        return "Time".equals(cfMetaData.indexProperty_);
    }
    

    public static List<String> getTables()
    {
        return tables_;
    }

    public static void  setTables(String table)
    {
        tables_.add(table);
    }

    public static int getStoragePort()
    {
        return storagePort_;
    }

    public static int getControlPort()
    {
        return controlPort_;
    }

    public static int getHttpPort()
    {
        return httpPort_;
    }

    public static int getReplicationFactor()
    {
        return replicationFactor_;
    }

    public static long getRpcTimeout()
    {
        return rpcTimeoutInMillis_;
    }

    public static int getThreadsPerPool()
    {
        return threadsPerPool_;
    }

    public static String getMetadataDirectory()
    {
        return metadataDirectory_;
    }

    public static void setMetadataDirectory(String metadataDirectory)
    {
        metadataDirectory_ = metadataDirectory;
    }

    public static String getSnapshotDirectory()
    {
        return snapshotDirectory_;
    }

    public static void setSnapshotDirectory(String snapshotDirectory)
    {
    	snapshotDirectory_ = snapshotDirectory;
    }
    
    public static String[] getAllMapOutputDirectories()
    {
        return mapOutputDirectories_;
    }
    
    public static String getMapOutputLocation()
    {
        String mapOutputDirectory = mapOutputDirectories_[currentIndex_];
        return mapOutputDirectory;
    }

    public static String[] getAllDataFileLocations()
    {
        return dataFileDirectories_;
    }

    public static String getDataFileLocation()
    {
    	String dataFileDirectory = dataFileDirectories_[currentIndex_];
        return dataFileDirectory;
    }
    
    public static String getCompactionFileLocation()
    {
    	String dataFileDirectory = dataFileDirectories_[currentIndex_];
    	currentIndex_ = (currentIndex_ + 1 )%dataFileDirectories_.length ;
        return dataFileDirectory;
    }

    public static String getBootstrapFileLocation()
    {
        return bootstrapFileDirectory_;
    }

    public static void setBootstrapFileLocation(String bfLocation)
    {
        bootstrapFileDirectory_ = bfLocation;
    }

    public static int getLogFileSizeThreshold()
    {
        return logRotationThreshold_;
    }

    public static String getLogFileLocation()
    {
        return logFileDirectory_;
    }

    public static void setLogFileLocation(String logLocation)
    {
        logFileDirectory_ = logLocation;
    }

    public static boolean isFastSync()
    {
        return fastSync_;
    }

    public static boolean isRackAware()
    {
        return rackAware_;
    }

    public static Set<String> getSeeds()
    {
        return seeds_;
    }

    public static String getColumnFamilyType(String cfName)
    {
        String cfType = getColumnType(cfName);
        if ( cfType == null )
            cfType = "Standard";
    	return cfType;
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public static String getCompactionFileLocation(long expectedCompactedFileSize)
    {
      long maxFreeDisk = 0;
      int maxDiskIndex = 0;
      String dataFileDirectory = null;
      for ( int i = 0 ; i < dataFileDirectories_.length ; i++ )
      {
        File f = new File(dataFileDirectories_[i]);
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
        dataFileDirectory = dataFileDirectories_[maxDiskIndex];
        currentIndex_ = (maxDiskIndex + 1 )%dataFileDirectories_.length ;
      }
      else
      {
        currentIndex_ = maxDiskIndex;
      }
        return dataFileDirectory;
    }
    
    public static TypeInfo getTypeInfo(String cfName)
    {
        String table = DatabaseDescriptor.getTables().get(0);
        CFMetaData cfMetadata = DatabaseDescriptor.getCFMetaData(table, cfName);
        if ( cfMetadata.indexProperty_.equals("Name") )
        {
            return TypeInfo.STRING;
        }
        else
        {
            return TypeInfo.LONG;
        }
    }

    public static Map<String, Map<String, CFMetaData>> getTableToColumnFamilyMap()
    {
        return tableToCFMetaDataMap_;
    }

    public static String getTableName()
    {
        return tables_.get(0);
    }
}
