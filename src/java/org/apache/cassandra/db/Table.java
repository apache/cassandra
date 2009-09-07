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

package org.apache.cassandra.db;

import java.util.*;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BootstrapInitiateMessage;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.FileStruct;
import org.apache.cassandra.io.SSTableWriter;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.io.IStreamComplete;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.db.filter.*;

import org.apache.log4j.Logger;

public class Table 
{
    public static final String SYSTEM_TABLE = "system";

    private static Logger logger_ = Logger.getLogger(Table.class);
    private static final String SNAPSHOT_SUBDIR_NAME = "snapshots";

    /*
     * This class represents the metadata of this Table. The metadata
     * is basically the column family name and the ID associated with
     * this column family. We use this ID in the Commit Log header to
     * determine when a log file that has been rolled can be deleted.
    */
    public static class TableMetadata
    {
        private static HashMap<String,TableMetadata> tableMetadataMap_ = new HashMap<String,TableMetadata>();
        private static Map<Integer, String> idCfMap_ = new HashMap<Integer, String>();
        static
        {
            try
            {
                DatabaseDescriptor.storeMetadata();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static synchronized Table.TableMetadata instance(String tableName) throws IOException
        {
            if ( tableMetadataMap_.get(tableName) == null )
            {
                tableMetadataMap_.put(tableName, new Table.TableMetadata());
            }
            return tableMetadataMap_.get(tableName);
        }

        /* The mapping between column family and the column type. */
        private Map<String, String> cfTypeMap_ = new HashMap<String, String>();
        private Map<String, Integer> cfIdMap_ = new HashMap<String, Integer>();

        public void add(String cf, int id)
        {
            add(cf, id, "Standard");
        }
        
        public void add(String cf, int id, String type)
        {
            if (logger_.isDebugEnabled())
              logger_.debug("adding " + cf + " as " + id);
            assert !idCfMap_.containsKey(id);
            cfIdMap_.put(cf, id);
            idCfMap_.put(id, cf);
            cfTypeMap_.put(cf, type);
        }
        
        public boolean isEmpty()
        {
            return cfIdMap_.isEmpty();
        }

        int getColumnFamilyId(String columnFamily)
        {
            return cfIdMap_.get(columnFamily);
        }

        public static String getColumnFamilyName(int id)
        {
            return idCfMap_.get(id);
        }
        
        String getColumnFamilyType(String cfName)
        {
            return cfTypeMap_.get(cfName);
        }

        Set<String> getColumnFamilies()
        {
            return cfIdMap_.keySet();
        }
        
        int size()
        {
            return cfIdMap_.size();
        }
        
        boolean isValidColumnFamily(String cfName)
        {
            return cfIdMap_.containsKey(cfName);
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder("");
            Set<String> cfNames = cfIdMap_.keySet();
            
            for ( String cfName : cfNames )
            {
                sb.append(cfName);
                sb.append("---->");
                sb.append(cfIdMap_.get(cfName));
                sb.append(System.getProperty("line.separator"));
            }
            
            return sb.toString();
        }

        public static int getColumnFamilyCount()
        {
            return idCfMap_.size();
        }
    }

    /**
     * This is the callback handler that is invoked when we have
     * completely been bootstrapped for a single file by a remote host.
    */
    public static class BootstrapCompletionHandler implements IStreamComplete
    {                
        public void onStreamCompletion(String host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus) throws IOException
        {                        
            /* Parse the stream context and the file to the list of SSTables in the associated Column Family Store. */
            if (streamContext.getTargetFile().contains("-Data.db"))
            {
                String tableName = streamContext.getTable();
                File file = new File( streamContext.getTargetFile() );
                String fileName = file.getName();
                String [] temp = fileName.split("-");
                
                //Open the file to see if all parts are now here
                SSTableReader sstable = null;
                try 
                {
                    sstable = SSTableWriter.renameAndOpen(streamContext.getTargetFile());
                    
                    //TODO add a sanity check that this sstable has all its parts and is ok
                    Table.open(tableName).getColumnFamilyStore(temp[0]).addToList(sstable);
                    logger_.info("Bootstrap added " + sstable.getFilename());
                }
                catch (IOException e)
                {
                    logger_.error("Not able to bootstrap with file " + streamContext.getTargetFile(), e);                    
                }
            }
            
            EndPoint to = new EndPoint(host, DatabaseDescriptor.getStoragePort());
            if (logger_.isDebugEnabled())
              logger_.debug("Sending a bootstrap terminate message with " + streamStatus + " to " + to);
            /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
            StreamContextManager.StreamStatusMessage streamStatusMessage = new StreamContextManager.StreamStatusMessage(streamStatus);
            Message message = StreamContextManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
            MessagingService.getMessagingInstance().sendOneWay(message, to);
            /* If we're done with everything for this host, remove from bootstrap sources */
            if (StreamContextManager.isDone(to.getHost()))
                StorageService.instance().removeBootstrapSource(to);
        }
    }

    public static class BootStrapInitiateVerbHandler implements IVerbHandler
    {
        /*
         * Here we handle the BootstrapInitiateMessage. Here we get the
         * array of StreamContexts. We get file names for the column
         * families associated with the files and replace them with the
         * file names as obtained from the column family store on the
         * receiving end.
        */
        public void doVerb(Message message)
        {
            byte[] body = message.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length); 
            
            try
            {
                BootstrapInitiateMessage biMsg = BootstrapInitiateMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamContext[] streamContexts = biMsg.getStreamContext();                
                
                Map<String, String> fileNames = getNewNames(streamContexts);
                /*
                 * For each of stream context's in the incoming message
                 * generate the new file names and store the new file names
                 * in the StreamContextManager.
                */
                for (StreamContextManager.StreamContext streamContext : streamContexts )
                {                    
                    StreamContextManager.StreamStatus streamStatus = new StreamContextManager.StreamStatus(streamContext.getTargetFile(), streamContext.getExpectedBytes() );
                    String file = getNewFileNameFromOldContextAndNames(fileNames, streamContext);
                    
                    //String file = DatabaseDescriptor.getDataFileLocationForTable(streamContext.getTable()) + File.separator + newFileName + "-Data.db";
                    if (logger_.isDebugEnabled())
                      logger_.debug("Received Data from  : " + message.getFrom() + " " + streamContext.getTargetFile() + " " + file);
                    streamContext.setTargetFile(file);
                    addStreamContext(message.getFrom().getHost(), streamContext, streamStatus);                                            
                }    
                                             
                StreamContextManager.registerStreamCompletionHandler(message.getFrom().getHost(), new Table.BootstrapCompletionHandler());
                /* Send a bootstrap initiation done message to execute on default stage. */
                if (logger_.isDebugEnabled())
                  logger_.debug("Sending a bootstrap initiate done message ...");
                Message doneMessage = new Message( StorageService.getLocalStorageEndPoint(), "", StorageService.bootStrapInitiateDoneVerbHandler_, new byte[0] );
                MessagingService.getMessagingInstance().sendOneWay(doneMessage, message.getFrom());
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
        
        String getNewFileNameFromOldContextAndNames(Map<String, String> fileNames,
                StreamContextManager.StreamContext streamContext)
        {
            File sourceFile = new File( streamContext.getTargetFile() );
            String[] piece = FBUtilities.strip(sourceFile.getName(), "-");
            String cfName = piece[0];
            String ssTableNum = piece[1];
            String typeOfFile = piece[2];             

            String newFileNameExpanded = fileNames.get( streamContext.getTable() + "-" + cfName + "-" + ssTableNum );
            //Drop type (Data.db) from new FileName
            String newFileName = newFileNameExpanded.replace("Data.db", typeOfFile);
            String file = DatabaseDescriptor.getDataFileLocationForTable(streamContext.getTable()) + File.separator + newFileName ;
            return file;
        }

        Map<String, String> getNewNames(StreamContextManager.StreamContext[] streamContexts) throws IOException
        {
            /* 
             * Mapping for each file with unique CF-i ---> new file name. For eg.
             * for a file with name <CF>-<i>-Data.db there is a corresponding
             * <CF>-<i>-Index.db. We maintain a mapping from <CF>-<i> to a newly
             * generated file name.
            */
            Map<String, String> fileNames = new HashMap<String, String>();
            /* Get the distinct entries from StreamContexts i.e have one entry per Data/Index/Filter file set */
            Set<String> distinctEntries = new HashSet<String>();
            for ( StreamContextManager.StreamContext streamContext : streamContexts )
            {
                String[] pieces = FBUtilities.strip(new File(streamContext.getTargetFile()).getName(), "-");
                distinctEntries.add(streamContext.getTable() + "-" + pieces[0] + "-" + pieces[1] );
            }
            
            /* Generate unique file names per entry */
            for ( String distinctEntry : distinctEntries )
            {
                String tableName;
                String[] peices = FBUtilities.strip(distinctEntry, "-");
                tableName = peices[0];
                Table table = Table.open( tableName );
                Map<String, ColumnFamilyStore> columnFamilyStores = table.getColumnFamilyStores();

                ColumnFamilyStore cfStore = columnFamilyStores.get(peices[1]);
                if (logger_.isDebugEnabled())
                  logger_.debug("Generating file name for " + distinctEntry + " ...");
                fileNames.put(distinctEntry, cfStore.getTempSSTableFileName());
            }
            
            return fileNames;
        }

        private void addStreamContext(String host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus)
        {
            if (logger_.isDebugEnabled())
              logger_.debug("Adding stream context " + streamContext + " for " + host + " ...");
            StreamContextManager.addStreamContext(host, streamContext, streamStatus);
        }
    }
    
    /* Used to lock the factory for creation of Table instance */
    private static Lock createLock_ = new ReentrantLock();
    private static Map<String, Table> instances_ = new HashMap<String, Table>();
    /* Table name. */
    private String table_;
    /* Handle to the Table Metadata */
    private Table.TableMetadata tableMetadata_;
    /* ColumnFamilyStore per column family */
    private Map<String, ColumnFamilyStore> columnFamilyStores_ = new HashMap<String, ColumnFamilyStore>();
    // cache application CFs since Range queries ask for them a _lot_
    private SortedSet<String> applicationColumnFamilies_;

    public static Table open(String table) throws IOException
    {
        Table tableInstance = instances_.get(table);
        /*
         * Read the config and figure the column families for this table.
         * Set the isConfigured flag so that we do not read config all the
         * time.
        */
        if ( tableInstance == null )
        {
            Table.createLock_.lock();
            try
            {
                if ( tableInstance == null )
                {
                    tableInstance = new Table(table);
                    instances_.put(table, tableInstance);
                }
            }
            finally
            {
                createLock_.unlock();
            }
        }
        return tableInstance;
    }
        
    public Set<String> getColumnFamilies()
    {
        return tableMetadata_.getColumnFamilies();
    }

    Map<String, ColumnFamilyStore> getColumnFamilyStores()
    {
        return columnFamilyStores_;
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        return columnFamilyStores_.get(cfName);
    }

    /*
     * This method is called to obtain statistics about
     * the table. It will return statistics about all
     * the column families that make up this table. 
    */
    public String tableStats(String newLineSeparator, java.text.DecimalFormat df)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(table_ + " statistics :");
        sb.append(newLineSeparator);
        int oldLength = sb.toString().length();
        
        Set<String> cfNames = columnFamilyStores_.keySet();
        for ( String cfName : cfNames )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(cfName);
            sb.append(cfStore.cfStats(newLineSeparator));
        }
        int newLength = sb.toString().length();
        
        /* Don't show anything if there is nothing to show. */
        if ( newLength == oldLength )
            return "";
        
        return sb.toString();
    }

    public void onStart() throws IOException
    {
        for (String columnFamily : tableMetadata_.getColumnFamilies())
        {
            columnFamilyStores_.get(columnFamily).onStart();
        }
    }
    
    /** 
     * Do a cleanup of keys that do not belong locally.
     */
    public void forceCleanup()
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                cfStore.forceCleanup();
        }   
    }
    
    
    /**
     * Take a snapshot of the entire set of column families with a given timestamp.
     * 
     * @param clientSuppliedName the tag associated with the name of the snapshot.  This
     *                           value can be null.
     */
    public void snapshot(String clientSuppliedName) throws IOException
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }

        for (ColumnFamilyStore cfStore : columnFamilyStores_.values())
        {
            cfStore.snapshot(snapshotName);
        }
    }


    /**
     * Clear all the snapshots for a given table.
     */
    public void clearSnapshot() throws IOException
    {
        for (String dataDirPath : DatabaseDescriptor.getAllDataFileLocations())
        {
            String snapshotPath = dataDirPath + File.separator + table_ + File.separator + SNAPSHOT_SUBDIR_NAME;
            File snapshotDir = new File(snapshotPath);
            if (snapshotDir.exists())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Removing snapshot directory " + snapshotPath);
                if (!FileUtils.deleteDir(snapshotDir))
                    throw new IOException("Could not clear snapshot directory " + snapshotPath);
            }
        }
    }

    /*
     * This method is invoked only during a bootstrap process. We basically
     * do a complete compaction since we can figure out based on the ranges
     * whether the files need to be split.
    */
    public boolean forceCompaction(List<Range> ranges, EndPoint target, List<String> fileList)
    {
        boolean result = true;
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            if ( !isApplicationColumnFamily(columnFamily) )
                continue;
            
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
            {
                cfStore.forceCompaction(ranges, target, 0, fileList);                
            }
        }
        return result;
    }
    
    /*
     * This method is an ADMIN operation to force compaction
     * of all SSTables on disk. 
    */
    public void forceCompaction()
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                MinorCompactionManager.instance().submitMajor(cfStore, 0);
        }
    }

    /*
     * Get the list of all SSTables on disk.  Not safe unless you aquire the CFS readlocks!
    */
    public List<SSTableReader> getAllSSTablesOnDisk()
    {
        List<SSTableReader> list = new ArrayList<SSTableReader>();
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                list.addAll(cfStore.getSSTables());
        }
        return list;
    }

    private Table(String table) throws IOException
    {
        table_ = table;
        tableMetadata_ = Table.TableMetadata.instance(table);
        for (String columnFamily : tableMetadata_.getColumnFamilies())
        {
            columnFamilyStores_.put(columnFamily, ColumnFamilyStore.getColumnFamilyStore(table, columnFamily));
        }
    }

    boolean isApplicationColumnFamily(String columnFamily)
    {
        return DatabaseDescriptor.isApplicationColumnFamily(columnFamily);
    }

    int getColumnFamilyId(String columnFamily)
    {
        return tableMetadata_.getColumnFamilyId(columnFamily);
    }

    boolean isValidColumnFamily(String columnFamily)
    {
        return tableMetadata_.isValidColumnFamily(columnFamily);
    }

    /**
     * Selects the row associated with the given key.
    */
    @Deprecated // CF should be our atom of work, not Row
    public Row get(String key) throws IOException
    {
        Row row = new Row(table_, key);
        for (String columnFamily : getColumnFamilies())
        {
            ColumnFamily cf = get(key, columnFamily);
            if (cf != null)
            {
                row.addColumnFamily(cf);
            }
        }
        return row;
    }


    /**
     * Selects the specified column family for the specified key.
    */
    @Deprecated // single CFs could be larger than memory
    public ColumnFamily get(String key, String cfName) throws IOException
    {
        ColumnFamilyStore cfStore = columnFamilyStores_.get(cfName);
        assert cfStore != null : "Column family " + cfName + " has not been defined";
        return cfStore.getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfName)));
    }

    /**
     * Selects only the specified column family for the specified key.
    */
    @Deprecated
    public Row getRow(String key, String cfName) throws IOException
    {
        Row row = new Row(table_, key);
        ColumnFamily columnFamily = get(key, cfName);
        if ( columnFamily != null )
        	row.addColumnFamily(columnFamily);
        return row;
    }
    
    public Row getRow(QueryFilter filter) throws IOException
    {
        ColumnFamilyStore cfStore = columnFamilyStores_.get(filter.getColumnFamilyName());
        Row row = new Row(table_, filter.key);
        ColumnFamily columnFamily = cfStore.getColumnFamily(filter);
        if (columnFamily != null)
            row.addColumnFamily(columnFamily);
        return row;
    }

    /**
     * This method adds the row to the Commit Log associated with this table.
     * Once this happens the data associated with the individual column families
     * is also written to the column family store's memtable.
    */
    void apply(Row row) throws IOException
    {
        CommitLog.CommitLogContext cLogCtx = CommitLog.open().add(row);

        for (ColumnFamily columnFamily : row.getColumnFamilies())
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.apply(row.key(), columnFamily, cLogCtx);
        }
    }

    void applyNow(Row row) throws IOException
    {
        String key = row.key();
        for (ColumnFamily columnFamily : row.getColumnFamilies())
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.applyNow( key, columnFamily );
        }
    }

    public void flush(boolean fRecovery) throws IOException
    {
        for (String cfName : columnFamilyStores_.keySet())
        {
            if (fRecovery)
            {
                columnFamilyStores_.get(cfName).flushMemtableOnRecovery();
            }
            else
            {
                columnFamilyStores_.get(cfName).forceFlush();
            }
        }
    }

    // for binary load path.  skips commitlog.
    void load(Row row) throws IOException
    {
        String key = row.key();
                
        for (ColumnFamily columnFamily : row.getColumnFamilies())
        {
            Collection<IColumn> columns = columnFamily.getSortedColumns();
            for (IColumn column : columns)
            {
                ColumnFamilyStore cfStore = columnFamilyStores_.get(new String(column.name(), "UTF-8"));
                cfStore.applyBinary(key, column.value());
            }
        }
        row.clear();
    }

    public SortedSet<String> getApplicationColumnFamilies()
    {
        if (applicationColumnFamilies_ == null)
        {
            applicationColumnFamilies_ = new TreeSet<String>();
            for (String cfName : getColumnFamilies())
            {
                if (DatabaseDescriptor.isApplicationColumnFamily(cfName))
                {
                    applicationColumnFamilies_.add(cfName);
                }
            }
        }
        return applicationColumnFamilies_;
    }

    public static String getSnapshotPath(String dataDirPath, String tableName, String snapshotName)
    {
        return dataDirPath + File.separator + tableName + File.separator + SNAPSHOT_SUBDIR_NAME + File.separator + snapshotName;
    }
}
