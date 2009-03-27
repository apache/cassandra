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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.cassandra.analytics.DBAnalyticsSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BootstrapInitiateMessage;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.io.IStreamComplete;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
*/

public class Table
{
    /*
     * This class represents the metadata of this Table. The metadata
     * is basically the column family name and the ID associated with
     * this column family. We use this ID in the Commit Log header to
     * determine when a log file that has been rolled can be deleted.
    */    
    public static class TableMetadata
    {
        /* Name of the column family */
        public final static String cfName_ = "TableMetadata";
        /* 
         * Name of one of the columns. The other columns are the individual
         * column families in the system. 
        */
        public static final String cardinality_ = "PrimaryCardinality";
        private static ICompactSerializer<TableMetadata> serializer_;
        static
        {
            serializer_ = new TableMetadataSerializer();
        }
        
        private static TableMetadata tableMetadata_;
        /* Use the following writer/reader to write/read to Metadata table */
        private static IFileWriter writer_;
        private static IFileReader reader_;
        
        public static Table.TableMetadata instance() throws IOException
        {
            if ( tableMetadata_ == null )
            {
                String file = getFileName();
                writer_ = SequenceFile.writer(file);        
                reader_ = SequenceFile.reader(file);
                Table.TableMetadata.load();
                if ( tableMetadata_ == null )
                    tableMetadata_ = new Table.TableMetadata();
            }
            return tableMetadata_;
        }

        static ICompactSerializer<TableMetadata> serializer()
        {
            return serializer_;
        }
        
        private static void load() throws IOException
        {            
            String file = Table.TableMetadata.getFileName();
            File f = new File(file);
            if ( f.exists() )
            {
                DataOutputBuffer bufOut = new DataOutputBuffer();
                DataInputBuffer bufIn = new DataInputBuffer();
                
                if ( reader_ == null )
                {
                    reader_ = SequenceFile.reader(file);
                }
                
                while ( !reader_.isEOF() )
                {
                    /* Read the metadata info. */
                    reader_.next(bufOut);
                    bufIn.reset(bufOut.getData(), bufOut.getLength());

                    /* The key is the table name */
                    String key = bufIn.readUTF();
                    /* read the size of the data we ignore this value */
                    bufIn.readInt();
                    tableMetadata_ = Table.TableMetadata.serializer().deserialize(bufIn);
                    break;
                }        
            }            
        }
        
        /* The mapping between column family and the column type. */
        private Map<String, String> cfTypeMap_ = new HashMap<String, String>();
        private Map<String, Integer> cfIdMap_ = new HashMap<String, Integer>();
        private Map<Integer, String> idCfMap_ = new HashMap<Integer, String>();        
        
        private static String getFileName()
        {
            String table = DatabaseDescriptor.getTables().get(0);
            return DatabaseDescriptor.getMetadataDirectory() + System.getProperty("file.separator") + table + "-Metadata.db";
        }

        public void add(String cf, int id)
        {
            add(cf, id, "Standard");
        }
        
        public void add(String cf, int id, String type)
        {
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

        String getColumnFamilyName(int id)
        {
            return idCfMap_.get(id);
        }
        
        String getColumnFamilyType(String cfName)
        {
            return cfTypeMap_.get(cfName);
        }

        void setColumnFamilyType(String cfName, String type)
        {
            cfTypeMap_.put(cfName, type);
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
        
        public void apply() throws IOException
        {
            String table = DatabaseDescriptor.getTables().get(0);
            DataOutputBuffer bufOut = new DataOutputBuffer();
            Table.TableMetadata.serializer_.serialize(this, bufOut);
            try
            {
                writer_.append(table, bufOut);
            }
            catch ( IOException ex )
            {
                writer_.seek(0L);
                logger_.debug(LogUtil.throwableToString(ex));
            }
        }
        
        public void reset() throws IOException
        {        
            writer_.seek(0L);
            apply();
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
    }

    static class TableMetadataSerializer implements ICompactSerializer<TableMetadata>
    {
        public void serialize(TableMetadata tmetadata, DataOutputStream dos) throws IOException
        {
            int size = tmetadata.cfIdMap_.size();
            dos.writeInt(size);
            Set<String> cfNames = tmetadata.cfIdMap_.keySet();

            for ( String cfName : cfNames )
            {
                dos.writeUTF(cfName);
                dos.writeInt( tmetadata.cfIdMap_.get(cfName).intValue() );
                dos.writeUTF(tmetadata.getColumnFamilyType(cfName));
            }            
        }

        public TableMetadata deserialize(DataInputStream dis) throws IOException
        {
            TableMetadata tmetadata = new TableMetadata();
            int size = dis.readInt();
            for( int i = 0; i < size; ++i )
            {
                String cfName = dis.readUTF();
                int id = dis.readInt();
                String type = dis.readUTF();
                tmetadata.add(cfName, id, type);
            }            
            return tmetadata;
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
            if ( streamContext.getTargetFile().indexOf("-Data.db") != -1 )
            {
                File file = new File( streamContext.getTargetFile() );
                String fileName = file.getName();
                /*
                 * If the file is a Data File we need to load the indicies associated
                 * with this file. We also need to cache the file name in the SSTables
                 * list of the associated Column Family. Also merge the CBF into the
                 * sampler.
                */                
                SSTable ssTable = new SSTable(streamContext.getTargetFile() );
                ssTable.close();
                logger_.debug("Merging the counting bloom filter in the sampler ...");                
                String[] peices = FBUtilities.strip(fileName, "-");
                Table.open(peices[0]).getColumnFamilyStore(peices[1]).addToList(streamContext.getTargetFile());                
            }
            
            EndPoint to = new EndPoint(host, DatabaseDescriptor.getStoragePort());
            logger_.debug("Sending a bootstrap terminate message with " + streamStatus + " to " + to);
            /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
            StreamContextManager.StreamStatusMessage streamStatusMessage = new StreamContextManager.StreamStatusMessage(streamStatus);
            Message message = StreamContextManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
            MessagingService.getMessagingInstance().sendOneWay(message, to);           
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
            byte[] body = (byte[])message.getMessageBody()[0];
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
                    File sourceFile = new File( streamContext.getTargetFile() );
                    String[] peices = FBUtilities.strip(sourceFile.getName(), "-");
                    String newFileName = fileNames.get( peices[1] + "-" + peices[2] );
                    
                    String file = new String(DatabaseDescriptor.getDataFileLocation() + System.getProperty("file.separator") + newFileName + "-Data.db");
                    logger_.debug("Received Data from  : " + message.getFrom() + " " + streamContext.getTargetFile() + " " + file);
                    streamContext.setTargetFile(file);
                    addStreamContext(message.getFrom().getHost(), streamContext, streamStatus);                                            
                }    
                                             
                StreamContextManager.registerStreamCompletionHandler(message.getFrom().getHost(), new Table.BootstrapCompletionHandler());
                /* Send a bootstrap initiation done message to execute on default stage. */
                logger_.debug("Sending a bootstrap initiate done message ...");                
                Message doneMessage = new Message( StorageService.getLocalStorageEndPoint(), "", StorageService.bootStrapInitiateDoneVerbHandler_, new Object[]{new byte[0]} );
                MessagingService.getMessagingInstance().sendOneWay(doneMessage, message.getFrom());
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
        
        private Map<String, String> getNewNames(StreamContextManager.StreamContext[] streamContexts)
        {
            /* 
             * Mapping for each file with unique CF-i ---> new file name. For eg.
             * for a file with name <Table>-<CF>-<i>-Data.db there is a corresponding
             * <Table>-<CF>-<i>-Index.db. We maintain a mapping from <CF>-<i> to a newly
             * generated file name.
            */
            Map<String, String> fileNames = new HashMap<String, String>();
            /* Get the distinct entries from StreamContexts i.e have one entry per Data/Index file combination */
            Set<String> distinctEntries = new HashSet<String>();
            for ( StreamContextManager.StreamContext streamContext : streamContexts )
            {
                String[] peices = FBUtilities.strip(streamContext.getTargetFile(), "-");
                distinctEntries.add(peices[1] + "-" + peices[2]);
            }
            
            /* Generate unique file names per entry */
            Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
            Map<String, ColumnFamilyStore> columnFamilyStores = table.getColumnFamilyStores();
            
            for ( String distinctEntry : distinctEntries )
            {
                String[] peices = FBUtilities.strip(distinctEntry, "-");
                ColumnFamilyStore cfStore = columnFamilyStores.get(peices[0]);
                logger_.debug("Generating file name for " + distinctEntry + " ...");
                fileNames.put(distinctEntry, cfStore.getNextFileName());
            }
            
            return fileNames;
        }
        
        private boolean isStreamContextForThisColumnFamily(StreamContextManager.StreamContext streamContext, String cf)
        {
            String[] peices = FBUtilities.strip(streamContext.getTargetFile(), "-");
            return peices[1].equals(cf);
        }
        
        private String getColumnFamilyFromFile(String file)
        {
            String[] peices = FBUtilities.strip(file, "-");
            return peices[1];
        }
                
        private void addStreamContext(String host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus)
        {
            logger_.debug("Adding stream context " + streamContext + " for " + host + " ...");
            StreamContextManager.addStreamContext(host, streamContext, streamStatus);
        }
    }
    
    private static Logger logger_ = Logger.getLogger(Table.class);
    public static final String newLine_ = System.getProperty("line.separator");
    public static final String recycleBin_ = "RecycleColumnFamily";
    public static final String hints_ = "HintsColumnFamily";
    
    /* Used to lock the factory for creation of Table instance */
    private static Lock createLock_ = new ReentrantLock();
    private static Map<String, Table> instances_ = new HashMap<String, Table>();
    /* Table name. */
    private String table_;
    /* Handle to the Table Metadata */
    private Table.TableMetadata tableMetadata_;
    /* ColumnFamilyStore per column family */
    private Map<String, ColumnFamilyStore> columnFamilyStores_ = new HashMap<String, ColumnFamilyStore>();
    /* The AnalyticsSource instance which keeps track of statistics reported to Ganglia. */
    private DBAnalyticsSource dbAnalyticsSource_;    
    
    public static Table open(String table)
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

    ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        return columnFamilyStores_.get(cfName);
    }
    
    String getColumnFamilyType(String cfName)
    {
        String cfType = null;
        if ( tableMetadata_ != null )
          cfType = tableMetadata_.getColumnFamilyType(cfName);
        return cfType;
    }

    public void setColumnFamilyType(String cfName, String type)
    {
        tableMetadata_.setColumnFamilyType(cfName, type);
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
            sb.append(cfStore.cfStats(newLineSeparator, df));
        }
        int newLength = sb.toString().length();
        
        /* Don't show anything if there is nothing to show. */
        if ( newLength == oldLength )
            return "";
        
        return sb.toString();
    }

    void onStart() throws IOException
    {
        /* Cache the callouts if any */
        CalloutManager.instance().onStart();
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                cfStore.onStart();
        }         
    }
    
    /** 
     * Do a cleanup of keys that do not belong locally.
     */
    public void doGC()
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                cfStore.forceCleanup();
        }   
    }
    
    
    /*
     * This method is used to ensure that all keys
     * prior to the specified key, as dtermined by
     * the SSTable index bucket it falls in, are in
     * buffer cache.  
    */
    public void touch(String key, boolean fData) throws IOException
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            if ( DatabaseDescriptor.isApplicationColumnFamily(columnFamily) )
            {
                ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
                if ( cfStore != null )
                    cfStore.touch(key, fData);
            }
        }
    }

    /*
     * Clear the existing snapshots in the system
     */
    public void clearSnapshot()
    {
    	String snapshotDir = DatabaseDescriptor.getSnapshotDirectory();
    	File snapshot = new File(snapshotDir);
    	FileUtils.deleteDir(snapshot);
    }
    
    /*
     * This method is invoked only during a bootstrap process. We basically
     * do a complete compaction since we can figure out based on the ranges
     * whether the files need to be split.
    */
    public boolean forceCompaction(List<Range> ranges, EndPoint target, List<String> fileList) throws IOException
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
                /* Counting Bloom Filter for the Column Family */
                cfStore.forceCompaction(ranges, target, 0, fileList);                
            }
        }
        return result;
    }
    
    /*
     * This method is an ADMIN operation to force compaction
     * of all SSTables on disk. 
    */
    public void forceCompaction() throws IOException
    {
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                MinorCompactionManager.instance().submitMajor(cfStore, null, 0);
        }
    }

    /*
     * Get the list of all SSTables on disk. 
    */
    public List<String> getAllSSTablesOnDisk()
    {
        List<String> list = new ArrayList<String>();
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get( columnFamily );
            if ( cfStore != null )
                list.addAll( cfStore.getAllSSTablesOnDisk() );
        }
        return list;
    }

    private Table(String table)
    {
        table_ = table;
        dbAnalyticsSource_ = new DBAnalyticsSource();
        try
        {
            tableMetadata_ = Table.TableMetadata.instance();
            Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
            for ( String columnFamily : columnFamilies )
            {
                columnFamilyStores_.put( columnFamily, new ColumnFamilyStore(table, columnFamily) );
            }
        }
        catch ( IOException ex )
        {
            logger_.info(LogUtil.throwableToString(ex));
        }
    }

    String getTableName()
    {
        return table_;
    }
    
    boolean isApplicationColumnFamily(String columnFamily)
    {
        return DatabaseDescriptor.isApplicationColumnFamily(columnFamily);
    }

    int getNumberOfColumnFamilies()
    {
        return tableMetadata_.size();
    }

    int getColumnFamilyId(String columnFamily)
    {
        return tableMetadata_.getColumnFamilyId(columnFamily);
    }

    String getColumnFamilyName(int id)
    {
        return tableMetadata_.getColumnFamilyName(id);
    }

    boolean isValidColumnFamily(String columnFamily)
    {
        return tableMetadata_.isValidColumnFamily(columnFamily);
    }

    /**
     * Selects the row associated with the given key.
    */
    public Row get(String key) throws IOException
    {        
        Row row = new Row(key);
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        long start = System.currentTimeMillis();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily);
            if ( cfStore != null )
            {                
                ColumnFamily cf = cfStore.getColumnFamily(key, columnFamily, new IdentityFilter());                
                if ( cf != null )
                    row.addColumnFamily(cf);
            }
        }
        
        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateReadStatistics(timeTaken);
        return row;
    }
    
    public Row getRowFromMemory(String key)
    {
        Row row = new Row(key);
        Set<String> columnFamilies = tableMetadata_.getColumnFamilies();
        long start = System.currentTimeMillis();
        for ( String columnFamily : columnFamilies )
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily);
            if ( cfStore != null )
            {    
                ColumnFamily cf = cfStore.getColumnFamilyFromMemory(key, columnFamily, new IdentityFilter());
                if ( cf != null )
                    row.addColumnFamily(cf);
            }
        }
        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateReadStatistics(timeTaken);
        return row;
    }


    /**
     * Selects the specified column family for the specified key.
    */
    public ColumnFamily get(String key, String cf) throws ColumnFamilyNotDefinedException, IOException
    {
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        long start = System.currentTimeMillis();
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new IdentityFilter());
            long timeTaken = System.currentTimeMillis() - start;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return columnFamily;
        }
        else
        {
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
        }
    }

    /**
     * Selects only the specified column family for the specified key.
    */
    public Row getRow(String key, String cf) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        ColumnFamily columnFamily = get(key, cf);
        if ( columnFamily != null )
        	row.addColumnFamily(columnFamily);
        return row;
    }
  
    /**
     * Selects only the specified column family for the specified key.
    */
    public Row getRow(String key, String cf, int start, int count) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        long start1 = System.currentTimeMillis();
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new CountFilter(count));
            if ( columnFamily != null )
                row.addColumnFamily(columnFamily);
            long timeTaken = System.currentTimeMillis() - start1;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return row;
        }
        else
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
    }
    
    public Row getRow(String key, String cf, long sinceTimeStamp) throws ColumnFamilyNotDefinedException, IOException
    {
        Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);
        long start1 = System.currentTimeMillis();
        if ( cfStore != null )
        {
            ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new TimeFilter(sinceTimeStamp));
            if ( columnFamily != null )
                row.addColumnFamily(columnFamily);
            long timeTaken = System.currentTimeMillis() - start1;
            dbAnalyticsSource_.updateReadStatistics(timeTaken);
            return row;
        }
        else
            throw new ColumnFamilyNotDefinedException("Column family " + cf + " has not been defined");
    }

    /**
     * This method returns the specified columns for the specified
     * column family.
     * 
     *  param @ key - key for which data is requested.
     *  param @ cf - column family we are interested in.
     *  param @ columns - columns that are part of the above column family.
    */
    public Row getRow(String key, String cf, List<String> columns) throws IOException
    {
    	Row row = new Row(key);
        String[] values = RowMutation.getColumnAndColumnFamily(cf);
        ColumnFamilyStore cfStore = columnFamilyStores_.get(values[0]);

        if ( cfStore != null )
        {
        	ColumnFamily columnFamily = cfStore.getColumnFamily(key, cf, new NamesFilter(new ArrayList<String>(columns)));
        	if ( columnFamily != null )
        		row.addColumnFamily(columnFamily);
        }
    	return row;
    }

    /**
     * This method adds the row to the Commit Log associated with this table.
     * Once this happens the data associated with the individual column families
     * is also written to the column family store's memtable.
    */
    void apply(Row row) throws IOException
    {
        /* Add row to the commit log. */
        long start = System.currentTimeMillis();
        CommitLog.CommitLogContext cLogCtx = CommitLog.open(table_).add(row);

        for (ColumnFamily columnFamily : row.getColumnFamilies())
        {
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.apply(row.key(), columnFamily, cLogCtx);
        }

        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateWriteStatistics(timeTaken);
    }

    void applyNow(Row row) throws IOException
    {
        String key = row.key();
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilyMap();

        Set<String> cNames = columnFamilies.keySet();
        for ( String cName : cNames )
        {
            ColumnFamily columnFamily = columnFamilies.get(cName);
            ColumnFamilyStore cfStore = columnFamilyStores_.get(columnFamily.name());
            cfStore.applyNow( key, columnFamily );
        }
    }

    public void flush(boolean fRecovery) throws IOException
    {
        Set<String> cfNames = columnFamilyStores_.keySet();
        for ( String cfName : cfNames )
        {
            if (fRecovery) {
                columnFamilyStores_.get(cfName).flushMemtableOnRecovery();
            } else {
                columnFamilyStores_.get(cfName).forceFlush();
            }
        }
    }

    void load(Row row) throws IOException
    {
        String key = row.key();
        /* Add row to the commit log. */
        long start = System.currentTimeMillis();
                
        Map<String, ColumnFamily> columnFamilies = row.getColumnFamilyMap();
        Set<String> cNames = columnFamilies.keySet();
        for ( String cName : cNames )
        {
        	if( cName.equals(Table.recycleBin_))
        	{
	        	ColumnFamily columnFamily = columnFamilies.get(cName);
	        	Collection<IColumn> columns = columnFamily.getAllColumns();
        		for(IColumn column : columns)
        		{
    	            ColumnFamilyStore cfStore = columnFamilyStores_.get(column.name());
    	            if(column.timestamp() == 1)
    	            {
    	            	cfStore.forceFlushBinary();
    	            }
    	            else if(column.timestamp() == 2)
    	            {
    	            	cfStore.forceCompaction(null, null, BasicUtilities.byteArrayToLong(column.value()), null);
    	            }
    	            else if(column.timestamp() == 3)
    	            {
    	            	cfStore.forceFlush();
    	            }
    	            else if(column.timestamp() == 4)
    	            {
    	            	cfStore.forceCleanup();
    	            }    	            
    	            else
    	            {
    	            	cfStore.applyBinary(key, column.value());
    	            }
        		}
        	}
        }
        row.clear();
        long timeTaken = System.currentTimeMillis() - start;
        dbAnalyticsSource_.updateWriteStatistics(timeTaken);
    }
}
