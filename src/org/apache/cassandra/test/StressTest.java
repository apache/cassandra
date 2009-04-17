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

package org.apache.cassandra.test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.analytics.AnalyticsContext;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ReadMessage;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.IResponseResolver;
import org.apache.cassandra.service.QuorumResponseHandler;
import org.apache.cassandra.service.ReadResponseResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.batch_mutation_super_t;
import org.apache.cassandra.service.batch_mutation_t;
import org.apache.cassandra.service.column_t;
import org.apache.cassandra.service.superColumn_t;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import com.martiansoftware.jsap.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class StressTest
{
	private static Logger logger_ = Logger.getLogger(DataImporter.class);

	private static final String tablename_ = new String("Test");

	public static EndPoint from_ = new EndPoint("172.24.24.209", 10001);

	public static EndPoint to_ = new EndPoint("hadoop071.sf2p.facebook.com", 7000);
	private static  String server_ = new String("hadoop071.sf2p.facebook.com");
	private static final String columnFamilyColumn_ = new String("ColumnList");
	private static final String columnFamilySuperColumn_ = new String("SuperColumnList");
	private static final String keyFix_ = new String("");
	private static final String columnFix_ = new String("Column-");
	private static final String superColumnFix_ = new String("SuperColumn-");

	private Cassandra.Client peerstorageClient_ = null;
	TTransport transport_ = null;
	private int requestsPerSecond_ = 1000;
    private ExecutorService runner_ = null;

    
    class LoadManager implements Runnable
    {
    	private RowMutationMessage rmsg_ = null;
    	private batch_mutation_t bt_ = null;
    	private batch_mutation_super_t bts_ = null;
   
    	LoadManager(RowMutationMessage rmsg)
        {
    		rmsg_ = rmsg;
        }
    	LoadManager(batch_mutation_t bt)
        {
    		bt_ = bt;
        }
    	LoadManager(batch_mutation_super_t bts)
        {
    		bts_ = bts;
        }
        
        public void run()
        {
        	if( rmsg_ != null )
        	{
				Message message = new Message(from_ , StorageService.mutationStage_,
						StorageService.loadVerbHandler_, new Object[] { rmsg_ });
				MessagingService.getMessagingInstance().sendOneWay(message, to_);
        	}
        	
       	}
    }

	
    /*
     * This function will apply the given task . It is based on a requests per
     * second member variable which can be set to teh required ammount , it will
     * generate only those many requests and if thos emany requests have already
     * been entered then it will sleep . This function assumes that there is no
     * waiting in any other part of the code so the requests are being generated
     * instantaniously .
     */
    public void applyLoad(RowMutation rm) throws IOException {
        try
        {
            long t = System.currentTimeMillis();
            RowMutationMessage rmMsg = new RowMutationMessage(rm);           
            Message message = new Message(from_, 
                    StorageService.mutationStage_,
                    StorageService.mutationVerbHandler_, 
                    new Object[]{ rmMsg }
            );                                                            
			MessagingService.getMessagingInstance().sendOneWay(message, to_);
            Thread.sleep(1, 1000000000/requestsPerSecond_);
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
    }	
	
    public void readLoad(ReadMessage readMessage)
    {
		IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
		QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
				1,
				readResponseResolver);
		Message message = new Message(from_, StorageService.readStage_,
				StorageService.readVerbHandler_,
				new Object[] { readMessage });
		MessagingService.getMessagingInstance().sendOneWay(message, to_);
		/*IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, to_);
		try
		{
			long t = System.currentTimeMillis();
			iar.get(2000, TimeUnit.MILLISECONDS );
			logger_.debug("Time taken for read..."
					+ (System.currentTimeMillis() - t));
			
		}
		catch (Exception ex)
		{
            ex.printStackTrace();
		}*/
    }
    
    
    
    
    
	public void randomReadColumn  (int keys, int columns, int size, int tps)
	{
        Random random = new Random();
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int j = random.nextInt(columns) + 1;
	            ReadMessage rm = new ReadMessage(tablename_, stringKey, columnFamilyColumn_ + ":" + columnFix_ + j);
	            readLoad(rm);
				if ( requestsPerSecond_ > 1000)
					Thread.sleep(0, 1000000000/requestsPerSecond_);
				else
					Thread.sleep(1000/requestsPerSecond_);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void randomWriteColumn(int keys, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            RowMutation rm = new RowMutation(tablename_, stringKey);
            	int j = random.nextInt(columns) + 1;
                random.nextBytes(bytes);
                rm.add( columnFamilyColumn_ + ":" + columnFix_ + j, bytes, ts);
                if ( ts == Integer.MAX_VALUE)
                {
                	ts = 0 ;
                }
                ts++;
				for(int k = 0 ; k < requestsPerSecond_/1000 +1 ; k++ )
				{
					runner_.submit(new LoadManager(new RowMutationMessage(rm)));
				}
				try
				{
					if ( requestsPerSecond_ > 1000)
						Thread.sleep(1);
					else
						Thread.sleep(1000/requestsPerSecond_);
				}
				catch ( Exception ex)
				{
					
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void randomReadSuperColumn(int keys, int superColumns, int columns, int size, int tps)
	{
        Random random = new Random();
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int i = random.nextInt(superColumns) + 1;
            	int j = random.nextInt(columns) + 1;
	            ReadMessage rm = new ReadMessage(tablename_, stringKey, columnFamilySuperColumn_ + ":" + superColumnFix_ + i + ":" + columnFix_ + j);
	            readLoad(rm);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	
	public void randomWriteSuperColumn(int keys, int superColumns,int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            RowMutation rm = new RowMutation(tablename_, stringKey);
            	int i = random.nextInt(superColumns) + 1;
            	int j = random.nextInt(columns) + 1;
                random.nextBytes(bytes);
                rm.add( columnFamilySuperColumn_ + ":" + superColumnFix_ + i + ":" + columnFix_ + j, bytes, ts);
                if ( ts == Integer.MAX_VALUE )
                {
                	ts = 0 ;
                }
                ts++;
				for(int k = 0 ; k < requestsPerSecond_/1000 +1 ; k++ )
				{
					runner_.submit(new LoadManager(new RowMutationMessage(rm)));
				}
				try
				{
					if ( requestsPerSecond_ > 1000)
						Thread.sleep(1);
					else
						Thread.sleep(1000/requestsPerSecond_);
				}
				catch ( Exception ex)
				{
					
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void bulkWriteColumn(int keys, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		long time = System.currentTimeMillis();
		try
		{
			for(int key = 1; key <= keys ; key++)
			{
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            RowMutation rm = new RowMutation(tablename_, stringKey);
	            for( int j = 1; j <= columns ; j++)
	            {
	                random.nextBytes(bytes);
	                rm.add( columnFamilyColumn_ + ":" + columnFix_ + j, bytes, ts);
	            }
				RowMutationMessage rmMsg = new RowMutationMessage(rm);
				
				for(int k = 0 ; k < requestsPerSecond_/1000 +1 ; k++ )
				{
					runner_.submit(new LoadManager(rmMsg));
				}
				try
				{
					if ( requestsPerSecond_ > 1000)
						Thread.sleep(1);
					else
						Thread.sleep(1000/requestsPerSecond_);
				}
				catch ( Exception ex)
				{
					
				}
				
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - time);
	}
	
	public void bulkWriteSuperColumn(int keys, int superColumns, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		try
		{
			for(int key = 1; key <= keys ; key++)
			{
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            RowMutation rm = new RowMutation(tablename_, stringKey);
	            for( int i = 1; i <= superColumns ; i++)
	            {
		            for( int j = 1; j <= columns ; j++)
		            {
		                random.nextBytes(bytes);
		                rm.add( columnFamilySuperColumn_ + ":" + superColumnFix_ + i + ":" + columnFix_ + j, bytes, ts);
		            }
	            }
	            RowMutationMessage rmMsg = new RowMutationMessage(rm);
				for(int k = 0 ; k < requestsPerSecond_/1000 +1 ; k++ )
				{
					runner_.submit(new LoadManager(rmMsg));
				}
				try
				{
					if ( requestsPerSecond_ > 1000)
						Thread.sleep(1);
					else
						Thread.sleep(1000/requestsPerSecond_);
				}
				catch ( Exception ex)
				{
					
				}
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	//  Stress the server using the thrift API 
	
	public Cassandra.Client connect() {
		int port = 9160;
		TSocket socket = new TSocket(server_, port); 
		if(transport_ != null)
			transport_.close();
		transport_ = socket;

		TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport_, false,
				false);
		Cassandra.Client peerstorageClient = new Cassandra.Client(
				binaryProtocol);
		try
		{
			transport_.open();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return peerstorageClient;
	}

	public void applyThrift(String table, String key, String columnFamily, byte[] bytes, long ts ) {

		try {
			if ( requestsPerSecond_ > 1000)
				Thread.sleep(0, 1000000000/requestsPerSecond_);
			else
				Thread.sleep(1000/requestsPerSecond_);
			peerstorageClient_.insert(table, key, columnFamily, bytes, ts);
		} catch (Exception e) {
			try {
				peerstorageClient_ = connect();
				peerstorageClient_.insert(table, key, columnFamily, bytes, ts);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	
	public void apply(batch_mutation_t batchMutation) {

		try {
			if ( requestsPerSecond_ > 1000)
				Thread.sleep(0, 1000000000/requestsPerSecond_);
			else
				Thread.sleep(1000/requestsPerSecond_);
			peerstorageClient_.batch_insert(batchMutation);
		} catch (Exception e) {
			try {
				peerstorageClient_ = connect();
				peerstorageClient_.batch_insert(batchMutation);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public void apply(batch_mutation_super_t batchMutation) {

		try {
			if ( requestsPerSecond_ > 1000)
				Thread.sleep(0, 1000000000/requestsPerSecond_);
			else
				Thread.sleep(1000/requestsPerSecond_);
			long t = System.currentTimeMillis();
			peerstorageClient_.batch_insert_superColumn(batchMutation);
			logger_.debug("Time taken for thrift..."
					+ (System.currentTimeMillis() - t));
		} catch (Exception e) {
			try {
				peerstorageClient_ = connect();
				peerstorageClient_.batch_insert_superColumn(batchMutation);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void readLoadColumn(String tableName, String key, String cf)
	{
		try
		{
			column_t column = peerstorageClient_.get_column(tableName, key, cf);
		}
		catch(Exception ex)
		{
			peerstorageClient_ = connect();
			ex.printStackTrace();
		}
	}
	
	public void randomReadColumnThrift(int keys, int columns, int size, int tps)
	{
        Random random = new Random();
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int j = random.nextInt(columns) + 1;
            	readLoadColumn(tablename_, stringKey, columnFamilyColumn_ + ":" + columnFix_ + j);
				if ( requestsPerSecond_ > 1000)
					Thread.sleep(0, 1000000000/requestsPerSecond_);
				else
					Thread.sleep(1000/requestsPerSecond_);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void randomWriteColumnThrift(int keys, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int j = random.nextInt(columns) + 1;
                random.nextBytes(bytes);
                if ( ts == Integer.MAX_VALUE)
                {
                	ts = 0 ;
                }
                ts++;
	            applyThrift(tablename_, stringKey, columnFamilyColumn_ + ":" + columnFix_ + j, bytes, ts);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void randomReadSuperColumnThrift(int keys, int superColumns, int columns, int size, int tps)
	{
        Random random = new Random();
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int i = random.nextInt(superColumns) + 1;
            	int j = random.nextInt(columns) + 1;
            	readLoadColumn(tablename_, stringKey, columnFamilySuperColumn_ + ":" + superColumnFix_ + i + ":" + columnFix_ + j);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	
	public void randomWriteSuperColumnThrift(int keys, int superColumns,int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		try
		{
			while(true)
			{
				int key = random.nextInt(keys) + 1;
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
            	int i = random.nextInt(superColumns) + 1;
            	int j = random.nextInt(columns) + 1;
                random.nextBytes(bytes);
                if ( ts == Integer.MAX_VALUE)
                {
                	ts = 0 ;
                }
                ts++;
	            applyThrift(tablename_, stringKey, columnFamilySuperColumn_ + ":" + superColumnFix_ + i + ":" + columnFix_ + j, bytes, ts);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

	
	}

	public void bulkWriteColumnThrift(int keys, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		long time = System.currentTimeMillis();
		try
		{
			for(int key = 1; key <= keys ; key++)
			{
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            batch_mutation_t bt = new batch_mutation_t();
	            bt.key = stringKey;
	            bt.table = tablename_;
	            bt.cfmap = new HashMap<String,List<column_t>>();
	            ArrayList<column_t> column_arr = new ArrayList<column_t>();
	            for( int j = 1; j <= columns ; j++)
	            {
	                random.nextBytes(bytes);
	                column_arr.add(new column_t(columnFix_ + j, bytes, ts));
	            }
	            bt.cfmap.put(columnFamilyColumn_, column_arr);
	            apply(bt);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - time);
	}
	
	public void bulkWriteSuperColumnThrift(int keys, int supercolumns, int columns, int size, int tps)
	{
        Random random = new Random();
        byte[] bytes = new byte[size];
		int ts = 1;
		long time = System.currentTimeMillis();
		try
		{
			for(int key = 1; key <= keys ; key++)
			{
	            String stringKey = new Integer(key).toString();
	            stringKey = stringKey + keyFix_ ;
	            batch_mutation_super_t bt = new batch_mutation_super_t();
	            bt.key = stringKey;
	            bt.table = tablename_;
	            bt.cfmap = new HashMap<String,List<superColumn_t>>();
	            ArrayList<superColumn_t> superColumn_arr = new ArrayList<superColumn_t>();
	            
	            for( int i = 1; i <= supercolumns; i++ )
	            {
		            ArrayList<column_t> column_arr = new ArrayList<column_t>();
		            for( int j = 1; j <= columns ; j++)
		            {
		                random.nextBytes(bytes);
		                column_arr.add(new column_t(columnFix_ + j, bytes, ts));
		            }
	            	superColumn_arr.add(new superColumn_t(superColumnFix_ + i, column_arr));	
	            }
	            bt.cfmap.put(columnFamilySuperColumn_, superColumn_arr);
	            apply(bt);
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		System.out.println(System.currentTimeMillis() - time);
	}
	
	public void testCommitLog() throws Throwable
	{
        Random random = new Random(System.currentTimeMillis());
    	byte[] bytes = new byte[4096];
    	random.nextBytes(bytes);
    	byte[] bytes1 = new byte[64];
    	random.nextBytes(bytes1);
    	peerstorageClient_ = connect();
    	int t = 0 ;
    	while( true )
    	{
	    	int key = random.nextInt();
	    	int threadId = random.nextInt();
	    	int word = random.nextInt();
			peerstorageClient_.insert("Mailbox", Integer.toString(key), "MailboxMailList0:" + Integer.toString(threadId), bytes1, t++);
			peerstorageClient_.insert("Mailbox", Integer.toString(key), "MailboxThreadList0:" + Integer.toString(word) + ":" + Integer.toString(threadId), bytes, t++);
			peerstorageClient_.insert("Mailbox", Integer.toString(key), "MailboxUserList0:"+ Integer.toString(word) + ":" + Integer.toString(threadId), bytes, t++);
    	}
	}

	JSAPResult ParseArguments(String[] args)
	{
        JSAPResult config = null;    
		try
		{
			
	        SimpleJSAP jsap = new SimpleJSAP( 
	                "StressTest", 
	                "Runs stress test for Cassandra",
	                new Parameter[] {
	                    new FlaggedOption( "keys", JSAP.INTEGER_PARSER, "10000", JSAP.REQUIRED, 'k', JSAP.NO_LONGFLAG, 
	                        "The number of keys from 1 to this number" ),
	                    new FlaggedOption( "columns", JSAP.INTEGER_PARSER, "1000", JSAP.REQUIRED, 'c', JSAP.NO_LONGFLAG, 
	                        "The number of columns from 1 to this number" ),
	                    new FlaggedOption( "supercolumns", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 'u', JSAP.NO_LONGFLAG, 
	                        "The number of super columns from 1 to this number" ),
	                    new FlaggedOption( "size", JSAP.INTEGER_PARSER, "1000", JSAP.REQUIRED, 's', JSAP.NO_LONGFLAG, 
	                        "The Size in bytes of each column" ),
	                    new FlaggedOption( "tps", JSAP.INTEGER_PARSER, "1000", JSAP.REQUIRED, 't', JSAP.NO_LONGFLAG, 
	                        "Requests per second" ),
	                    new FlaggedOption( "thrift", JSAP.INTEGER_PARSER, "0", JSAP.REQUIRED, 'h', JSAP.NO_LONGFLAG, 
	                        "Use Thrift - 1 , use messaging - 0" ),
	                    new FlaggedOption( "mailboxstress", JSAP.INTEGER_PARSER, "0", JSAP.REQUIRED, 'M', JSAP.NO_LONGFLAG, 
	                        "Run mailbox stress  - 1 , hmm default - 0" ),
	                    new FlaggedOption( "commitLogTest", JSAP.INTEGER_PARSER, "0", JSAP.REQUIRED, 'C', JSAP.NO_LONGFLAG, 
	                        "Run mailbox stress  - 1 , hmm default - 0" ),
	                    new QualifiedSwitch( "randomize", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'z', "randomize", 
	                        "Random reads or writes" ).setList( true ).setListSeparator( ',' ),
	                    new QualifiedSwitch( "reads", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "reads", 
	                        "Read data" ).setList( true ).setListSeparator( ',' ),
	                    new QualifiedSwitch( "writes", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'w', "writes", 
	                        "Write Data" ).setList( false ).setListSeparator( ',' ),
	                    new QualifiedSwitch( "bulkwrites", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "bulkwrites", 
	                        "Bulk Write Data" ).setList( false ).setListSeparator( ',' ),
                        new UnflaggedOption( "Server", JSAP.STRING_PARSER, JSAP.REQUIRED, "Name of the server the request needs to be sent to." ) }
	            )	;
	        
	            
	        	config = jsap.parse(args);    
	    		if( !config.success())
	    		{
	                System.err.println();
	                System.err.println("Usage: java "
	                                    + StressTest.class.getName());
	                System.err.println("                "
	                                    + jsap.getUsage());
	                System.err.println();
	                // show full help as well
	                System.err.println(jsap.getHelp());	                
	                System.err.println("**********Errors*************");
	    		}
	            if ( jsap.messagePrinted() ) return null;		
	    		String hostName = FBUtilities.getHostName();
	    		from_ = new EndPoint(hostName,10001);
	    		MessagingService.getMessagingInstance().listen(from_, false);
		}
		catch ( Exception ex) 
		{
			logger_.debug(LogUtil.throwableToString(ex));
		}
        return config;
	}
	
	void run( JSAPResult config ) throws Throwable
	{
		requestsPerSecond_ = config.getInt("tps");
		int numThreads = requestsPerSecond_/1000 + 1;
		if(config.getString("Server") != null)
		{
			server_ = config.getString("Server");
			to_ = new EndPoint(config.getString("Server"), 7000);
		}
		runner_ = new DebuggableThreadPoolExecutor( numThreads,
				numThreads,
	            Integer.MAX_VALUE,
	            TimeUnit.SECONDS,
	            new LinkedBlockingQueue<Runnable>(),
	            new ThreadFactoryImpl("MEMTABLE-FLUSHER-POOL")
	            );  
		if(config.getInt("mailboxstress") == 1)
		{
//			stressMailboxWrites();
			return;
		}
		if(config.getInt("commitLogTest") == 1)
		{
			testCommitLog();
			return;
		}
		if(config.getInt("thrift") == 0)
		{
			if(config.getInt("supercolumns") == 0)
			{
				if(config.getBoolean("reads"))
				{
					randomReadColumn(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("bulkwrites"))
				{
					bulkWriteColumn(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("writes"))
				{
					randomWriteColumn(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
			}
			else
			{
				if(config.getBoolean("reads"))
				{
					randomReadSuperColumn(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("bulkwrites"))
				{
					bulkWriteSuperColumn(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("writes"))
				{
					randomWriteSuperColumn(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				
			}
		}
		else
		{
			peerstorageClient_ = connect();
			if(config.getInt("supercolumns") == 0)
			{
				if(config.getBoolean("reads"))
				{
					randomReadColumnThrift(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("bulkwrites"))
				{
					bulkWriteColumnThrift(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("writes"))
				{
					randomWriteColumnThrift(config.getInt("keys"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
			}
			else
			{
				if(config.getBoolean("reads"))
				{
					randomReadSuperColumnThrift(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("bulkwrites"))
				{
					bulkWriteSuperColumnThrift(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				if(config.getBoolean("writes"))
				{
					randomWriteSuperColumnThrift(config.getInt("keys"), config.getInt("supercolumns"), config.getInt("columns"), config.getInt("size"), config.getInt("tps"));
					return;
				}
				
			}
			
		}
		System.out.println(" StressTest : Done !!!!!!");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable
	{
		LogUtil.init();
  		StressTest stressTest = new StressTest();
		JSAPResult config = stressTest.ParseArguments( args );
		if( config == null ) System.exit(-1);
		stressTest.run(config);
	}

}
