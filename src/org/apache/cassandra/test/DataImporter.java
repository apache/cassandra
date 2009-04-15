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

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadMessage;
import org.apache.cassandra.db.ReadResponseMessage;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.batch_mutation_super_t;
import org.apache.cassandra.service.batch_mutation_t;
import org.apache.cassandra.service.column_t;
import org.apache.cassandra.service.superColumn_t;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class DataImporter {
	private static final String delimiter_ = new String(",");

	private static Logger logger_ = Logger.getLogger(DataImporter.class);

	private static final String tablename_ = new String("Mailbox");

	public static EndPoint from_ = new EndPoint("172.21.211.181", 10001);

	public static EndPoint to_ = new EndPoint("hadoop071.sf2p.facebook.com",
			7000);

	public static EndPoint[] tos_ = new EndPoint[]{ new EndPoint("hadoop038.sf2p.facebook.com",	7000),
													new EndPoint("hadoop039.sf2p.facebook.com", 7000),
													new EndPoint("hadoop040.sf2p.facebook.com", 7000),
													new EndPoint("hadoop041.sf2p.facebook.com",	7000)
												};
	private static final String columnFamily_ = new String("MailboxUserList");

	private Cassandra.Client peerstorageClient_ = null;

	public void test(String line) throws IOException {
		StringTokenizer st = new StringTokenizer(line, delimiter_);
		StringBuilder sb = new StringBuilder("");
		int i = 0;
		String column = null;
		int ts = 0;
		String columnValue = null;

		while (st.hasMoreElements()) {
			switch (i) {
			case 0:
				sb.append((String) st.nextElement());
				sb.append(":");
				break;

			case 1:
				sb.append((String) st.nextElement());
				break;

			case 2:
				column = (String) st.nextElement();
				break;

			case 3:
				ts = Integer.parseInt((String) st.nextElement());
				break;

			case 4:
				columnValue = (String) st.nextElement();
				break;

			default:
				break;
			}
			++i;
		}

		String rowKey = sb.toString();
		try {
			long t = System.currentTimeMillis();
			peerstorageClient_.insert(tablename_, rowKey, columnFamily_ + ":"
					+ column, columnValue, ts);
			logger_.debug("Time taken for thrift..."
					+ (System.currentTimeMillis() - t));
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* Added the thrift call to storage. */
	}

	private int roundRobin_ = 0 ;
    private Random random_ = new Random();
    
	public void apply(batch_mutation_t batchMutation) {

		columnFamilyHack_++;
		try {
			Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
			long t = System.currentTimeMillis();
			peerstorageClient_.batch_insert(batchMutation);
			logger_.debug("Time taken for thrift..."
					+ (System.currentTimeMillis() - t));
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

		columnFamilyHack_++;
		try {
			Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
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
	TTransport transport_ = null;
	public Cassandra.Client connect() {
//		String host = "hadoop034.sf2p.facebook.com";
		String[] hosts = new String[] { "hadoop038.sf2p.facebook.com",
				"hadoop039.sf2p.facebook.com",
				"hadoop040.sf2p.facebook.com",
				"hadoop041.sf2p.facebook.com"
			  };
		int port = 9160;
            
		//TNonBlockingSocket socket = new TNonBlockingSocket(hosts[roundRobin_], port);
		TSocket socket = new TSocket("hadoop071.sf2p.facebook.com", port); 
		roundRobin_ = (roundRobin_+1)%4;
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
    
    private static boolean isNumeric(String str)
    {
        try
        {
            Integer.parseInt(str);
            return true;
        }
        catch (NumberFormatException nfe)
        {
            return false;
        }
    }

    int columnFamilyHack_ = 0 ;
    public int  divideby_ = 4;

	public void testBatchRunner(String filepath) throws IOException {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		String firstuser = null;
		String nextuser = null;
		batch_mutation_t rmInbox = null;
		batch_mutation_t rmOutbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			int uid =0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}

			nextuser = user;
			if (firstuser == null || firstuser.compareTo(nextuser) != 0) {
				firstuser = nextuser;
				if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
/*					fos_.write(rmInbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
*/					apply(rmInbox);
				}
				if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
/*					fos_.write(rmOutbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
*/					apply(rmOutbox);
				}
				rmInbox = new batch_mutation_t();
				rmInbox.table = "Mailbox";
				rmInbox.key = firstuser + ":0";
				rmInbox.cfmap = new HashMap<String, List<column_t>>();

				rmOutbox = new batch_mutation_t();
				rmOutbox.table = "Mailbox";
				rmOutbox.key = firstuser + ":1";
				rmOutbox.cfmap = new HashMap<String, List<column_t>>();
			}
			column_t columnData = new column_t();
			columnData.columnName = threadId;
			columnData.value = String.valueOf(isDeleted);
			columnData.timestamp = lastUpdated;
			// List <MboxStruct> list = userthreadmap.get(rs.getString(1));
			if (folder == 0) {
				List<column_t> list = rmInbox.cfmap.get("MailboxUserList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<column_t>();
					rmInbox.cfmap.put("MailboxUserList"+(columnFamilyHack_%divideby_), list);
				}
                //if(list.size() < 500)
                    list.add(columnData);
			} else {
				List<column_t> list = rmOutbox.cfmap
						.get("MailboxUserList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<column_t>();
					rmOutbox.cfmap.put("MailboxUserList"+(columnFamilyHack_%divideby_), list);
				}
                //if(list.size() < 500)
                    list.add(columnData);
			}
		}
		if (firstuser != null) {
			if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
/*				fos_.write(rmInbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
*/				apply(rmInbox);
			}
			if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
/*				fos_.write(rmOutbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
*/				apply(rmOutbox);
			}
		}
		/* Added the thrift call to storage. */
	}


	public void testMailboxBatchRunner(String filepath) throws IOException {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		String firstuser = null;
		String nextuser = null;
		batch_mutation_t rmInbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			int uid =0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}

			nextuser = user;
			if (firstuser == null || firstuser.compareTo(nextuser) != 0) {
				firstuser = nextuser;
				if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
					apply(rmInbox);
				}
				rmInbox = new batch_mutation_t();
				rmInbox.table = "Mailbox";
				rmInbox.key = firstuser;
				rmInbox.cfmap = new HashMap<String, List<column_t>>();
			}
			column_t columnData = new column_t();
			columnData.columnName = threadId;
			columnData.value = String.valueOf(isDeleted);
			columnData.timestamp = lastUpdated;
			List<column_t> list = rmInbox.cfmap.get("MailboxMailList"+(columnFamilyHack_%divideby_));
			if (list == null) {
				list = new ArrayList<column_t>();
				rmInbox.cfmap.put("MailboxMailList"+(columnFamilyHack_%divideby_), list);
			}
            list.add(columnData);
		}
		if (firstuser != null) {
			if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
				apply(rmInbox);
			}
		}
	}

	public void testSuperBatchRunner(String filepath) throws IOException {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		String firstuser = null;
		String nextuser = null;
		batch_mutation_super_t rmInbox = null;
		batch_mutation_super_t rmOutbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			int uid =0;
			String user = null;
			String subject = null;
			String body = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				case 5:
					st.nextElement();
					break;

				case 6:
					st.nextElement();
					break;

				case 7:
					subject = (String) st.nextElement();
					break;

				case 8:
					body = (String) st.nextElement();
					break;
				default:
					st.nextElement();
					break;
				}
				++i;
			}

			nextuser = user;
			if (firstuser == null || firstuser.compareTo(nextuser) != 0) {
				firstuser = nextuser;
				if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
					fos_.write(rmInbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
					apply(rmInbox);
				}
				if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
					fos_.write(rmOutbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
					apply(rmOutbox);
				}
				rmInbox = new batch_mutation_super_t();
				rmInbox.table = "Mailbox";
				rmInbox.key = firstuser ;//+ ":0";
				rmInbox.cfmap = new HashMap<String, List<superColumn_t>>();

				rmOutbox = new batch_mutation_super_t();
				rmOutbox.table = "Mailbox";
				rmOutbox.key = firstuser ;//+ ":1";
				rmOutbox.cfmap = new HashMap<String, List<superColumn_t>>();
			}
			column_t columnData = new column_t();
			columnData.columnName = threadId;
			columnData.value = String.valueOf(isDeleted);
			columnData.timestamp = lastUpdated;
			// List <MboxStruct> list = userthreadmap.get(rs.getString(1));
			if (folder == 0) {
				List<superColumn_t> list = rmInbox.cfmap.get("MailboxThreadList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<superColumn_t>();
					rmInbox.cfmap.put("MailboxThreadList"+(columnFamilyHack_%divideby_), list);
				}
				if( subject == null)
					subject = "";
				if( body == null ) 
					body = "";
				List<String> tokenList  = tokenize(subject + " " + body);
				for(String token : tokenList)
				{
					superColumn_t superColumn = new superColumn_t();
					superColumn.name = token;
					superColumn.columns = new ArrayList<column_t>();
					superColumn.columns.add(columnData);
					list.add(superColumn);
				}
			} else {
				List<superColumn_t> list = rmOutbox.cfmap.get("MailboxThreadList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<superColumn_t>();
					rmOutbox.cfmap.put("MailboxThreadList"+(columnFamilyHack_%divideby_), list);
				}
				if( subject == null)
					subject = "";
				if( body == null ) 
					body = "";
				List<String> tokenList  = tokenize(subject + " " + body);
				for(String token : tokenList)
				{
					superColumn_t superColumn = new superColumn_t();
					superColumn.name = token;
					superColumn.columns = new ArrayList<column_t>();
					superColumn.columns.add(columnData);
					list.add(superColumn);
				}
			}
		}
		if (firstuser != null) {
			if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
				fos_.write(rmInbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
				apply(rmInbox);
			}
			if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
				fos_.write(rmOutbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
				apply(rmOutbox);
			}
		}
		/* Added the thrift call to storage. */
	}
	
    public static String[] getTokens(String str, String delim)
    {
        StringTokenizer st = new StringTokenizer(str, delim);
        String[] values = new String[st.countTokens()];
        int i = 0;
        while ( st.hasMoreElements() )
	    {
		values[i++] = (String)st.nextElement();
	    }
        return values;
    }

    public static boolean checkUser(String user, String[] list)
    {
    	boolean bFound = false;
    	for(String l:list)
    	{	
    		if(user.equals(l))
    		{
    			bFound = true;
    		}
    	}
    	return bFound;
    }
    
    public void testSuperUserBatchRunner(String filepath) throws IOException {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		String firstuser = null;
		String nextuser = null;
		batch_mutation_super_t rmInbox = null;
		batch_mutation_super_t rmOutbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			int uid =0;
			String user = null;
			String subject = null;
			String body = null;
			String authors = null;
			String participants = null;
			
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				case 5:
					authors = (String) st.nextElement();
					break;

				case 6:
					participants = (String)st.nextElement();
					break;

				case 7:
					subject = (String) st.nextElement();
					break;

				case 8:
					body = (String) st.nextElement();
					break;
				default:
					st.nextElement();
					break;
				}
				++i;
			}

			nextuser = user;
			if (firstuser == null || firstuser.compareTo(nextuser) != 0) {
				firstuser = nextuser;
				if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
					fos_.write(rmInbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
					apply(rmInbox);
				}
				if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
					fos_.write(rmOutbox.key.getBytes());
					fos_.write( System.getProperty("line.separator").getBytes());
	                counter_.incrementAndGet();
					apply(rmOutbox);
				}
				rmInbox = new batch_mutation_super_t();
				rmInbox.table = "Mailbox";
				rmInbox.key = firstuser ;//+ ":0";
				rmInbox.cfmap = new HashMap<String, List<superColumn_t>>();

				rmOutbox = new batch_mutation_super_t();
				rmOutbox.table = "Mailbox";
				rmOutbox.key = firstuser ;//+ ":1";
				rmOutbox.cfmap = new HashMap<String, List<superColumn_t>>();
			}
			column_t columnData = new column_t();
			columnData.columnName = threadId;
			columnData.value = String.valueOf(isDeleted);
			columnData.timestamp = lastUpdated;
			// List <MboxStruct> list = userthreadmap.get(rs.getString(1));
			if (folder == 0) {
				List<superColumn_t> list = rmInbox.cfmap.get("MailboxUserList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<superColumn_t>();
					rmInbox.cfmap.put("MailboxUserList"+(columnFamilyHack_%divideby_), list);
				}
				if( authors == null)
					authors = "";
				if( participants == null ) 
					participants = "";
				String[] authorList = getTokens(authors,":");
				String[] partList = getTokens(participants,":");
				String[] tokenList = null;
				if(checkUser(user,authorList))
				{
					tokenList = partList;
				}
				else
				{
					tokenList = authorList;
				}
				
				for(String token : tokenList)
				{
					superColumn_t superColumn = new superColumn_t();
					superColumn.name = token;
					superColumn.columns = new ArrayList<column_t>();
					superColumn.columns.add(columnData);
					list.add(superColumn);
				}
			} else {
				List<superColumn_t> list = rmOutbox.cfmap.get("MailboxUserList"+(columnFamilyHack_%divideby_));
				if (list == null) {
					list = new ArrayList<superColumn_t>();
					rmOutbox.cfmap.put("MailboxUserList"+(columnFamilyHack_%divideby_), list);
				}
				if( authors == null)
					authors = "";
				if( participants == null ) 
					participants = "";
				String[] authorList = getTokens(authors,":");
				String[] partList = getTokens(participants,":");
				String[] tokenList = null;
				if(checkUser(user,authorList))
				{
					tokenList = partList;
				}
				else
				{
					tokenList = authorList;
				}
				for(String token : tokenList)
				{
					superColumn_t superColumn = new superColumn_t();
					superColumn.name = token;
					superColumn.columns = new ArrayList<column_t>();
					superColumn.columns.add(columnData);
					list.add(superColumn);
				}
			}
		}
		if (firstuser != null) {
			if (rmInbox != null && !rmInbox.cfmap.isEmpty()) {
				fos_.write(rmInbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
				apply(rmInbox);
			}
			if (rmOutbox != null && !rmOutbox.cfmap.isEmpty()) {
				fos_.write(rmOutbox.key.getBytes());
				fos_.write( System.getProperty("line.separator").getBytes());
                counter_.incrementAndGet();
				apply(rmOutbox);
			}
		}
		/* Added the thrift call to storage. */
	}
	
	
	
	// Defining these privates here as they make more snese with the functions
	// below
	// Sorry

	private int numCreated_ = 0;

	ThreadFactory tf_ = null;

	ScheduledExecutorService pool_ = null;

	private int requestsPerSecond_ = 50;
    public FileOutputStream fos_ = null;
    private AtomicInteger counter_ = new AtomicInteger(0);
    
	// This is the task that gets scheduled
	// This could be different for different kind of tasks
	class Task implements Runnable {       
		RowMutationMessage rmMsg_ = null;
		
		public Task(RowMutationMessage rmMsg) {
			rmMsg_ = rmMsg;
		}

		public void run() {
			try {
				long t = System.currentTimeMillis();
                counter_.incrementAndGet();
				Message message = new Message(DataImporter.from_,
						StorageService.mutationStage_,
						StorageService.mutationVerbHandler_,
						new Object[] { rmMsg_ });
				MessagingService.getMessagingInstance().sendOneWay(message,
						DataImporter.to_);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public DataImporter() throws Throwable {
		tf_ = new ThreadFactoryImpl("LOAD-GENERATOR");
		pool_ = new DebuggableScheduledThreadPoolExecutor(100, tf_);
		fos_ = new FileOutputStream("keys.dat", true);
	}
    
	public long errorCount_ = 0;

	public long queryCount_ = 0;

	public void testRead(String filepath) throws Throwable {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		RowMutationMessage rmInbox = null;
		RowMutationMessage rmOutbox = null;
		ColumnFamily cfInbox = null;
		ColumnFamily cfOutbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}
			String key = null;
			if (folder == 0) {
				key = user + ":0";
			} else {
				key = user + ":1";
			}

			ReadMessage readMessage = new ReadMessage(tablename_, key);
			Message message = new Message(from_, StorageService.readStage_,
					StorageService.readVerbHandler_,
					new Object[] { readMessage });
			IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(
					message, to_);
			Object[] result = iar.get();
			ReadResponseMessage readResponseMessage = (ReadResponseMessage) result[0];
			Row row = readResponseMessage.row();
			if (row == null) {
				logger_.debug("ERROR No row for this key .....: " + line);
                Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
				errorCount_++;
			} else {
				Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
				if (cfMap == null || cfMap.size() == 0) {
					logger_
							.debug("ERROR ColumnFamil map is missing.....: "
									+ threadId + "   key:" + key
									+ "    record:" + line);
					System.out
							.println("ERROR ColumnFamil map is missing.....: "
									+ threadId + "   key:" + key
									+ "    record:" + line);
					errorCount_++;
					continue;
				}
				ColumnFamily cfamily = cfMap.get(columnFamily_);
				if (cfamily == null) {
					logger_
							.debug("ERROR ColumnFamily  is missing.....: "
									+ threadId + "   key:" + key
									+ "    record:" + line);
					System.out
							.println("ERROR ColumnFamily  is missing.....: "
									+ threadId + "   key:" + key
									+ "    record:" + line);
					errorCount_++;
					continue;
				}

				IColumn clmn = cfamily.getColumn(threadId);
				queryCount_++;
				if (clmn == null) {
					logger_.debug("ERROR Column is missing.....: " + threadId
							+ "    record:" + line);
					System.out.println("ERROR Column is missing.....: "
							+ threadId + "    record:" + line);
                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
					errorCount_++;
				} else {
					// logger_.debug("SUCCESS .....for column : "+clmn.name()+"
					// Record:" + line);
                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
				}
			}
			
		}

	}

	private List<String> tokenize(String string)
	{
		List<String> stringList = new ArrayList<String>();
	    Analyzer analyzer = new StandardAnalyzer();
	    TokenStream ts = analyzer.tokenStream("superColumn", new StringReader(string));
	    Token token = null;
	    try
	    {
		    token = ts.next();
		    while(token != null)
		    {
		    	stringList.add(token.termText());
			    token = ts.next();
		    }
	    }
	    catch(IOException ex)
	    {
	    	ex.printStackTrace();
	    }
		
		return stringList;
	}
	private int numReqs_ = 0;
	private long totalTime_ = 0 ;
	
	public void testReadThrift(String filepath) throws Throwable {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		RowMutationMessage rmInbox = null;
		RowMutationMessage rmOutbox = null;
		ColumnFamily cfInbox = null;
		ColumnFamily cfOutbox = null;
		String firstuser = null ;
		String nextuser = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}
			String key = null;
			if (folder == 0) {
				key = user + ":0";
			} else {
				key = user + ":1";
			}

			nextuser = key;
			if(firstuser == null || firstuser.compareTo(nextuser) != 0)
			{
				List<column_t> columns = null;
				firstuser = key;
				try {
                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
					long t = System.currentTimeMillis();
					
					columns = peerstorageClient_.get_slice(tablename_,key,columnFamily_+(columnFamilyHack_%divideby_),0,10);
					numReqs_++;
					totalTime_ = totalTime_ + (System.currentTimeMillis() - t);
					logger_.debug("Numreqs:" + numReqs_ + " Average: " + totalTime_/numReqs_+  "   Time taken for thrift..."
							+ (System.currentTimeMillis() - t));
				} catch (Exception e) {
						e.printStackTrace();
					}
				if (columns == null) {
					logger_.debug("ERROR No row for this key .....: " + line);
                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
					errorCount_++;
				} else {
					if (columns.size() == 0) {
						logger_
								.debug("ERROR ColumnFamil map is missing.....: "
										+ threadId + "   key:" + key
										+ "    record:" + line);
						System.out
								.println("ERROR ColumnFamil map is missing.....: "
										+ threadId + "   key:" + key
										+ "    record:" + line);
						errorCount_++;
						continue;
					}
					else {
						//logger_.debug("SUCCESS .....for key : "+key);
						//System.out.println("SUCCESS .....for key : "+key);
						//for(int j = 0 ; j< columns.size() ; j++ ){
							//System.out.print("  " + columns.get(j)+",");
						//}
						// Record:" + line);
						//Thread.sleep(5);
					}
				}
				queryCount_++;
			}
		}

	}
	

	public void testSuperReadThrift(String filepath) throws Throwable {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		RowMutationMessage rmInbox = null;
		RowMutationMessage rmOutbox = null;
		ColumnFamily cfInbox = null;
		ColumnFamily cfOutbox = null;
		String firstuser = null ;
		String nextuser = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			String user = null;
			String subject = null;
			String body = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;
				
				case 5:
					st.nextElement();
					break;


				case 6:
					st.nextElement();
					break;

				case 7:
					subject = (String) st.nextElement();
					break;

				case 8:
					body = (String) st.nextElement();
					break;
				default:
					st.nextElement();
					break;
				}
				++i;
			}
			String key = null;
			if (folder == 0) {
				key = user ;//+ ":0";
			} else {
				key = user ;//+ ":1";
			}

			List<column_t> columns = null;
			firstuser = key;
			try {
                Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
				if( subject == null )
					subject = "";
				if( body == null )
					body = "";
				List<String> tokenList = tokenize(subject + " " + body ) ;
				
				for( String token: tokenList )
				{
					long t = System.currentTimeMillis();
					columns = peerstorageClient_.get_slice(tablename_,key,"MailboxThreadList"+(columnFamilyHack_%divideby_)+":"+token,0,10);
					totalTime_ = totalTime_ + (System.currentTimeMillis() - t);
					numReqs_++;
					logger_.debug("Numreqs:" + numReqs_ + " Average: " + totalTime_/numReqs_+  "   Time taken for thrift..."
							+ (System.currentTimeMillis() - t));
					if (columns == null) {
						logger_.debug(" TOKEN: " + token + "  ERROR No row for this key .....: " + line);
	                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
						errorCount_++;
					} else {
						if (columns.size() == 0) {
							logger_
									.debug("ERROR ColumnFamil map is missing.....: "
											+ threadId + "   key:" + key
											+ " TOKEN: " + token
											+ "    record:" + line);
							System.out
									.println("ERROR ColumnFamil map is missing.....: "
											+ threadId + "   key:" + key
											+ " TOKEN: " + token
											+ "    record:" + line);
							errorCount_++;
							continue;
						}
						else {
							boolean found = false;
							for(column_t column : columns)
							{
								if(column.columnName.equalsIgnoreCase(threadId))
								{
									found = true ;
									break;
								}
							}
							if(!found)
							{
								logger_
										.debug("ERROR column is missing.....: "
												+ threadId + "   key:" + key
												+ " TOKEN: " + token
												+ "    record:" + line);
								System.out
										.println("ERROR column is missing.....: "
												+ threadId + "   key:" + key
												+ " TOKEN: " + token
												+ "    record:" + line);
								errorCount_++;
										
							}
							//logger_.debug("SUCCESS .....for key : "+key);
							//System.out.println("SUCCESS .....for key : "+key);
							//for(int j = 0 ; j< columns.size() ; j++ ){
								//System.out.print("  " + columns.get(j)+",");
							//}
							// Record:" + line);
							//Thread.sleep(5);
						}
					}
				}
				queryCount_++;
			} catch (Exception e) {
				e.printStackTrace();
			}
				
		}

	}
	
	
	
	public void testLoadGeneratorBatchRunner(String filepath) throws Throwable
    {
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(filepath)), 16 * 1024 * 1024);
        String line = null;
        String delimiter_ = new String(",");
        String firstuser = null;
        String nextuser = null;
        RowMutation rmInbox = null;
        RowMutation rmOutbox = null;
        ColumnFamily cfInbox = null;
        ColumnFamily cfOutbox = null;
        while ((line = bufReader.readLine()) != null)
        {
            StringTokenizer st = new StringTokenizer(line, delimiter_);
            int i = 0;
            String threadId = null;
            int lastUpdated = 0;
            int isDeleted = 0;
            int folder = 0;
            String user = null;
            while (st.hasMoreElements())
            {
                switch (i)
                {
                case 0:
                    user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    break;

                case 1:
                    folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
                    break;

                case 2:
                    threadId = (String) st.nextElement();
                    break;

                case 3:
                    lastUpdated = Integer.parseInt((String) st.nextElement());
                    break;

                case 4:
                    isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
                    break;

                default:
                    break;
                }
                ++i;
            }

         nextuser = user;
          if (firstuser == null || firstuser.compareTo(nextuser) != 0) {
                  firstuser = nextuser;
                  if (rmInbox != null) {
                          applyLoad(rmInbox);
                  }
                  if (rmOutbox != null) {
                          applyLoad(rmOutbox);
                  }
                  rmInbox = new RowMutation(tablename_, firstuser + ":0");
                  rmOutbox = new RowMutation(tablename_, firstuser + ":1");
          }
          // List <MboxStruct> list = userthreadmap.get(rs.getString(1));
          if (folder == 0) {
              rmInbox.add(columnFamily_+(columnFamilyHack_%divideby_)+":"+threadId, String.valueOf(isDeleted).getBytes(), lastUpdated);
          } else {
              rmOutbox.add(columnFamily_+(columnFamilyHack_%divideby_)+":"+threadId,String.valueOf(isDeleted).getBytes(),lastUpdated);
          }
  }
  if (firstuser != null) {
          if (rmInbox != null) {
                  applyLoad(rmInbox);
          }
          if (rmOutbox != null) {
                  applyLoad(rmOutbox);
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
            counter_.incrementAndGet();
    		columnFamilyHack_++;
            EndPoint to = new EndPoint(7000);
            RowMutationMessage rmMsg = new RowMutationMessage(rm);           
            Message message = new Message(to, 
                    StorageService.mutationStage_,
                    StorageService.mutationVerbHandler_, 
                    new Object[]{ rmMsg }
            );                                                            
			MessagingService.getMessagingInstance().sendRR(message, to);
            Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
    }

    
    
	public void testRemove(String filepath) throws Throwable {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		RowMutationMessage rmInbox = null;
		RowMutationMessage rmOutbox = null;
		ColumnFamily cfInbox = null;
		ColumnFamily cfOutbox = null;
		String firstuser = null ;
		String nextuser = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}
			String key = null;
			if (folder == 0) {
				key = user + ":0";
			} else {
				key = user + ":1";
			}

			nextuser = key;
			if(firstuser == null || firstuser.compareTo(nextuser) != 0)
			{
				ArrayList<column_t> columns = null;
				firstuser = key;
				try {
                    Thread.sleep(1000/requestsPerSecond_, 1000%requestsPerSecond_);
					long t = System.currentTimeMillis();
					
					peerstorageClient_.remove(tablename_,key,(columnFamilyHack_%divideby_)+":"+threadId);
					numReqs_++;
					totalTime_ = totalTime_ + (System.currentTimeMillis() - t);
					logger_.debug("Numreqs:" + numReqs_ + " Average: " + totalTime_/numReqs_+  "   Time taken for thrift..."
							+ (System.currentTimeMillis() - t));
				} catch (Exception e) {
						e.printStackTrace();
					}
			}
		}

	}

	public void run(String[] args) throws Throwable
    {
		if (args[0].compareTo("-testWriteMailbox") == 0  ||
			args[0].compareTo("-testSuperUserWrite") == 0  ||
			args[0].compareTo("-testPhp") == 0  ||
			args[0].compareTo("-testWrite") == 0  ||
			args[0].compareTo("-testRead") == 0 ||
			args[0].compareTo("-testWriteSuper") == 0  ||
			args[0].compareTo("-testReadSuper") == 0 ||
			args[0].compareTo("-testRemove") == 0 
			) 
		{
			int totalNumofServers = 5; // total number of servers we want to
			if (args.length > 4) {
				totalNumofServers = Integer.parseInt((String) args[4]);
			}			// run on
			if (args.length > 5) {
				divideby_ = Integer.parseInt((String) args[5]);
			}			// run on
			int index = Integer.parseInt(args[1]);
			int fileCount = 0; // need to get the file count
			int start = 0;
			int end = 0;
			File file = new File(args[2]);
			fileCount = file.list().length;
			if (index == -1) {
				start = 0;
				end = fileCount;
			} else {
				int skip = fileCount / totalNumofServers;
				if (fileCount != 0 && skip == 0) {
					skip = 1;
				}
				start = skip * index;
				if (index == totalNumofServers - 1) {
					end = fileCount;
				} else {
					end = start + skip;
				}
			}
			if (args.length > 3) {
				requestsPerSecond_ = Integer.parseInt((String) args[3]);
			}
			long t = System.currentTimeMillis();
			peerstorageClient_ = connect();
			for (int i = start; i < end; i++) {
				String fileName = file.list()[i];
				if ( args[0].compareTo("-testRead") == 0 )
				{
					testReadThrift(args[2] + System.getProperty("file.separator")
							+ fileName);
				}
				if ( args[0].compareTo("-testWrite") == 0 )
				{
					testBatchRunner(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testWriteSuper") == 0 )
				{
					testSuperBatchRunner(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testReadSuper") == 0 )
				{
					testSuperReadThrift(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testRemove") == 0 )
				{
					testRemove(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testPhp") == 0 )
				{
					testPhp(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testSuperUserWrite") == 0 )
				{
					testSuperUserBatchRunner(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				if ( args[0].compareTo("-testWriteMailbox") == 0 )
				{
					testMailboxBatchRunner(args[2]
							+ System.getProperty("file.separator") + fileName);
				}
				System.out.println(args[2]
						+ System.getProperty("file.separator") + fileName);
			}
			if(transport_ != null)
				transport_.close();
			System.out.println("start :" + start + "    end : " + end);
			System.out.println("Time taken  .."
					+ (System.currentTimeMillis() - t));
            System.out.println("Keys sent over: " + counter_.get());
			fos_.close();
			return;
		}
		else
		{
			System.out.println("Invalid option");
		}
    }


	
	public void testPhp(String filepath) throws IOException {
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(
				new FileInputStream(filepath)), 16 * 1024 * 1024);
		String line = null;
		String delimiter_ = new String(",");
		String firstuser = null;
		String nextuser = null;
		batch_mutation_t rmInbox = null;
		batch_mutation_t rmOutbox = null;
		while ((line = bufReader.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, delimiter_);
			int i = 0;
			String threadId = null;
			int lastUpdated = 0;
			int isDeleted = 0;
			int folder = 0;
			int uid =0;
			String user = null;
			while (st.hasMoreElements()) {
				switch (i) {
				case 0:
					user = (String) st.nextElement();// sb.append((String)st.nextElement());
                    if ( !isNumeric(user))
                        continue;
					
					break;

				case 1:
					folder = Integer.parseInt((String) st.nextElement());// sb.append((String)st.nextElement());
					break;

				case 2:
					threadId = (String) st.nextElement();
					break;

				case 3:
					lastUpdated = Integer.parseInt((String) st.nextElement());
					break;

				case 4:
					isDeleted = Integer.parseInt((String) st.nextElement());// (String)st.nextElement();
					break;

				default:
					break;
				}
				++i;
			}
			String cmd = "php /home/pmalik/www/scripts/mbox_index/search_test.php  "
				+ (new File(filepath)).getName() + "  " + user +"  " +threadId + "  "+ line;
			Process process = Runtime.getRuntime().exec(cmd);
		
		}
	}
    class PhpExecute implements Runnable
    {
        private String cmdLine_;
        
        PhpExecute(String cmdLine)
        {
        	cmdLine_ = cmdLine;
        }
        
        public void run()
        {
            try
            {
    			System.out.println(cmdLine_);
    			Process process = Runtime.getRuntime().exec(cmdLine_);
    			try
    			{
    				//process.waitFor();
    			}
    			catch ( Exception e)
    			{
    				e.printStackTrace();
    			}
            }
            catch (Exception ex)
            {
            	ex.printStackTrace();
            }
        }        
    }
}
