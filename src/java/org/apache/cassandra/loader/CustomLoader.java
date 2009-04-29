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

package org.apache.cassandra.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;


public class CustomLoader
{
    private static Logger logger_ = Logger.getLogger( CustomLoader.class );
    private StorageService storageService_;
    private String path_;
    
    public CustomLoader(StorageService storageService, String path)
    {
        storageService_ = storageService;
        path_ = path;
    }
    /*
     * This function checks if the local storage endpoint 
     * is reponsible for storing this key .
     */
    boolean checkIfProcessKey(String key)
    {
		EndPoint[] endPoints = storageService_.getNStorageEndPoint(key);
    	EndPoint localEndPoint = StorageService.getLocalStorageEndPoint();
    	for(EndPoint endPoint : endPoints)
    	{
    		if(endPoint.equals(localEndPoint))
    			return true;
    	}
    	return false;
    }
    
    boolean checkUser(String user, String[] list)
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


     void parse(String filepath) throws Throwable
     {
    	 try
    	 {
	         BufferedReader bufReader = new BufferedReader(new InputStreamReader(
	                 new FileInputStream(filepath)), 16 * 1024 * 1024);
	         String line = null;
	         RowMutation rm = null;
	         while ((line = bufReader.readLine()) != null)
	         {
	 	      	// userid  threadid  folder  date  part-list  author-list  subject  body
	 	      	String columns[] = line.split("\t");
	 	      	if(columns.length < 7)
	 	      		continue;
	            if( rm == null)
	            {
	            	rm = new RowMutation("Mailbox", columns[0]);
	            }
	       	    Analyzer analyzer = new StandardAnalyzer();
	       	    String body = null;
	       	    if(columns.length > 7 )
	       	    	body = columns[6]+" "+columns[7];
	       	    else
	       	    	body = columns[6];
	       	    
	     	    TokenStream ts = analyzer.tokenStream("superColumn", new StringReader(body));
	     	    Token token = null;
	     	    token = ts.next();
	     	    while(token != null)
	     	    {
	     	    	if(token.termText() != "")
	     	    	{
	     	    		rm.add("MailboxThreadList0:"+token.termText()+":"+columns[1], columns[2].getBytes(), Integer.parseInt(columns[3]) );
	     	    	}
	         	    token = ts.next();
	     	    }
	     	    rm.add("MailboxMailList0:"+columns[1], columns[2].getBytes(), Integer.parseInt(columns[3]));
	    		String authors = columns[5];
	    		String participants = columns[4];
				if( authors == null)
					authors = "";
				if(participants == null)
					participants = "";
				String[] authorList = authors.split(":");
				String[] partList = participants.split(":");
	            String[] mailersList = null;
				if(checkUser(columns[0], authorList))
					mailersList = partList;
				else
					mailersList = authorList;
				for(String mailer : mailersList)
				{
					if(!mailer.equals(columns[0]))
					{
						rm.add("MailboxUserList0:"+ mailer + ":" +columns[1], columns[2].getBytes(), Integer.parseInt(columns[3]) );
					}
				}
	         }
	         if(rm != null)
	         {
	        	 rm.apply();
	         }
		}
		catch ( Throwable ex ) 
		{
          logger_.error( LogUtil.throwableToString(ex) );
		}
     }

     void parseFileList(File dir) 
     {
 		int fileCount = dir.list().length;
 		String[] dirList = dir.list();
 		File[] fileList = dir.listFiles();
 		for ( int i = 0 ; i < fileCount ; i++ ) 
 		{
 			File file = new File(fileList[i].getAbsolutePath());
 			if ( file.isDirectory())
 			{
 				parseFileList(file);
 			}
 			else 
 			{
 				try
 				{
					if(checkIfProcessKey(dirList[i]))
					{
						parse(fileList[i].getAbsolutePath());
					}
 				}
 				catch ( Throwable ex ) 
 				{
 	                logger_.error( LogUtil.throwableToString(ex) );
 				}
 			}
 		}
     }
     
     
     
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable
	{
		if(args.length != 1)
		{
			System.out.println("Usage: CustomLoader <root path to the data files>");
		}
		LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();
        CustomLoader loader = new CustomLoader(s, args[0]);
        File rootDirectory = new File(args[0]);
        long start = System.currentTimeMillis();
        loader.parseFileList(rootDirectory);
        logger_.info("Done Loading: " + (System.currentTimeMillis() - start)
                + " ms.");
    }

}
