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
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class PreLoad
{
	
	private static long siesta_ = 2*60*1000;
	private static Logger logger_ = Logger.getLogger( Loader.class );
    private StorageService storageService_;
	
	public PreLoad(StorageService storageService)
    {
        storageService_ = storageService;
    }
    /*
     * This method loads all the keys into a special column family 
     * called "RecycleBin". This column family is used for temporary
     * processing of data and then can be recycled. The idea is that 
     * after the load is complete we have all the keys in the system.
     * Now we force a compaction and examine the single Index file 
     * that is generated to determine how the nodes need to relocate
     * to be perfectly load balanced.
     * 
     *  param @ rootDirectory - rootDirectory at which the parsing begins.
     *  param @ table - table that will be populated.
     *  param @ cfName - name of the column that will be populated. This is 
     *  passed in so that we do not unncessary allocate temporary String objects.
    */
    private void preParse(String rootDirectory, String table, String cfName) throws Throwable
    {        
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(rootDirectory)), 16 * 1024 * 1024);
        String line = null;
        while ((line = bufReader.readLine()) != null)
        {
                String userId = line;
                RowMutation rm = new RowMutation(table, userId);
                rm.add(cfName, userId.getBytes(), 0);
                rm.apply();
        }
    }
    
    void run(String userFile) throws Throwable
    {
        String table = DatabaseDescriptor.getTables().get(0);
        String cfName = Table.recycleBin_ + ":" + "Keys";
        /* populate just the keys. */
        preParse(userFile, table, cfName);
        /* dump the memtables */
        Table.open(table).flush(false);
        /* force a compaction of the files. */
        Table.open(table).forceCompaction(null, null,null);
        
        /*
         * This is a hack to let everyone finish. Just sleep for
         * a couple of minutes. 
        */
        logger_.info("Taking a nap after forcing a compaction ...");
        Thread.sleep(PreLoad.siesta_);
        
        /* Figure out the keys in the index file to relocate the node */
        List<String> ssTables = Table.open(table).getAllSSTablesOnDisk();
        /* Load the indexes into memory */
        for ( String df : ssTables )
        {
        	SSTable ssTable = new SSTable(df);
        	ssTable.close();
        }
        /* We should have only one file since we just compacted. */        
        List<String> indexedKeys = SSTable.getIndexedKeys();        
        storageService_.relocate(indexedKeys.toArray( new String[0]) );
        
        /*
         * This is a hack to let everyone relocate and learn about
         * each other. Just sleep for a couple of minutes. 
        */
        logger_.info("Taking a nap after relocating ...");
        Thread.sleep(PreLoad.siesta_);  
        
        /* 
         * Do the cleanup necessary. Delete all commit logs and
         * the SSTables and reset the load state in the StorageService. 
        */
        SSTable.delete(ssTables.get(0));
        storageService_.resetLoadState();
        logger_.info("Finished all the requisite clean up ...");
    }

    
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable
	{
		if(args.length != 1)
		{
			System.out.println("Usage: PreLoad <Fully qualified path of the file containing user names>");
		}
		// TODO Auto-generated method stub
		LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();
        PreLoad preLoad = new PreLoad(s);
        preLoad.run(args[0]);
	}

}
