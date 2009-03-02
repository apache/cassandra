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

package org.apache.cassandra.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LogUtil;


public class KeyChecker
{
    private static final int bufSize_ = 128*1024*1024;
    /*
     * This function checks if the local storage endpoint 
     * is reponsible for storing this key .
     */
    private static boolean checkIfProcessKey(String key)
    {
        EndPoint[] endPoints = StorageService.instance().getNStorageEndPoint(key);
        EndPoint localEndPoint = StorageService.getLocalStorageEndPoint();
        for(EndPoint endPoint : endPoints)
        {
            if(endPoint.equals(localEndPoint))
                return true;
        }
        return false;
    }
    
    public static void main(String[] args) throws Throwable
    {
        if ( args.length != 1 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.KeyChecker <file containing all keys>");
            System.exit(1);
        }
        
        LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();
        
        /* Sleep for proper discovery */
        Thread.sleep(240000);
        /* Create the file for the missing keys */
        RandomAccessFile raf = new RandomAccessFile( "Missing-" + FBUtilities.getHostName() + ".dat", "rw");
        
        /* Start reading the file that contains the keys */
        BufferedReader bufReader = new BufferedReader( new InputStreamReader( new FileInputStream(args[0]) ), KeyChecker.bufSize_ );
        String key = null;
        boolean bStarted = false;
        
        while ( ( key = bufReader.readLine() ) != null )
        {            
            if ( !bStarted )
            {
                bStarted = true;
                System.out.println("Started the processing of the file ...");
            }
            
            key = key.trim();
            if ( StorageService.instance().isPrimary(key) )
            {
                System.out.println("Processing key " + key);
                Row row = Table.open("Mailbox").getRow(key, "MailboxMailList0");
                if ( row.isEmpty() )
                {
                    System.out.println("MISSING KEY : " + key);
                    raf.write(key.getBytes());
                    raf.write(System.getProperty("line.separator").getBytes());
                }
            }
        }
        System.out.println("DONE checking keys ...");
        raf.close();
    }
}
