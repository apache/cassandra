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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.*;

public class FileSizeTokenGenerator
{
    private static Logger logger_ = Logger.getLogger(IndexBuilder.class);
    
    public static void main(String[] args)
    {
        if ( args.length != 4 )
        {
            System.out.println("Usage : java com.facebook.infrastructure.tools.IndexBuilder <full path to the data file> < split factor>");
            System.exit(1);
        }
        
        try
        {
            int splitCount = Integer.parseInt(args[3]);
            BigInteger l = new BigInteger(args[1]);
            BigInteger h = new BigInteger(args[2]);
            long totalSize = getTotalSize(args[0], l, h);
        	System.out.println(" Total Size :  " + totalSize);
            BigInteger[] tokens = generateTokens(args[0], l, h, totalSize, splitCount);
            int i = 0 ;
            for( BigInteger token : tokens)
            {
            	System.out.println(i++ + " th Token " + token);
            }
        }
        catch( Throwable th )
        {
            logger_.warn(LogUtil.throwableToString(th));
        }
    }

    private static long  getTotalSize(String dataFile, BigInteger l , BigInteger h) throws IOException
    {
        final int bufferSize = 64*1024;
        
        IFileReader dataReader = SequenceFile.bufferedReader(dataFile, bufferSize);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        long totalSize = 0;
        try
        {                                            
            while ( !dataReader.isEOF() )
            {                
                bufOut.reset();                
                /* Record the position of the key. */
                dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* Key just read */
                String key = bufIn.readUTF();    
                if ( !key.equals(SSTable.blockIndexKey_) && l.compareTo(StorageService.hash(key)) < 0 && h.compareTo(StorageService.hash(key)) > 0 )
                {                                        
                    int sz = bufIn.readInt();
                	byte[] keyData = new byte[sz];
                	bufIn.read(keyData, 0, sz);
                    totalSize= totalSize + sz;
                }
            }
        }
        finally
        {
            dataReader.close();
        }
        return totalSize;
    }
    
    
    private static BigInteger[] generateTokens(String dataFile,BigInteger l , BigInteger h, long totalSize, int splitCount) throws IOException
    {
        final int bufferSize = 64*1024;
        
        IFileReader dataReader = SequenceFile.bufferedReader(dataFile, bufferSize);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();
        long splitFactor = totalSize/(splitCount+1);
        long curSize = 0;
        BigInteger[] tokens = new BigInteger[splitCount];
        int k = 0 ;
        try
        {                                            
            while ( !dataReader.isEOF())
            {                
                bufOut.reset();                
                /* Record the position of the key. */
                dataReader.next(bufOut);
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* Key just read */
                String key = bufIn.readUTF();       
                if ( !key.equals(SSTable.blockIndexKey_) && l.compareTo(StorageService.hash(key)) < 0 && h.compareTo(StorageService.hash(key)) > 0 )
                {                                        
                        int sz = bufIn.readInt();
                        curSize = curSize + sz;
                    	byte[] keyData = new byte[sz];
                    	bufIn.read(keyData, 0, sz);
                        
                        if( curSize > splitFactor)
                        {
                        	tokens[k++] = StorageService.hash(key);
                        	curSize = 0 ;
                        	if( k == splitCount)
                        	{
                        		break;
                        	}
                        }
                    }
                }
        }
        finally
        {
            dataReader.close();
        }
        return tokens;
    }
        
}
