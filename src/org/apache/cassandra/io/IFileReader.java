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

package org.apache.cassandra.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

/**
 * Interface to read from the SequenceFile abstraction.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public interface IFileReader
{
    public String getFileName();
    public long getEOF() throws IOException;
    public long getCurrentPosition() throws IOException;
    public boolean isHealthyFileDescriptor() throws IOException;
    public void seek(long position) throws IOException;
    public boolean isEOF() throws IOException;

    /**
     * Be extremely careful while using this API. This currently
     * used to read the commit log header from the commit logs.
     * Treat this as an internal API.
     * 
     * @param bytes read into this byte array.
    */
    public void readDirect(byte[] bytes) throws IOException;
    
    /**
     * Read a long value from the underlying sub system.
     * @return value read
     * @throws IOException
     */
    public long readLong() throws IOException;
    
    /**
     * This functions is used to help out subsequent reads
     * on the specified key. It reads the keys prior to this
     * one on disk so that the buffer cache is hot.
     * 
     *  @param key key for which we are performing the touch.
     *  @param fData true implies we fetch the data into buffer cache.
    */
    public long touch(String key , boolean fData) throws IOException;
    
    /**
     * This method helps is retrieving the offset of the specified
     * key in the file using the block index.
     * 
     * @param key key whose position we need in the block index.
    */
    public long getPositionFromBlockIndex(String key) throws IOException;
    
    /**
     * This method returns the position of the specified key and the 
     * size of its data segment from the block index.
     * 
     * @param key key whose block metadata we are interested in.
     * @return an instance of the block metadata for this key.
    */
    public SSTable.BlockMetadata getBlockMetadata(String key) throws IOException;

    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in.
     *
     * @param bufOut DataOutputStream that needs to be filled.
     * @return number of bytes read.
     * @throws IOException 
    */
    public long next(DataOutputBuffer bufOut) throws IOException;

    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in.
     *
     * @param key key we are interested in.
     * @param bufOut DataOutputStream that needs to be filled.
     * @param section region of the file that needs to be read
     * @throws IOException
     * @return the number of bytes read.
    */
    public long next(String key, DataOutputBuffer bufOut, Coordinate section) throws IOException;

    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in.
     *
     * @param key key we are interested in.
     * @param bufOut DataOutputStream that needs to be filled.
     * @param column name of the column in our format.
     * @param section region of the file that needs to be read
     * @throws IOException
     * @return number of bytes that were read.
    */
    public long next(String key, DataOutputBuffer bufOut, String column, Coordinate section) throws IOException;

    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in. Always use this method to query for application
     * specific data as it will have indexes.
     *
     * @param key - key we are interested in.
     * @param bufOut - DataOutputStream that needs to be filled.
     * @param columnFamilyName The name of the column family only without the ":"
     * @param columnNames - The list of columns in the cfName column family
     * 					     that we want to return
     * @param section region of the file that needs to be read
     * @throws IOException
     * @return number of bytes read.
     *
    */
    public long next(String key, DataOutputBuffer bufOut, String columnFamilyName, List<String> columnNames, Coordinate section) throws IOException;
    
    /**
     * This method dumps the next key/value into the DataOuputStream
     * passed in.
     *
     * @param key key we are interested in.
     * @param bufOut DataOutputStream that needs to be filled.
     * @param column name of the column in our format.
     * @param timeRange time range we are interested in.
     * @param section region of the file that needs to be read
     * @throws IOException
     * @return number of bytes that were read.
    */
    public long next(String key, DataOutputBuffer bufOut, String column, IndexHelper.TimeRange timeRange, Coordinate section) throws IOException;

    /**
     * Close the file after reading.
     * @throws IOException
     */
    public void close() throws IOException;
}
