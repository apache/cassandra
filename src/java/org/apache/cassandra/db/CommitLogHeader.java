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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class CommitLogHeader
{
    private static ICompactSerializer<CommitLogHeader> serializer_;
    
    static
    {
        serializer_ = new CommitLogHeaderSerializer();
    }
    
    static ICompactSerializer<CommitLogHeader> serializer()
    {
        return serializer_;
    }
    
    static int size(int size)
    {
        /* 
         * We serialize the CommitLogHeader as a byte[] and write it
         * to disk. So we first write an "int" to specify the length 
         * of the byte[] which is why we first have a 4 in the sum.
         * We then have size which is the number of bits to track who
         * has been flushed and then the rest is the position[]
         * size = #of column families 
         *        + 
         *        size of the bitset 
         *        + 
         *        size of position array 
         */
        return 4 + size + (4 * size); 
    }
    
    static int getLowestPosition(CommitLogHeader clHeader)
    {
        int[] positions = clHeader.getPositions();
        int minPosition = Integer.MAX_VALUE;
        for ( int position : positions )
        {
            if ( position < minPosition && position > 0)
            {
                minPosition = position;
            }
        }
        
        if(minPosition == Integer.MAX_VALUE)
            minPosition = 0;
        return minPosition;
    }
    
    /* 
     * Bitwise & of each byte in the two arrays.
     * Both arrays are of same length. In order
     * to be memory efficient the result is in
     * the third parameter.
    */
    static byte[] and(byte[] bytes, byte[] bytes2) throws IOException
    { 
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, 0, bytes.length);
        CommitLogHeader clHeader = CommitLogHeader.serializer().deserialize(bufIn);
        byte[] clh = clHeader.getBitSet();
        
        bufIn.reset(bytes2, 0, bytes2.length);
        CommitLogHeader clHeader2 = CommitLogHeader.serializer().deserialize(bufIn);
        byte[] clh2 = clHeader2.getBitSet();
        
        byte[] result = new byte[clh.length];
        for ( int i = 0; i < clh.length; ++i )
        {            
            result[i] = (byte)(clh[i] & clh2[i]);
        }
        
        return result;
    }
    
    static boolean isZero(byte[] bytes)
    {
        for ( byte b : bytes )
        {
            if ( b == 1 )
                return false;
        }
        return true;
    }
    
    private byte[] header_ = new byte[0];
    private int[] position_ = new int[0];
    
    CommitLogHeader(int size)
    {
        header_ = new byte[size];
        position_ = new int[size];
    }
    
    /*
     * This ctor is used while deserializing. This ctor
     * also builds an index of position to column family
     * Id.
    */
    CommitLogHeader(byte[] header, int[] position)
    {
        header_ = header;
        position_ = position;
    }
    
    CommitLogHeader(CommitLogHeader clHeader)
    {
        header_ = new byte[clHeader.header_.length];
        System.arraycopy(clHeader.header_, 0, header_, 0, header_.length);
        position_ = new int[clHeader.position_.length];
        System.arraycopy(clHeader.position_, 0, position_, 0, position_.length);
    }
    
    byte get(int index)
    {
        return header_[index];
    } 
    
    int getPosition(int index)
    {
        return position_[index];
    }
    
    void turnOn(int index, long position)
    {
        turnOn(header_, index, position);
    }
    
    void turnOn(byte[] bytes, int index, long position)
    {
        bytes[index] = (byte)1;
        position_[index] = (int)position;
    }
    
    void turnOff(int index)
    {
        turnOff(header_, index);
    }
    
    void turnOff(byte[] bytes, int index)
    {
        bytes[index] = (byte)0;
        position_[index] = 0; 
    }
    
    boolean isSafeToDelete() throws IOException
    {
        return isSafeToDelete(header_);
    }
    
    boolean isSafeToDelete(byte[] bytes) throws IOException
    {        
        for ( byte b : bytes )
        {
            if ( b == 1 )
                return false;
        }
        return true;
    }
    
    byte[] getBitSet()
    {
        return header_;
    }
    
    int[] getPositions()
    {
        return position_;
    }
    
    void zeroPositions()
    {
        int size = position_.length;
        position_ = new int[size];
    }
    
    void and (CommitLogHeader commitLogHeader)
    {        
        byte[] clh2 = commitLogHeader.header_;
        for ( int i = 0; i < header_.length; ++i )
        {            
            header_[i] = (byte)(header_[i] & clh2[i]);
        }
    }
    
    byte[] toByteArray() throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);        
        CommitLogHeader.serializer().serialize(this, dos);
        return bos.toByteArray();
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("");        
        for ( int i = 0; i < header_.length; ++i )
        {
            sb.append(header_[i]);
            sb.append(":");
            Table table = Table.open( DatabaseDescriptor.getTables().get(0));
            sb.append(table.getColumnFamilyName(i));
            sb.append(" ");
        }        
        sb.append(" | " );        
        for ( int position : position_ )
        {
            sb.append(position);
            sb.append(" ");
        }        
        return sb.toString();
    }
}

class CommitLogHeaderSerializer implements ICompactSerializer<CommitLogHeader>
{
    public void serialize(CommitLogHeader clHeader, DataOutputStream dos) throws IOException
    {        
        dos.writeInt(clHeader.getBitSet().length);
        dos.write(clHeader.getBitSet());
        int[] positions = clHeader.getPositions();        
        
        for ( int position : positions )
        {
            dos.writeInt(position);
        }
    }
    
    public CommitLogHeader deserialize(DataInputStream dis) throws IOException
    {
        int size = dis.readInt();
        byte[] bitFlags = new byte[size];
        dis.readFully(bitFlags);
        
        int[] position = new int[size];
        for ( int i = 0; i < size; ++i )
        {
            position[i] = dis.readInt();
        }
                                                 
        return new CommitLogHeader(bitFlags, position);
    }
}


