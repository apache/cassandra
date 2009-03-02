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

import java.util.concurrent.atomic.AtomicInteger;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;

/*
 * An instance of this class represents an update to a table. 
 * This is written to the CommitLog to be replayed on recovery. It
 * contains enough information to be written to a SSTable to 
 * capture events that happened before some catastrophe.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
class CommitLogEntry
{    
    private static AtomicInteger lsnGenerator_ = new AtomicInteger(0);
    private static ICompactSerializer<CommitLogEntry> serializer_;
    static
    {
        serializer_ = new CommitLogEntrySerializer();
    }
    
    static ICompactSerializer<CommitLogEntry> serializer()
    {
        return serializer_;
    }    
    
    private int length_;
    private byte[] value_ = new byte[0];
    
    CommitLogEntry()
    {
    }
    
    CommitLogEntry(byte[] value)
    {
        this(value, 0);
    }
    
    CommitLogEntry(byte[] value, int length)
    {
        value_ = value;   
        length_ = length;
    }
    
    void value(byte[] bytes)
    {
        value_ = bytes;
    }
    
    byte[] value()
    {
        return value_;
    }
    
    void length(int size)
    {
        length_ = size;
    }
    
    int length()
    {
        return length_;
    }
}

class CommitLogEntrySerializer implements ICompactSerializer<CommitLogEntry>
{
    public void serialize(CommitLogEntry logEntry, DataOutputStream dos) throws IOException
    {    
        int length = logEntry.length();
        dos.writeInt(length);
        dos.write(logEntry.value(), 0, length);           
    }
    
    public CommitLogEntry deserialize(DataInputStream dis) throws IOException
    {        
        byte[] value = new byte[dis.readInt()];
        dis.readFully(value);        
        return new CommitLogEntry(value);
    }
}

