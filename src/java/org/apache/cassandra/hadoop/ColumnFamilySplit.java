/*
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
package org.apache.cassandra.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

public class ColumnFamilySplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit
{
    private String startToken;
    private String endToken;
    private long length;
    private String[] dataNodes;

    @Deprecated
    public ColumnFamilySplit(String startToken, String endToken, String[] dataNodes)
    {
        this(startToken, endToken, Long.MAX_VALUE, dataNodes);
    }

    public ColumnFamilySplit(String startToken, String endToken, long length, String[] dataNodes)
    {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.length = length;
        this.dataNodes = dataNodes;
    }

    public String getStartToken()
    {
        return startToken;
    }

    public String getEndToken()
    {
        return endToken;
    }

    // getLength and getLocations satisfy the InputSplit abstraction

    public long getLength()
    {
        return length;
    }

    public String[] getLocations()
    {
        return dataNodes;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    // These three methods are for serializing and deserializing
    // KeyspaceSplits as needed by the Writable interface.
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(startToken);
        out.writeUTF(endToken);
        out.writeInt(dataNodes.length);
        for (String endpoint : dataNodes)
        {
            out.writeUTF(endpoint);
        }
        out.writeLong(length);
    }

    public void readFields(DataInput in) throws IOException
    {
        startToken = in.readUTF();
        endToken = in.readUTF();
        int numOfEndpoints = in.readInt();
        dataNodes = new String[numOfEndpoints];
        for(int i = 0; i < numOfEndpoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }
        try
        {
            length = in.readLong();
        }
        catch (EOFException e)
        {
            //We must be deserializing in a mixed-version cluster.
        }
    }

    @Override
    public String toString()
    {
        return "ColumnFamilySplit(" +
               "(" + startToken
               + ", '" + endToken + ']'
               + " @" + (dataNodes == null ? null : Arrays.asList(dataNodes)) + ')';
    }

    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }
}
