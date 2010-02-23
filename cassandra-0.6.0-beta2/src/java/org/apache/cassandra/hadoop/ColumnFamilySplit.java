package org.apache.cassandra.hadoop;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ColumnFamilySplit extends InputSplit implements Writable
{
    private String startToken;
    private String endToken;
    private String table;
    private String columnFamily;
    private String[] dataNodes;
    private SlicePredicate predicate;

    public ColumnFamilySplit(String table, String columnFamily, SlicePredicate predicate, String startToken, String endToken, String[] dataNodes)
    {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.columnFamily = columnFamily;
        this.predicate = predicate;
        this.table = table;
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

    public String getTable()
    {
        return table;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    public SlicePredicate getPredicate()
    {
        return predicate;
    }

    // getLength and getLocations satisfy the InputSplit abstraction
    
    public long getLength()
    {
        // only used for sorting splits. we don't have the capability, yet.
        return 0;
    }

    public String[] getLocations()
    {
        return dataNodes;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    private static final TSerializer tSerializer = new TSerializer(new TBinaryProtocol.Factory());
    private static final TDeserializer tDeserializer = new TDeserializer(new TBinaryProtocol.Factory());

    // These three methods are for serializing and deserializing
    // KeyspaceSplits as needed by the Writable interface.
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(table);
        out.writeUTF(columnFamily);
        out.writeUTF(startToken);
        out.writeUTF(endToken);
        FBUtilities.serialize(tSerializer, predicate, out);

        out.writeInt(dataNodes.length);
        for (String endPoint : dataNodes)
        {
            out.writeUTF(endPoint);
        }
    }

    public void readFields(DataInput in) throws IOException
    {
        table = in.readUTF();
        columnFamily = in.readUTF();
        startToken = in.readUTF();
        endToken = in.readUTF();
        predicate = new SlicePredicate();
        FBUtilities.deserialize(tDeserializer, predicate, in);

        int numOfEndPoints = in.readInt();
        dataNodes = new String[numOfEndPoints];
        for(int i = 0; i < numOfEndPoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }
    }

    @Override
    public String toString()
    {
        return "ColumnFamilySplit{" +
               "startToken='" + startToken + '\'' +
               ", endToken='" + endToken + '\'' +
               ", table='" + table + '\'' +
               ", columnFamily='" + columnFamily + '\'' +
               ", dataNodes=" + (dataNodes == null ? null : Arrays.asList(dataNodes)) +
               ", predicate=" + predicate +
               '}';
    }

    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }
}
