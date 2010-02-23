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


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyRecordReader extends RecordReader<String, SortedMap<byte[], IColumn>>
{
    private static final int ROWS_PER_RANGE_QUERY = 1024;

    private ColumnFamilySplit split;
    private RowIterator iter;
    private Pair<String, SortedMap<byte[], IColumn>> currentRow;

    public void close() {}
    
    public String getCurrentKey()
    {
        return currentRow.left;
    }

    public SortedMap<byte[], IColumn> getCurrentValue()
    {
        return currentRow.right;
    }
    
    public float getProgress()
    {
        return ((float)iter.rowsRead()) / iter.size();
    }
    
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        iter = new RowIterator();
    }
    
    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
            return false;
        currentRow = iter.next();
        return true;
    }

    private class RowIterator extends AbstractIterator<Pair<String, SortedMap<byte[], IColumn>>>
    {

        private List<KeySlice> rows;
        private int i = 0;
        private AbstractType comparator = DatabaseDescriptor.getComparator(split.getTable(), split.getColumnFamily());

        private void maybeInit()
        {
            if (rows != null)
                return;
            TSocket socket = new TSocket(getLocation(),
                                         DatabaseDescriptor.getThriftPort());
            TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
            Cassandra.Client client = new Cassandra.Client(binaryProtocol);
            try
            {
                socket.open();
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(e);
            }
            KeyRange keyRange = new KeyRange(ROWS_PER_RANGE_QUERY)
                                .setStart_token(split.getStartToken())
                                .setEnd_token(split.getEndToken());
            try
            {
                rows = client.get_range_slices(split.getTable(),
                                               new ColumnParent(split.getColumnFamily()),
                                               split.getPredicate(),
                                               keyRange,
                                               ConsistencyLevel.ONE);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        // we don't use endpointsnitch since we are trying to support hadoop nodes that are
        // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
        private String getLocation()
        {
            InetAddress[] localAddresses = new InetAddress[0];
            try
            {
                localAddresses = InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
            for (InetAddress address : localAddresses)
            {
                for (String location : split.getLocations())
                {
                    InetAddress locationAddress = null;
                    try
                    {
                        locationAddress = InetAddress.getByName(location);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new AssertionError(e);
                    }
                    if (address.equals(locationAddress))
                    {
                        return location;
                    }
                }
            }
            return split.getLocations()[0];
        }

        public int size()
        {
            maybeInit();
            return rows.size();
        }

        public int rowsRead()
        {
            return i;
        }

        @Override
        protected Pair<String, SortedMap<byte[], IColumn>> computeNext()
        {
            maybeInit();
            if (i == rows.size())
                return endOfData();
            KeySlice ks = rows.get(i++);
            SortedMap<byte[], IColumn> map = new TreeMap<byte[], IColumn>(comparator);
            for (ColumnOrSuperColumn cosc : ks.columns)
            {
                IColumn column = unthriftify(cosc);
                map.put(column.name(), column);
            }
            return new Pair<String, SortedMap<byte[], IColumn>>(ks.key, map);
        }
    }

    private IColumn unthriftify(ColumnOrSuperColumn cosc)
    {
        if (cosc.column == null)
            return unthriftifySuper(cosc.super_column);
        return unthriftifySimple(cosc.column);
    }

    private IColumn unthriftifySuper(SuperColumn super_column)
    {
        AbstractType subComparator = DatabaseDescriptor.getSubComparator(split.getTable(), split.getColumnFamily());
        org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(super_column.name, subComparator);
        for (Column column : super_column.columns)
        {
            sc.addColumn(unthriftifySimple(column));
        }
        return sc;
    }

    private IColumn unthriftifySimple(Column column)
    {
        return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
    }
}
