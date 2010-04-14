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

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import static org.apache.cassandra.utils.FBUtilities.UTF8;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyRecordReader extends RecordReader<String, SortedMap<byte[], IColumn>>
{
    private ColumnFamilySplit split;
    private RowIterator iter;
    private Pair<String, SortedMap<byte[], IColumn>> currentRow;
    private SlicePredicate predicate;
    private int totalRowCount; // total number of rows to fetch
    private int batchRowCount; // fetch this many per batch
    private String cfName;
    private String keyspace;

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
        // the progress is likely to be reported slightly off the actual but close enough
        return ((float)iter.rowsRead()) / totalRowCount;
    }
    
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        predicate = ConfigHelper.getSlicePredicate(conf);
        totalRowCount = ConfigHelper.getInputSplitSize(conf);
        batchRowCount = ConfigHelper.getRangeBatchSize(conf);
        cfName = ConfigHelper.getColumnFamily(conf);
        keyspace = ConfigHelper.getKeyspace(conf);
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
        private String startToken;
        private int totalRead = 0;
        private int i = 0;
        private AbstractType comparator = DatabaseDescriptor.getComparator(keyspace, cfName);

        private void maybeInit()
        {
            // check if we need another batch 
            if (rows != null && i >= rows.size())
                rows = null;
            
            if (rows != null)
                return;
            TSocket socket = new TSocket(getLocation(),
                                         DatabaseDescriptor.getRpcPort());
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
            
            if (startToken == null)
            {
                startToken = split.getStartToken();
            } 
            else if (startToken.equals(split.getEndToken()))
            {
                rows = null;
                return;
            }
            
            KeyRange keyRange = new KeyRange(batchRowCount)
                                .setStart_token(startToken)
                                .setEnd_token(split.getEndToken());
            try
            {
                rows = client.get_range_slices(keyspace,
                                               new ColumnParent(cfName),
                                               predicate,
                                               keyRange,
                                               ConsistencyLevel.ONE);
                    
                // nothing new? reached the end
                if (rows.isEmpty())
                {
                    rows = null;
                    return;
                }
                               
                // reset to iterate through this new batch
                i = 0;
                
                // prepare for the next slice to be read
                KeySlice lastRow = rows.get(rows.size() - 1);
                IPartitioner p = DatabaseDescriptor.getPartitioner();
                // FIXME: thrift strings
                byte[] rowkey = lastRow.getKey().getBytes(UTF8);
                startToken = p.getTokenFactory().toString(p.getToken(rowkey));
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

        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead()
        {
            return totalRead;
        }

        @Override
        protected Pair<String, SortedMap<byte[], IColumn>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();
            
            totalRead++;
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
        AbstractType subComparator = DatabaseDescriptor.getSubComparator(keyspace, cfName);
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
