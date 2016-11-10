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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

@Deprecated
public class ColumnFamilyRecordReader extends RecordReader<ByteBuffer, SortedMap<ByteBuffer, ColumnFamilyRecordReader.Column>>
    implements org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, ColumnFamilyRecordReader.Column>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordReader.class);

    public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private ColumnFamilySplit split;
    private RowIterator iter;
    private Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> currentRow;
    private SlicePredicate predicate;
    private boolean isEmptyPredicate;
    private int totalRowCount; // total number of rows to fetch
    private int batchSize; // fetch this many per batch
    private String keyspace;
    private String cfName;
    private Cassandra.Client client;
    private ConsistencyLevel consistencyLevel;
    private int keyBufferSize = 8192;
    private List<IndexExpression> filter;


    public ColumnFamilyRecordReader()
    {
        this(ColumnFamilyRecordReader.CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT);
    }

    public ColumnFamilyRecordReader(int keyBufferSize)
    {
        super();
        this.keyBufferSize = keyBufferSize;
    }

    @SuppressWarnings("resource")
    public void close()
    {
        if (client != null)
        {
            TTransport transport = client.getOutputProtocol().getTransport();
            if (transport.isOpen())
                transport.close();
        }
    }

    public ByteBuffer getCurrentKey()
    {
        return currentRow.left;
    }

    public SortedMap<ByteBuffer, Column> getCurrentValue()
    {
        return currentRow.right;
    }

    public float getProgress()
    {
        if (!iter.hasNext())
            return 1.0F;

        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) iter.rowsRead() / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    static boolean isEmptyPredicate(SlicePredicate predicate)
    {
        if (predicate == null)
            return true;

        if (predicate.isSetColumn_names() && predicate.getSlice_range() == null)
            return false;

        if (predicate.getSlice_range() == null)
            return true;

        byte[] start = predicate.getSlice_range().getStart();
        if ((start != null) && (start.length > 0))
            return false;

        byte[] finish = predicate.getSlice_range().getFinish();
        if ((finish != null) && (finish.length > 0))
            return false;

        return true;
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = HadoopCompat.getConfiguration(context);
        KeyRange jobRange = ConfigHelper.getInputKeyRange(conf);
        filter = jobRange == null ? null : jobRange.row_filter;
        predicate = ConfigHelper.getInputSlicePredicate(conf);
        boolean widerows = ConfigHelper.getInputIsWide(conf);
        isEmptyPredicate = isEmptyPredicate(predicate);
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                ? (int) this.split.getLength()
                : ConfigHelper.getInputSplitSize(conf);
        batchSize = ConfigHelper.getRangeBatchSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));
        keyspace = ConfigHelper.getInputKeyspace(conf);
        
        if (batchSize < 2)
            throw new IllegalArgumentException("Minimum batchSize is 2.  Suggested batchSize is 100 or more");

        String[] locations = getLocations();
        int port = ConfigHelper.getInputRpcPort(conf);

        Exception lastException = null;
        for (String location : locations)
        {
            try
            {
                client = ColumnFamilyInputFormat.createAuthenticatedClient(location, port, conf);
                break;
            }
            catch (Exception e)
            {
                lastException = e;
                logger.warn("Failed to create authenticated client to {}:{}", location , port);
            }
        }
        if (client == null && lastException != null)
            throw new RuntimeException(lastException);

        iter = widerows ? new WideRowIterator() : new StaticRowIterator();
        logger.trace("created {}", iter);
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
        {
            logger.trace("Finished scanning {} rows (estimate was: {})", iter.rowsRead(), totalRowCount);
            return false;
        }

        currentRow = iter.next();
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
    private String[] getLocations()
    {
        Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();

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
                    return new String[]{location};
                }
            }
        }
        return split.getLocations();
    }

    private abstract class RowIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Column>>>
    {
        protected List<KeySlice> rows;
        protected int totalRead = 0;
        protected final boolean isSuper;
        protected final AbstractType<?> comparator;
        protected final AbstractType<?> subComparator;
        protected final IPartitioner partitioner;

        private RowIterator()
        {
            CfDef cfDef = new CfDef();
            try
            {
                partitioner = FBUtilities.newPartitioner(client.describe_partitioner());           
                // get CF meta data
                String query = String.format("SELECT comparator, subcomparator, type " +
                                             "FROM %s.%s " +
                                             "WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                                             SystemKeyspace.NAME,
                                             LegacySchemaTables.COLUMNFAMILIES,
                                             keyspace,
                                             cfName);

                CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);

                Iterator<CqlRow> iteraRow = result.rows.iterator();

                if (iteraRow.hasNext())
                {
                    CqlRow cqlRow = iteraRow.next();
                    cfDef.comparator_type = ByteBufferUtil.string(cqlRow.columns.get(0).value);
                    ByteBuffer subComparator = cqlRow.columns.get(1).value;
                    if (subComparator != null)
                        cfDef.subcomparator_type = ByteBufferUtil.string(subComparator);
                    
                    ByteBuffer type = cqlRow.columns.get(2).value;
                    if (type != null)
                        cfDef.column_type = ByteBufferUtil.string(type);
                }

                comparator = TypeParser.parse(cfDef.comparator_type);
                subComparator = cfDef.subcomparator_type == null ? null : TypeParser.parse(cfDef.subcomparator_type);
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException("unable to load sub/comparator", e);
            }
            catch (TException e)
            {
                throw new RuntimeException("error communicating via Thrift", e);
            }
            catch (Exception e)
            {
                throw new RuntimeException("unable to load keyspace " + keyspace, e);
            }
            isSuper = "Super".equalsIgnoreCase(cfDef.column_type);
        }

        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead()
        {
            return totalRead;
        }

        protected List<Pair<ByteBuffer, Column>> unthriftify(ColumnOrSuperColumn cosc)
        {
            if (cosc.counter_column != null)
                return Collections.singletonList(unthriftifyCounter(cosc.counter_column));
            if (cosc.counter_super_column != null)
                return unthriftifySuperCounter(cosc.counter_super_column);
            if (cosc.super_column != null)
                return unthriftifySuper(cosc.super_column);
            assert cosc.column != null;
            return Collections.singletonList(unthriftifySimple(cosc.column));
        }

        private List<Pair<ByteBuffer, Column>> unthriftifySuper(SuperColumn super_column)
        {
            List<Pair<ByteBuffer, Column>> columns = new ArrayList<>(super_column.columns.size());
            for (org.apache.cassandra.thrift.Column column : super_column.columns)
            {
                Pair<ByteBuffer, Column> c = unthriftifySimple(column);
                columns.add(Pair.create(CompositeType.build(super_column.name, c.left), c.right));
            }
            return columns;
        }

        protected Pair<ByteBuffer, Column> unthriftifySimple(org.apache.cassandra.thrift.Column column)
        {
            return Pair.create(column.name, Column.fromRegularColumn(column));
        }

        private Pair<ByteBuffer, Column> unthriftifyCounter(CounterColumn column)
        {
            return Pair.create(column.name, Column.fromCounterColumn(column));
        }

        private List<Pair<ByteBuffer, Column>> unthriftifySuperCounter(CounterSuperColumn super_column)
        {
            List<Pair<ByteBuffer, Column>> columns = new ArrayList<>(super_column.columns.size());
            for (CounterColumn column : super_column.columns)
            {
                Pair<ByteBuffer, Column> c = unthriftifyCounter(column);
                columns.add(Pair.create(CompositeType.build(super_column.name, c.left), c.right));
            }
            return columns;
        }
    }

    private class StaticRowIterator extends RowIterator
    {
        protected int i = 0;

        private void maybeInit()
        {
            // check if we need another batch
            if (rows != null && i < rows.size())
                return;

            String startToken;
            if (totalRead == 0)
            {
                // first request
                startToken = split.getStartToken();
            }
            else
            {
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(Iterables.getLast(rows).key));
                if (startToken.equals(split.getEndToken()))
                {
                    // reached end of the split
                    rows = null;
                    return;
                }
            }

            KeyRange keyRange = new KeyRange(batchSize)
                                .setStart_token(startToken)
                                .setEnd_token(split.getEndToken())
                                .setRow_filter(filter);
            try
            {
                rows = client.get_range_slices(new ColumnParent(cfName), predicate, keyRange, consistencyLevel);

                // nothing new? reached the end
                if (rows.isEmpty())
                {
                    rows = null;
                    return;
                }

                // remove ghosts when fetching all columns
                if (isEmptyPredicate)
                {
                    Iterator<KeySlice> it = rows.iterator();
                    KeySlice ks;
                    do
                    {
                        ks = it.next();
                        if (ks.getColumnsSize() == 0)
                        {
                            it.remove();
                        }
                    } while (it.hasNext());

                    // all ghosts, spooky
                    if (rows.isEmpty())
                    {
                        // maybeInit assumes it can get the start-with key from the rows collection, so add back the last
                        rows.add(ks);
                        maybeInit();
                        return;
                    }
                }

                // reset to iterate through this new batch
                i = 0;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            KeySlice ks = rows.get(i++);
            AbstractType<?> comp = isSuper ? CompositeType.getInstance(comparator, subComparator) : comparator;
            SortedMap<ByteBuffer, Column> map = new TreeMap<>(comp);
            for (ColumnOrSuperColumn cosc : ks.columns)
            {
                List<Pair<ByteBuffer, Column>> columns = unthriftify(cosc);
                for (Pair<ByteBuffer, Column> column : columns)
                    map.put(column.left, column.right);
            }
            return Pair.create(ks.key, map);
        }
    }

    private class WideRowIterator extends RowIterator
    {
        private PeekingIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Column>>> wideColumns;
        private ByteBuffer lastColumn = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        private ByteBuffer lastCountedKey = ByteBufferUtil.EMPTY_BYTE_BUFFER;

        private void maybeInit()
        {
            if (wideColumns != null && wideColumns.hasNext())
                return;

            KeyRange keyRange;
            if (totalRead == 0)
            {
                String startToken = split.getStartToken();
                keyRange = new KeyRange(batchSize)
                          .setStart_token(startToken)
                          .setEnd_token(split.getEndToken())
                          .setRow_filter(filter);
            }
            else
            {
                KeySlice lastRow = Iterables.getLast(rows);
                logger.trace("Starting with last-seen row {}", lastRow.key);
                keyRange = new KeyRange(batchSize)
                          .setStart_key(lastRow.key)
                          .setEnd_token(split.getEndToken())
                          .setRow_filter(filter);
            }

            try
            {
                rows = client.get_paged_slice(cfName, keyRange, lastColumn, consistencyLevel);
                int n = 0;
                for (KeySlice row : rows)
                    n += row.columns.size();
                logger.trace("read {} columns in {} rows for {} starting with {}",
                             new Object[]{ n, rows.size(), keyRange, lastColumn });

                wideColumns = Iterators.peekingIterator(new WideColumnIterator(rows));
                if (wideColumns.hasNext() && wideColumns.peek().right.keySet().iterator().next().equals(lastColumn))
                    wideColumns.next();
                if (!wideColumns.hasNext())
                    rows = null;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> next = wideColumns.next();
            lastColumn = next.right.keySet().iterator().next().duplicate();

            maybeIncreaseRowCounter(next);
            return next;
        }


        /**
         * Increases the row counter only if we really moved to the next row.
         * @param next just fetched row slice
         */
        private void maybeIncreaseRowCounter(Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> next)
        {
            ByteBuffer currentKey = next.left;
            if (!currentKey.equals(lastCountedKey))
            {
                totalRead++;
                lastCountedKey = currentKey;
            }
        }

        private class WideColumnIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Column>>>
        {
            private final Iterator<KeySlice> rows;
            private Iterator<ColumnOrSuperColumn> columns;
            public KeySlice currentRow;

            public WideColumnIterator(List<KeySlice> rows)
            {
                this.rows = rows.iterator();
                if (this.rows.hasNext())
                    nextRow();
                else
                    columns = Iterators.emptyIterator();
            }

            private void nextRow()
            {
                currentRow = rows.next();
                columns = currentRow.columns.iterator();
            }

            protected Pair<ByteBuffer, SortedMap<ByteBuffer, Column>> computeNext()
            {
                AbstractType<?> comp = isSuper ? CompositeType.getInstance(comparator, subComparator) : comparator;
                while (true)
                {
                    if (columns.hasNext())
                    {
                        ColumnOrSuperColumn cosc = columns.next();
                        SortedMap<ByteBuffer, Column> map;
                        List<Pair<ByteBuffer, Column>> columns = unthriftify(cosc);
                        if (columns.size() == 1)
                        {
                            map = ImmutableSortedMap.of(columns.get(0).left, columns.get(0).right);
                        }
                        else
                        {
                            assert isSuper;
                            map = new TreeMap<>(comp);
                            for (Pair<ByteBuffer, Column> column : columns)
                                map.put(column.left, column.right);
                        }
                        return Pair.create(currentRow.key, map);
                    }

                    if (!rows.hasNext())
                        return endOfData();

                    nextRow();
                }
            }
        }
    }

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    public boolean next(ByteBuffer key, SortedMap<ByteBuffer, Column> value) throws IOException
    {
        if (this.nextKeyValue())
        {
            key.clear();
            key.put(this.getCurrentKey().duplicate());
            key.flip();

            value.clear();
            value.putAll(this.getCurrentValue());

            return true;
        }
        return false;
    }

    public ByteBuffer createKey()
    {
        return ByteBuffer.wrap(new byte[this.keyBufferSize]);
    }

    public SortedMap<ByteBuffer, Column> createValue()
    {
        return new TreeMap<>();
    }

    public long getPos() throws IOException
    {
        return iter.rowsRead();
    }

    public static final class Column
    {
        public final ByteBuffer name;
        public final ByteBuffer value;
        public final long timestamp;

        private Column(ByteBuffer name, ByteBuffer value, long timestamp)
        {
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
        }

        static Column fromRegularColumn(org.apache.cassandra.thrift.Column input)
        {
            return new Column(input.name, input.value, input.timestamp);
        }

        static Column fromCounterColumn(org.apache.cassandra.thrift.CounterColumn input)
        {
            return new Column(input.name, ByteBufferUtil.bytes(input.value), 0);
        }
    }
}
