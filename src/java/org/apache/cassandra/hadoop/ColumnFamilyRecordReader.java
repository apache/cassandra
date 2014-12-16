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
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.composites.CellNames;
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

public class ColumnFamilyRecordReader extends RecordReader<ByteBuffer, SortedMap<ByteBuffer, Cell>>
    implements org.apache.hadoop.mapred.RecordReader<ByteBuffer, SortedMap<ByteBuffer, Cell>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordReader.class);

    public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;

    private ColumnFamilySplit split;
    private RowIterator iter;
    private Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> currentRow;
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

    public SortedMap<ByteBuffer, Cell> getCurrentValue()
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

        try
        {
            if (client != null)
                return;

            // create connection using thrift
            String location = getLocation();

            int port = ConfigHelper.getInputRpcPort(conf);
            client = ColumnFamilyInputFormat.createAuthenticatedClient(location, port, conf);

        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        iter = widerows ? new WideRowIterator() : new StaticRowIterator();
        logger.debug("created {}", iter);
    }

    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
        {
            logger.debug("Finished scanning {} rows (estimate was: {})", iter.rowsRead(), totalRowCount);
            return false;
        }

        currentRow = iter.next();
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
    private String getLocation()
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
                    return location;
                }
            }
        }
        return split.getLocations()[0];
    }

    private abstract class RowIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>>>
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

        protected List<Cell> unthriftify(ColumnOrSuperColumn cosc)
        {
            if (cosc.counter_column != null)
                return Collections.<Cell>singletonList(unthriftifyCounter(cosc.counter_column));
            if (cosc.counter_super_column != null)
                return unthriftifySuperCounter(cosc.counter_super_column);
            if (cosc.super_column != null)
                return unthriftifySuper(cosc.super_column);
            assert cosc.column != null;
            return Collections.<Cell>singletonList(unthriftifySimple(cosc.column));
        }

        private List<Cell> unthriftifySuper(SuperColumn super_column)
        {
            List<Cell> cells = new ArrayList<Cell>(super_column.columns.size());
            for (org.apache.cassandra.thrift.Column column : super_column.columns)
            {
                Cell c = unthriftifySimple(column);
                cells.add(c.withUpdatedName(CellNames.simpleDense(CompositeType.build(super_column.name, c.name().toByteBuffer()))));
            }
            return cells;
        }

        protected Cell unthriftifySimple(org.apache.cassandra.thrift.Column column)
        {
            return new BufferCell(CellNames.simpleDense(column.name), column.value, column.timestamp);
        }

        private Cell unthriftifyCounter(CounterColumn column)
        {
            //CounterColumns read the counterID from the System keyspace, so need the StorageService running and access
            //to cassandra.yaml. To avoid a Hadoop needing access to yaml return a regular Cell.
            return new BufferCell(CellNames.simpleDense(column.name), ByteBufferUtil.bytes(column.value), 0);
        }

        private List<Cell> unthriftifySuperCounter(CounterSuperColumn super_column)
        {
            List<Cell> cells = new ArrayList<Cell>(super_column.columns.size());
            for (CounterColumn column : super_column.columns)
            {
                Cell c = unthriftifyCounter(column);
                cells.add(c.withUpdatedName(CellNames.simpleDense(CompositeType.build(super_column.name, c.name().toByteBuffer()))));
            }
            return cells;
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

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            totalRead++;
            KeySlice ks = rows.get(i++);
            AbstractType<?> comp = isSuper ? CompositeType.getInstance(comparator, subComparator) : comparator;
            SortedMap<ByteBuffer, Cell> map = new TreeMap<ByteBuffer, Cell>(comp);
            for (ColumnOrSuperColumn cosc : ks.columns)
            {
                List<Cell> cells = unthriftify(cosc);
                for (Cell cell : cells)
                    map.put(cell.name().toByteBuffer(), cell);
            }
            return Pair.create(ks.key, map);
        }
    }

    private class WideRowIterator extends RowIterator
    {
        private PeekingIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>>> wideColumns;
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
                logger.debug("Starting with last-seen row {}", lastRow.key);
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
                logger.debug("read {} columns in {} rows for {} starting with {}",
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

        protected Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> computeNext()
        {
            maybeInit();
            if (rows == null)
                return endOfData();

            Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> next = wideColumns.next();
            lastColumn = next.right.keySet().iterator().next().duplicate();

            maybeIncreaseRowCounter(next);
            return next;
        }


        /**
         * Increases the row counter only if we really moved to the next row.
         * @param next just fetched row slice
         */
        private void maybeIncreaseRowCounter(Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> next)
        {
            ByteBuffer currentKey = next.left;
            if (!currentKey.equals(lastCountedKey))
            {
                totalRead++;
                lastCountedKey = currentKey;
            }
        }

        private class WideColumnIterator extends AbstractIterator<Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>>>
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

            protected Pair<ByteBuffer, SortedMap<ByteBuffer, Cell>> computeNext()
            {
                AbstractType<?> comp = isSuper ? CompositeType.getInstance(comparator, subComparator) : comparator;
                while (true)
                {
                    if (columns.hasNext())
                    {
                        ColumnOrSuperColumn cosc = columns.next();
                        SortedMap<ByteBuffer, Cell> map;
                        List<Cell> cells = unthriftify(cosc);
                        if (cells.size() == 1)
                        {
                            map = ImmutableSortedMap.of(cells.get(0).name().toByteBuffer(), cells.get(0));
                        }
                        else
                        {
                            assert isSuper;
                            map = new TreeMap<ByteBuffer, Cell>(comp);
                            for (Cell cell : cells)
                                map.put(cell.name().toByteBuffer(), cell);
                        }
                        return Pair.<ByteBuffer, SortedMap<ByteBuffer, Cell>>create(currentRow.key, map);
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
    public boolean next(ByteBuffer key, SortedMap<ByteBuffer, Cell> value) throws IOException
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

    public SortedMap<ByteBuffer, Cell> createValue()
    {
        return new TreeMap<ByteBuffer, Cell>();
    }

    public long getPos() throws IOException
    {
        return (long)iter.rowsRead();
    }
}
