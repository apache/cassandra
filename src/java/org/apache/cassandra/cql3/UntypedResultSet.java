/*
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
 */
package org.apache.cassandra.cql3;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.driver.core.CodecUtils;
import org.apache.cassandra.cql3.functions.types.LocalDate;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/** a utility for doing internal cql-based queries */
public abstract class UntypedResultSet implements Iterable<UntypedResultSet.Row>
{
    public static UntypedResultSet create(ResultSet rs)
    {
        return new FromResultSet(rs);
    }

    public static UntypedResultSet create(List<Map<String, ByteBuffer>> results)
    {
        return new FromResultList(results);
    }

    public static UntypedResultSet create(SelectStatement select, QueryPager pager, int pageSize)
    {
        return new FromPager(select, pager, pageSize);
    }

    /**
     * This method is intended for testing purposes, since it executes query on cluster
     * and not on the local node only.
     */
    @VisibleForTesting
    public static UntypedResultSet create(SelectStatement select,
                                          ConsistencyLevel cl,
                                          ClientState clientState,
                                          QueryPager pager,
                                          int pageSize)
    {
        return new FromDistributedPager(select, cl, clientState, pager, pageSize);
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public Stream<Row> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    public abstract int size();
    public abstract Row one();

    // No implemented by all subclasses, but we use it when we know it's there (for tests)
    public abstract List<ColumnSpecification> metadata();

    private static class FromResultSet extends UntypedResultSet
    {
        private final ResultSet cqlRows;

        private FromResultSet(ResultSet cqlRows)
        {
            this.cqlRows = cqlRows;
        }

        public int size()
        {
            return cqlRows.size();
        }

        public Row one()
        {
            if (cqlRows.size() != 1)
                throw new IllegalStateException("One row required, " + cqlRows.size() + " found");
            return new Row(cqlRows.metadata.requestNames(), cqlRows.rows.get(0));
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                final Iterator<List<ByteBuffer>> iter = cqlRows.rows.iterator();

                protected Row computeNext()
                {
                    if (!iter.hasNext())
                        return endOfData();
                    return new Row(cqlRows.metadata.requestNames(), iter.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            return cqlRows.metadata.requestNames();
        }
    }

    private static class FromResultList extends UntypedResultSet
    {
        private final List<Map<String, ByteBuffer>> cqlRows;

        private FromResultList(List<Map<String, ByteBuffer>> cqlRows)
        {
            this.cqlRows = cqlRows;
        }

        public int size()
        {
            return cqlRows.size();
        }

        public Row one()
        {
            if (cqlRows.size() != 1)
                throw new IllegalStateException("One row required, " + cqlRows.size() + " found");
            return new Row(cqlRows.get(0));
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                final Iterator<Map<String, ByteBuffer>> iter = cqlRows.iterator();

                protected Row computeNext()
                {
                    if (!iter.hasNext())
                        return endOfData();
                    return new Row(iter.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class FromPager extends UntypedResultSet
    {
        private final SelectStatement select;
        private final QueryPager pager;
        private final int pageSize;
        private final List<ColumnSpecification> metadata;

        private FromPager(SelectStatement select, QueryPager pager, int pageSize)
        {
            this.select = select;
            this.pager = pager;
            this.pageSize = pageSize;
            this.metadata = select.getResultMetadata().requestNames();
        }

        public int size()
        {
            throw new UnsupportedOperationException();
        }

        public Row one()
        {
            throw new UnsupportedOperationException();
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                private Iterator<List<ByteBuffer>> currentPage;

                protected Row computeNext()
                {
                    long nowInSec = FBUtilities.nowInSeconds();
                    while (currentPage == null || !currentPage.hasNext())
                    {
                        if (pager.isExhausted())
                            return endOfData();

                        try (ReadExecutionController executionController = pager.executionController();
                             PartitionIterator iter = pager.fetchPageInternal(pageSize, executionController))
                        {
                            currentPage = select.process(iter, nowInSec, true).rows.iterator();
                        }
                    }
                    return new Row(metadata, currentPage.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            return metadata;
        }
    }

    /**
     * Pager that calls `execute` rather than `executeInternal`
     */
    private static class FromDistributedPager extends UntypedResultSet
    {
        private final SelectStatement select;
        private final ConsistencyLevel cl;
        private final ClientState clientState;
        private final QueryPager pager;
        private final int pageSize;
        private final List<ColumnSpecification> metadata;

        private FromDistributedPager(SelectStatement select,
                                     ConsistencyLevel cl,
                                     ClientState clientState,
                                     QueryPager pager, int pageSize)
        {
            this.select = select;
            this.cl = cl;
            this.clientState = clientState;
            this.pager = pager;
            this.pageSize = pageSize;
            this.metadata = select.getResultMetadata().requestNames();
        }

        public int size()
        {
            throw new UnsupportedOperationException();
        }

        public Row one()
        {
            throw new UnsupportedOperationException();
        }

        public Iterator<Row> iterator()
        {
            return new AbstractIterator<Row>()
            {
                private Iterator<List<ByteBuffer>> currentPage;

                protected Row computeNext()
                {
                    long nowInSec = FBUtilities.nowInSeconds();
                    while (currentPage == null || !currentPage.hasNext())
                    {
                        if (pager.isExhausted())
                            return endOfData();

                        try (PartitionIterator iter = pager.fetchPage(pageSize, cl, clientState, nanoTime()))
                        {
                            currentPage = select.process(iter, nowInSec, true).rows.iterator();
                        }
                    }
                    return new Row(metadata, currentPage.next());
                }
            };
        }

        public List<ColumnSpecification> metadata()
        {
            return metadata;
        }
    }

    public static class Row
    {
        private final Map<String, ByteBuffer> data = new HashMap<>();
        private final List<ColumnSpecification> columns = new ArrayList<>();

        public Row(Map<String, ByteBuffer> data)
        {
            this.data.putAll(data);
        }

        public Row(List<ColumnSpecification> names, List<ByteBuffer> columns)
        {
            this.columns.addAll(names);
            for (int i = 0; i < names.size(); i++)
                data.put(names.get(i).name.toString(), columns.get(i));
        }

        public static Row fromInternalRow(TableMetadata metadata, DecoratedKey key, org.apache.cassandra.db.rows.Row row)
        {
            Map<String, ByteBuffer> data = new HashMap<>();

            ByteBuffer[] keyComponents = SelectStatement.getComponents(metadata, key);
            for (ColumnMetadata def : metadata.partitionKeyColumns())
                data.put(def.name.toString(), keyComponents[def.position()]);

            Clustering<?> clustering = row.clustering();
            for (ColumnMetadata def : metadata.clusteringColumns())
                data.put(def.name.toString(), clustering.bufferAt(def.position()));

            for (ColumnMetadata def : metadata.regularAndStaticColumns())
            {
                if (def.isSimple())
                {
                    Cell<?> cell = row.getCell(def);
                    if (cell != null)
                        data.put(def.name.toString(), cell.buffer());
                }
                else
                {
                    ComplexColumnData complexData = row.getComplexColumnData(def);
                    if (complexData != null)
                        data.put(def.name.toString(), ((CollectionType<?>) def.type).serializeForNativeProtocol(complexData.iterator()));
                }
            }

            return new Row(data);
        }

        public boolean has(String column)
        {
            // Note that containsKey won't work because we may have null values
            return data.get(column) != null;
        }

        public ByteBuffer getBlob(String column)
        {
            return data.get(column);
        }

        public String getString(String column)
        {
            return UTF8Type.instance.compose(data.get(column));
        }

        public boolean getBoolean(String column)
        {
            return BooleanType.instance.compose(data.get(column));
        }

        public byte getByte(String column)
        {
            return ByteType.instance.compose(data.get(column));
        }

        public short getShort(String column)
        {
            return ShortType.instance.compose(data.get(column));
        }

        public int getInt(String column)
        {
            return Int32Type.instance.compose(data.get(column));
        }

        public int getInt(String column, int ifNull)
        {
            ByteBuffer bytes = data.get(column);
            return bytes == null ? ifNull : Int32Type.instance.compose(bytes);
        }

        public double getDouble(String column)
        {
            return DoubleType.instance.compose(data.get(column));
        }

        public ByteBuffer getBytes(String column)
        {
            return data.get(column);
        }

        public byte[] getByteArray(String column)
        {
            ByteBuffer buf = data.get(column);
            byte[] arr = new byte[buf.remaining()];
            for (int i = 0; i < arr.length; i++)
                arr[i] = buf.get(buf.position() + i);

            return arr;
        }

        public InetAddress getInetAddress(String column)
        {
            return InetAddressType.instance.compose(data.get(column));
        }

        public UUID getUUID(String column)
        {
            return UUIDType.instance.compose(data.get(column));
        }

        public UUID getUUID(String column, UUID ifNull)
        {
            ByteBuffer bytes = data.get(column);
            return bytes == null ? ifNull : UUIDType.instance.compose(bytes);
        }

        public TimeUUID getTimeUUID(String column)
        {
            return TimeUUID.deserialize(data.get(column));
        }

        public Date getTimestamp(String column)
        {
            return TimestampType.instance.compose(data.get(column));
        }

        public LocalDate getDate(String column) { return LocalDate.fromDaysSinceEpoch(CodecUtils.fromUnsignedToSignedInt(data.get(column).getInt()));}

        public long getLong(String column)
        {
            return LongType.instance.compose(data.get(column));
        }

        public <T> Set<T> getSet(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : SetType.getInstance(type, true).compose(raw);
        }

        public <T> List<T> getList(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : ListType.getInstance(type, true).compose(raw);
        }

        public <K, V> Map<K, V> getMap(String column, AbstractType<K> keyType, AbstractType<V> valueType)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : MapType.getInstance(keyType, valueType, true).compose(raw);
        }

        public <T> Set<T> getFrozenSet(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : SetType.getInstance(type, false).compose(raw);
        }

        public <T> List<T> getFrozenList(String column, AbstractType<T> type)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : ListType.getInstance(type, false).compose(raw);
        }

        public <K, V> Map<K, V> getFrozenMap(String column, AbstractType<K> keyType, AbstractType<V> valueType)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : MapType.getInstance(keyType, valueType, false).compose(raw);
        }

        public Map<String, String> getFrozenTextMap(String column)
        {
            return getFrozenMap(column, UTF8Type.instance, UTF8Type.instance);
        }

        public <T> List<T> getVector(String column, AbstractType<T> elementType, int dimension)
        {
            ByteBuffer raw = data.get(column);
            return raw == null ? null : VectorType.getInstance(elementType, dimension).compose(raw);
        }

        public List<ColumnSpecification> getColumns()
        {
            return columns;
        }

        @Override
        public String toString()
        {
            return data.toString();
        }
    }
}
