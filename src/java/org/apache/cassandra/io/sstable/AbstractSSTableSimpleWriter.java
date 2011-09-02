/**
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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NodeId;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractSSTableSimpleWriter
{
    protected final File directory;
    protected final CFMetaData metadata;
    protected DecoratedKey currentKey;
    protected ColumnFamily columnFamily;
    protected SuperColumn currentSuperColumn;
    protected final NodeId nodeid = NodeId.generate();

    public AbstractSSTableSimpleWriter(File directory, CFMetaData metadata)
    {
        this.metadata = metadata;
        this.directory = directory;
    }

    protected SSTableWriter getWriter() throws IOException
    {
        return new SSTableWriter(
            makeFilename(directory, metadata.ksName, metadata.cfName),
            0, // We don't care about the bloom filter
            metadata,
            StorageService.getPartitioner(),
            SSTableMetadata.createCollector());
    }

    // find available generation and pick up filename from that
    private static String makeFilename(File directory, final String keyspace, final String columnFamily)
    {
        final Set<Descriptor> existing = new HashSet<Descriptor>();
        directory.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (desc == null)
                    return false;

                if (desc.cfname.equals(columnFamily))
                    existing.add(desc);

                return false;
            }
        });
        int maxGen = 0;
        for (Descriptor desc : existing)
            maxGen = Math.max(maxGen, desc.generation);
        return new Descriptor(directory, keyspace, columnFamily, maxGen + 1, false).filenameFor(Component.DATA);
    }

    /**
     * Start a new row whose key is {@code key}.
     * @param key the row key
     */
    public void newRow(ByteBuffer key) throws IOException
    {
        if (currentKey != null && !columnFamily.isEmpty())
            writeRow(currentKey, columnFamily);

        currentKey = StorageService.getPartitioner().decorateKey(key);
        columnFamily = getColumnFamily();
    }

    /**
     * Start a new super column with name {@code name}.
     * @param name the name for the super column
     */
    public void newSuperColumn(ByteBuffer name)
    {
        if (!columnFamily.isSuper())
            throw new IllegalStateException("Cannot add a super column to a standard column family");

        currentSuperColumn = new SuperColumn(name, metadata.subcolumnComparator);
        columnFamily.addColumn(currentSuperColumn);
    }

    private void addColumn(IColumn column)
    {
        if (columnFamily.isSuper() && currentSuperColumn == null)
            throw new IllegalStateException("Trying to add a column to a super column family, but no super column has been started.");

        IColumnContainer container = columnFamily.isSuper() ? currentSuperColumn : columnFamily;
        container.addColumn(column);
    }

    /**
     * Insert a new "regular" column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the column value
     * @param timestamp the column timestamp
     */
    public void addColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        addColumn(new Column(name, value, timestamp));
    }

    /**
     * Insert a new expiring column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the column value
     * @param timestamp the column timestamp
     * @param ttl the column time to live in seconds
     * @param expirationTimestampMS the local expiration timestamp in milliseconds. This is the server time timestamp used for actually
     * expiring the column, and as a consequence should be synchronized with the cassandra servers time. If {@code timestamp} represents
     * the insertion time in microseconds (which is not required), this should be {@code (timestamp / 1000) + (ttl * 1000)}.
     */
    public void addExpiringColumn(ByteBuffer name, ByteBuffer value, long timestamp, int ttl, long expirationTimestampMS)
    {
        addColumn(new ExpiringColumn(name, value, timestamp, ttl, (int)(expirationTimestampMS / 1000)));
    }

    /**
     * Insert a new counter column to the current row (and super column if applicable).
     * @param name the column name
     * @param value the value of the counter
     */
    public void addCounterColumn(ByteBuffer name, long value)
    {
        addColumn(new CounterColumn(name, CounterContext.instance().create(nodeid, 1L, value, false), System.currentTimeMillis()));
    }

    /**
     * Close this writer.
     * This method should be called, otherwise the produced sstables are not
     * guaranteed to be complete (and won't be in practice).
     */
    public abstract void close() throws IOException;

    protected abstract void writeRow(DecoratedKey key, ColumnFamily columnFamily) throws IOException;

    protected abstract ColumnFamily getColumnFamily();
}
