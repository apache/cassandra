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
package org.apache.cassandra.index.sasi.conf;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.view.View;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.memory.IndexMemtable;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnIndex
{
    private static final String FILE_NAME_FORMAT = "SI_%s.db";

    private final AbstractType<?> keyValidator;

    private final ColumnMetadata column;
    private final Optional<IndexMetadata> config;

    private final AtomicReference<IndexMemtable> memtable;
    private final ConcurrentMap<Memtable, IndexMemtable> pendingFlush = new ConcurrentHashMap<>();

    private final IndexMode mode;

    private final Component component;
    private final DataTracker tracker;

    private final boolean isTokenized;

    public ColumnIndex(AbstractType<?> keyValidator, ColumnMetadata column, IndexMetadata metadata)
    {
        this.keyValidator = keyValidator;
        this.column = column;
        this.config = metadata == null ? Optional.empty() : Optional.of(metadata);
        this.mode = IndexMode.getMode(column, config);
        this.memtable = new AtomicReference<>(new IndexMemtable(this));
        this.tracker = new DataTracker(keyValidator, this);
        this.component = Components.Types.SECONDARY_INDEX.createComponent(String.format(FILE_NAME_FORMAT, getIndexName()));
        this.isTokenized = getAnalyzer().isTokenizing();
    }

    /**
     * Initialize this column index with specific set of SSTables.
     *
     * @param sstables The sstables to be used by index initially.
     *
     * @return A collection of sstables which don't have this specific index attached to them.
     */
    public Iterable<SSTableReader> init(Set<SSTableReader> sstables)
    {
        return tracker.update(Collections.emptySet(), sstables);
    }

    public AbstractType<?> keyValidator()
    {
        return keyValidator;
    }

    public long index(DecoratedKey key, Row row)
    {
        return getCurrentMemtable().index(key, getValueOf(column, row, FBUtilities.nowInSeconds()));
    }

    public void switchMemtable()
    {
        // discard current memtable with all of it's data, useful on truncate
        memtable.set(new IndexMemtable(this));
    }

    public void switchMemtable(Memtable parent)
    {
        pendingFlush.putIfAbsent(parent, memtable.getAndSet(new IndexMemtable(this)));
    }

    public void discardMemtable(Memtable parent)
    {
        pendingFlush.remove(parent);
    }

    @VisibleForTesting
    public IndexMemtable getCurrentMemtable()
    {
        return memtable.get();
    }

    @VisibleForTesting
    public Collection<IndexMemtable> getPendingMemtables()
    {
        return pendingFlush.values();
    }

    public RangeIterator<Long, Token> searchMemtable(Expression e)
    {
        RangeIterator.Builder<Long, Token> builder = new RangeUnionIterator.Builder<>();
        builder.add(getCurrentMemtable().search(e));
        for (IndexMemtable memtable : getPendingMemtables())
            builder.add(memtable.search(e));

        return builder.build();
    }

    public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
    {
        tracker.update(oldSSTables, newSSTables);
    }

    public ColumnMetadata getDefinition()
    {
        return column;
    }

    public AbstractType<?> getValidator()
    {
        return column.cellValueType();
    }

    public Component getComponent()
    {
        return component;
    }

    public IndexMode getMode()
    {
        return mode;
    }

    public String getColumnName()
    {
        return column.name.toString();
    }

    public String getIndexName()
    {
        return config.isPresent() ? config.get().name : "undefined";
    }

    public AbstractAnalyzer getAnalyzer()
    {
        AbstractAnalyzer analyzer = mode.getAnalyzer(getValidator());
        analyzer.init(config.isPresent() ? config.get().options : Collections.emptyMap(), column.cellValueType());
        return analyzer;
    }

    public View getView()
    {
        return tracker.getView();
    }

    public boolean hasSSTable(SSTableReader sstable)
    {
        return tracker.hasSSTable(sstable);
    }

    public void dropData(Collection<SSTableReader> sstablesToRebuild)
    {
        tracker.dropData(sstablesToRebuild);
    }

    public void dropData(long truncateUntil)
    {
        switchMemtable();
        tracker.dropData(truncateUntil);
    }

    public boolean isIndexed()
    {
        return mode != IndexMode.NOT_INDEXED;
    }

    public boolean isLiteral()
    {
        AbstractType<?> validator = getValidator();
        return isIndexed() ? mode.isLiteral : (validator instanceof UTF8Type || validator instanceof AsciiType);
    }

    public boolean supports(Operator op)
    {
        if (op == Operator.LIKE)
            return isLiteral();

        Op operator = Op.valueOf(op);
        return !(isTokenized && operator == Op.EQ) // EQ is only applicable to non-tokenized indexes
               && operator != Op.IN // IN operator is not supported
               && !(isTokenized && mode.mode == OnDiskIndexBuilder.Mode.CONTAINS && operator == Op.PREFIX) // PREFIX not supported on tokenized CONTAINS mode indexes
               && !(isLiteral() && operator == Op.RANGE) // RANGE only applicable to indexes non-literal indexes
               && mode.supports(operator); // for all other cases let's refer to index itself
    }

    public static ByteBuffer getValueOf(ColumnMetadata column, Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        switch (column.kind)
        {
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                if (row.isStatic())
                    return null;

                return row.clustering().bufferAt(column.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell<?> cell = row.getCell(column);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }
}
