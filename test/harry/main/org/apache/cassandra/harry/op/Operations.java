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

package org.apache.cassandra.harry.op;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import accord.utils.Invariants;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.util.BitSet;

public class Operations
{
    public static class WriteOp extends PartitionOperation
    {
        private final long cd;
        private final long vds[];
        private final long sds[];

        public WriteOp(long lts, long pd, long cd, long[] vds, long[] sds, Kind kind)
        {
            super(lts, pd, kind);
            this.cd = cd;
            this.vds = vds;
            this.sds = sds;
        }

        public long cd()
        {
            return this.cd;
        }

        public long[] vds()
        {
            return vds;
        }

        public long[] sds()
        {
            return sds;
        }
    }

    public static class DeleteRow extends PartitionOperation
    {
        protected final long cd;

        public DeleteRow(long lts, long pd, long cd)
        {
            super(lts, pd, Kind.DELETE_ROW);
            this.cd = cd;
        }

        public long cd()
        {
            return cd;
        }
    }

    public static class DeleteColumns extends PartitionOperation
    {
        private final long cd;
        private final BitSet regularColumns;
        private final BitSet staticColumns;

        public DeleteColumns(long lts, long pd, long cd, BitSet regularColumns, BitSet staticColumns)
        {
            super(lts, pd, Kind.DELETE_COLUMNS);
            this.cd = cd;
            this.regularColumns = regularColumns;
            this.staticColumns = staticColumns;
        }

        public long cd()
        {
            return cd;
        }

        public BitSet regularColumns()
        {
            return regularColumns;
        }

        public BitSet staticColumns()
        {
            return staticColumns;
        }
    }


    public static class DeleteRange extends PartitionOperation
    {
        final long lowerBound;
        final long upperBound;

        final Relations.RelationKind[] lowBoundRelation;
        final Relations.RelationKind[] highBoundRelation;

        public DeleteRange(long lts, long pd,
                           long lowerCdBound,
                           long upperCdBound,
                           Relations.RelationKind[] lowBoundRelation,
                           Relations.RelationKind[] highBoundRelation)
        {
            super(lts, pd, Kind.DELETE_RANGE);
            this.lowerBound = lowerCdBound;
            this.upperBound = upperCdBound;
            this.lowBoundRelation = lowBoundRelation;
            this.highBoundRelation = highBoundRelation;
        }

        public Relations.RelationKind[] lowerBoundRelation()
        {
            return lowBoundRelation;
        }

        public Relations.RelationKind[] upperBoundRelation()
        {
            return highBoundRelation;
        }

        public long lowerBound()
        {
            return lowerBound;
        }

        public long upperBound()
        {
            return upperBound;
        }
    }

    public static abstract class SelectStatement extends PartitionOperation
    {
        public SelectStatement(long lts, long pd, Kind kind)
        {
            super(lts, pd, kind);
        }

        public abstract ClusteringOrderBy orderBy();
        public abstract BitSet selection();
    }

    public static class SelectRange extends SelectStatement
    {
        final long lowerBound;
        final long upperBound;

        final Relations.RelationKind[] lowerBoundRelation;
        final Relations.RelationKind[] upperBoundRelation;

        public SelectRange(long lts, long pd,
                           long lowerBound,
                           long highBound,
                           Relations.RelationKind[] lowerBoundRelation,
                           Relations.RelationKind[] upperBoundRelation)
        {
            super(lts, pd, Kind.SELECT_RANGE);
            this.lowerBound = lowerBound;
            this.upperBound = highBound;
            this.lowerBoundRelation = lowerBoundRelation;
            this.upperBoundRelation = upperBoundRelation;
        }

        @Override
        public ClusteringOrderBy orderBy()
        {
            return ClusteringOrderBy.ASC;
        }

        @Override
        public BitSet selection()
        {
            return MagicConstants.ALL_COLUMNS;
        }

        public Relations.RelationKind[] lowerBoundRelation()
        {
            return lowerBoundRelation;
        }

        public Relations.RelationKind[] upperBoundRelation()
        {
            return upperBoundRelation;
        }

        public long lowerBound()
        {
            return lowerBound;
        }

        public long upperBound()
        {
            return upperBound;
        }
    }

    public static class SelectCustom extends SelectStatement
    {
        private final Relations.Relation[] ckRelations;
        private final Relations.Relation[] regularRelations;
        private final Relations.Relation[] staticRelations;

        public SelectCustom(long lts, long pd,
                            Relations.Relation[] ckRelations,
                            Relations.Relation[] regularRelations,
                            Relations.Relation[] staticRelations)
        {
            super(lts, pd, Kind.SELECT_CUSTOM);
            this.ckRelations = ckRelations;
            this.regularRelations = regularRelations;
            this.staticRelations = staticRelations;
        }

        @Override
        public ClusteringOrderBy orderBy()
        {
            return ClusteringOrderBy.ASC;
        }

        @Override
        public BitSet selection()
        {
            return MagicConstants.ALL_COLUMNS;
        }

        public Relations.Relation[] ckRelations()
        {
            return ckRelations;
        }

        public Relations.Relation[] staticRelations()
        {
            return staticRelations;
        }

        public Relations.Relation[] regularRelations()
        {
            return regularRelations;
        }
    }

    public static class SelectRow extends SelectStatement
    {
        private final long cd;
        public SelectRow(long lts, long pd, long cd)
        {
            super(lts, pd, Kind.SELECT_ROW);
            this.cd = cd;
        }

        public long cd()
        {
            return cd;
        }

        public ClusteringOrderBy orderBy()
        {
            return ClusteringOrderBy.ASC;
        }

        @Override
        public BitSet selection()
        {
            return MagicConstants.ALL_COLUMNS;
        }
    }

    public static class SelectPartition extends SelectStatement
    {
        private final ClusteringOrderBy orderBy;

        public SelectPartition(long lts, long pd)
        {
            this(lts, pd, ClusteringOrderBy.ASC);
        }

        public SelectPartition(long lts, long pd, ClusteringOrderBy orderBy)
        {
            super(lts, pd, Kind.SELECT_PARTITION);
            this.orderBy = orderBy;
        }

        public SelectPartition orderBy(ClusteringOrderBy orderBy)
        {
            return new SelectPartition(lts, pd, orderBy);
        }

        public ClusteringOrderBy orderBy()
        {
            return orderBy;
        }

        @Override
        public BitSet selection()
        {
            return MagicConstants.ALL_COLUMNS;
        }
    }

    public static class DeletePartition extends PartitionOperation
    {
        public DeletePartition(long lts, long pd)
        {
            super(lts, pd, Kind.DELETE_PARTITION);
        }
    }

    public interface DeleteColumnsOp extends Operation
    {
        long cd();
        BitSet regularColumns();
        BitSet staticColumns();
    }

    public interface Operation
    {
        long lts();
        Kind kind();
    }

    public static abstract class PartitionOperation implements Operation
    {
        public final long lts;
        public final long pd;
        public final Kind kind;

        public PartitionOperation(long lts, long pd, Kind kind)
        {
            this.pd = pd;
            this.lts = lts;
            this.kind = kind;
        }

        public final long pd()
        {
            return pd;
        }

        @Override
        public final long lts()
        {
            return lts;
        }

        @Override
        public final Kind kind()
        {
            return kind;
        }

        public String toString()
        {
            return "Operation{" +
                   "  lts=" + lts +
                   "  pd=" + pd +
                   ", kind=" + kind +
                   '}';
        }
    }

    public enum Kind
    {
        /**
         * Custom operation such as flush
         */
        CUSTOM(false),

        UPDATE(true),
        INSERT(true),

        DELETE_PARTITION(true),
        DELETE_ROW(false),
        DELETE_COLUMNS(true),
        DELETE_RANGE(false),

        SELECT_PARTITION(true),
        SELECT_ROW(false),
        SELECT_RANGE(true),
        SELECT_CUSTOM(true);
        public final boolean partititonLevel;

        Kind(boolean partitionLevel)
        {
            this.partititonLevel = partitionLevel;
        }

    }

    public static class CustomRunnableOperation implements Operation
    {
        public final long lts;
        public final long opId;
        private final Runnable run;

        public CustomRunnableOperation(long lts, long opId, Runnable runnable)
        {
            assert opId >= 0;
            this.lts = lts;
            this.opId = opId;
            this.run = runnable;
        }

        public void execute()
        {
            run.run();
        }

        @Override
        public long lts()
        {
            return lts;
        }

        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }
    }

    /**
     * ClusteringOrder by should be understood in terms of how we're going to iterate through this partition
     * (in other words, if first clustering component order is DESC, we'll iterate in ASC order)
     */
    public enum ClusteringOrderBy
    {
        ASC, DESC
    }

    public interface Selection
    {
        // TODO: allow expressions here
        Collection<ColumnSpec<?>> columns();
        boolean includeTimestamps();
        boolean isWildcard();

        boolean selects(ColumnSpec<?> column);
        boolean selectsAllOf(List<ColumnSpec<?>> subSelection);
        int indexOf(ColumnSpec<?> column);

        static Selection fromBitSet(BitSet bitSet, SchemaSpec schema)
        {
            if (bitSet == MagicConstants.ALL_COLUMNS)
            {
                Map<ColumnSpec<?>, Integer> columns = new HashMap<>();
                for (int i = 0; i < schema.allColumnInSelectOrder.size(); i++)
                    columns.put(schema.allColumnInSelectOrder.get(i), i);
                return new Wildcard(columns);
            }
            else
            {
                Invariants.checkState(schema.allColumnInSelectOrder.size() == bitSet.size());
                Map<ColumnSpec<?>, Integer> columns = new HashMap<>();
                for (int i = 0; i < schema.allColumnInSelectOrder.size(); i++)
                {
                    if (bitSet.isSet(i))
                        columns.put(schema.allColumnInSelectOrder.get(i), i);
                }
                // TODO: timestamp
                return new Columns(columns, false);
            }
        }
    }

    public static class Wildcard extends Columns
    {
        private Wildcard(Map<ColumnSpec<?>, Integer> columns)
        {
            super(columns, false);
        }

        @Override
        public Collection<ColumnSpec<?>> columns()
        {
            return columns.keySet();
        }

        @Override
        public boolean includeTimestamps()
        {
            return false;
        }

        @Override
        public boolean isWildcard()
        {
            return true;
        }
    }

    public static class Columns implements Selection
    {
        final Map<ColumnSpec<?>, Integer> columns;
        final boolean includeTimestamp;

        public Columns(Map<ColumnSpec<?>, Integer> columns, boolean includeTimestamp)
        {
            this.columns = columns;
            this.includeTimestamp = includeTimestamp;
        }

        @Override
        public Collection<ColumnSpec<?>> columns()
        {
            return columns.keySet();
        }

        @Override
        public boolean includeTimestamps()
        {
            return includeTimestamp;
        }

        @Override
        public boolean isWildcard()
        {
            return false;
        }

        public boolean selects(ColumnSpec<?> column)
        {
            return columns.containsKey(column);
        }

        public boolean selectsAllOf(List<ColumnSpec<?>> subSelection)
        {
            for (ColumnSpec<?> column : subSelection)
            {
                if (!selects(column))
                    return false;
            }
            return true;
        }

        public int indexOf(ColumnSpec<?> column)
        {
            return columns.get(column);
        }
    }
}
