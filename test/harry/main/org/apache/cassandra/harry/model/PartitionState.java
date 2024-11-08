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

package org.apache.cassandra.harry.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.util.StringUtils;

public class PartitionState implements Iterable<PartitionState.RowState>
{
    public static long STATIC_CLUSTERING = MagicConstants.NIL_DESCR;

    private static final Logger logger = LoggerFactory.getLogger(PartitionState.class);

    public final long pd;

    final List<Long> visitedLts = new ArrayList<>();
    final List<Long> skippedLts = new ArrayList<>();

    RowState staticRow;
    NavigableMap<Long, RowState> rows;

    final ValueGenerators valueGenerators;

    public PartitionState(long pd, ValueGenerators valueGenerators)
    {
        this.pd = pd;
        this.rows = new TreeMap<>(valueGenerators.ckGen.descriptorsComparator());
        this.valueGenerators = valueGenerators;
        this.staticRow = new RowState(this,
                                      STATIC_CLUSTERING,
                                      arr(valueGenerators.staticColumnGens.size(), MagicConstants.NIL_DESCR),
                                      arr(valueGenerators.staticColumnGens.size(), MagicConstants.NO_TIMESTAMP));
    }

    /**
     * Returns a navigable map of rows
     */
    public NavigableMap<Long, RowState> rows()
    {
        return rows;
    }

    public void writeStatic(long[] sds, long lts)
    {
        staticRow = updateRowState(staticRow, valueGenerators.staticColumnGens, STATIC_CLUSTERING, sds, lts, false);
    }

    public void writeRegular(long cd, long[] vds, long lts, boolean writePrimaryKeyLiveness)
    {
        rows.compute(cd, (cd_, current) -> updateRowState(current, valueGenerators.regularColumnGens, cd, vds, lts, writePrimaryKeyLiveness));
    }

    public void delete(Operations.DeleteRange delete, long lts)
    {
        // TODO: inefficient; need to search for lower/higher bounds
        rows.entrySet().removeIf(e -> Relations.matchRange(valueGenerators.ckGen,
                                                           valueGenerators.ckComparators,
                                                           delete.lowerBound(),
                                                           delete.upperBound(),
                                                           delete.lowerBoundRelation(),
                                                           delete.upperBoundRelation(),
                                                           e.getValue().cd));
    }

    public void reverse()
    {
        rows = rows.descendingMap();
    }

    public void filter(Operations.SelectStatement select)
    {
        switch (select.kind)
        {
            case SELECT_PARTITION:
                // selecting everything
                break;
            case SELECT_ROW:
                filterInternal((Operations.SelectRow) select);
                break;
            case SELECT_RANGE:
                filterInternal((Operations.SelectRange) select);
                break;
            case SELECT_CUSTOM:
                filterInternal((Operations.SelectCustom) select);
                break;
            default:
                throw new IllegalStateException("Filtering not implemented for " + select);
        }
    }

    private void filterInternal(Operations.SelectRow select)
    {
        // TODO: inefficient; need to search for lower/higher bounds
        rows.entrySet().removeIf(e -> e.getValue().cd != select.cd());
    }

    private void filterInternal(Operations.SelectRange select)
    {
        // TODO: inefficient; need to search for lower/higher bounds
        rows.entrySet().removeIf(e -> !Relations.matchRange(valueGenerators.ckGen,
                                                            valueGenerators.ckComparators,
                                                            select.lowerBound(),
                                                            select.upperBound(),
                                                            select.lowerBoundRelation(),
                                                            select.upperBoundRelation(),
                                                            e.getValue().cd));
    }

    private void filterInternal(Operations.SelectCustom select)
    {
        // TODO: inefficient; need to search for lower/higher bounds
        rows.entrySet().removeIf(e -> {
            Map<Long, Object[]> cache = new HashMap<>();
            for (Relations.Relation relation : select.ckRelations())
            {
                Object[] query = cache.computeIfAbsent(relation.descriptor, valueGenerators.ckGen::inflate);
                Object[] match = cache.computeIfAbsent(e.getValue().cd, valueGenerators.ckGen::inflate);
                if (!relation.kind.match(valueGenerators.ckComparators.get(relation.column), match[relation.column], query[relation.column]))
                    return true; // true means "no match", so remove from resultset
            }

            for (Relations.Relation relation : select.regularRelations())
            {
                Object query = valueGenerators.regularColumnGens.get(relation.column).inflate(relation.descriptor);
                long descriptor = e.getValue().vds[relation.column];
                if (MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(descriptor)) // TODO: do we allow UNSET queries?
                    return true;
                Object match = valueGenerators.regularColumnGens.get(relation.column).inflate(e.getValue().vds[relation.column]);
                if (!relation.kind.match(valueGenerators.regularComparators.get(relation.column), match, query))
                    return true;
            }

            for (Relations.Relation relation : select.staticRelations())
            {
                Object query = valueGenerators.staticColumnGens.get(relation.column).inflate(relation.descriptor);
                long descriptor = e.getValue().partitionState.staticRow.vds[relation.column];
                if (MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(descriptor)) // TODO: do we allow UNSET queries?
                    return true;
                Object match = valueGenerators.staticColumnGens.get(relation.column).inflate(e.getValue().partitionState.staticRow.vds[relation.column]);
                if (!relation.kind.match(valueGenerators.staticComparators.get(relation.column), match, query))
                    return true;
            }

            return false;
        });
    }

    public void delete(long cd, long lts)
    {
        RowState state = rows.remove(cd);
        if (state != null)
        {
            for (long v : state.lts)
                assert lts >= v : String.format("Attempted to remove a row with a tombstone that has older timestamp (%d): %s", lts, state);
        }
    }

    public boolean isEmpty()
    {
        return rows.isEmpty();
    }

    /**
     * Method used to update row state of both static and regular rows.
     */
    private RowState updateRowState(RowState currentState, List<Bijections.IndexedBijection<Object>> columns, long cd, long[] vds, long lts, boolean writePrimaryKeyLiveness)
    {
        if (currentState == null)
        {
            long[] ltss = new long[vds.length];
            long[] vdsCopy = new long[vds.length];
            for (int i = 0; i < vds.length; i++)
            {
                if (vds[i] != MagicConstants.UNSET_DESCR)
                {
                    ltss[i] = lts;
                    vdsCopy[i] = vds[i];
                }
                else
                {
                    ltss[i] = MagicConstants.NO_TIMESTAMP;
                    vdsCopy[i] = MagicConstants.NIL_DESCR;
                }
            }

            currentState = new RowState(this, cd, vdsCopy, ltss);
        }
        else
        {
            assert currentState.vds.length == vds.length : String.format("Vds: %d, sds: %d", currentState.vds.length, vds.length);
            for (int i = 0; i < vds.length; i++)
            {
                if (vds[i] == MagicConstants.UNSET_DESCR)
                    continue;

                assert lts >= currentState.lts[i] : String.format("Out-of-order LTS: %d. Max seen: %s", lts, currentState.lts[i]); // sanity check; we're iterating in lts order

                if (currentState.lts[i] == lts)
                {
                    // Timestamp collision case
                    Bijections.IndexedBijection<?> column = columns.get(i);
                    if (column.compare(vds[i], currentState.vds[i]) > 0)
                        currentState.vds[i] = vds[i];
                }
                else
                {
                    currentState.vds[i] = vds[i];
                    assert lts > currentState.lts[i];
                    currentState.lts[i] = lts;
                }
            }
        }

        if (writePrimaryKeyLiveness)
            currentState.hasPrimaryKeyLivenessInfo = true;

        return currentState;
    }

    public void deleteRegularColumns(long lts, long cd, BitSet columns)
    {
        deleteColumns(lts, rows.get(cd), columns);
    }

    public void deleteStaticColumns(long lts, BitSet columns)
    {
        deleteColumns(lts, staticRow, columns);
    }

    public void deleteColumns(long lts, RowState state, BitSet columns)
    {
        if (state == null)
            return;

        //TODO: optimise by iterating over the columns that were removed by this deletion
        //TODO: optimise final decision to fully remove the column by counting a number of set/unset columns
        boolean allNil = true;
        for (int i = 0; i < state.vds.length; i++)
        {
            if (columns.isSet(i))
            {
                state.vds[i] = MagicConstants.NIL_DESCR;
                state.lts[i] = MagicConstants.NO_TIMESTAMP;
            }
            else if (state.vds[i] != MagicConstants.NIL_DESCR)
            {
                allNil = false;
            }
        }

        if (state.cd != STATIC_CLUSTERING && allNil & !state.hasPrimaryKeyLivenessInfo)
            delete(state.cd, lts);
    }

    public void deletePartition(long lts)
    {
        rows.clear();
        Arrays.fill(staticRow.vds, MagicConstants.NIL_DESCR);
        Arrays.fill(staticRow.lts, MagicConstants.NO_TIMESTAMP);
    }

    public Iterator<RowState> iterator()
    {
        return iterator(false);
    }

    public Iterator<RowState> iterator(boolean reverse)
    {
        if (reverse)
            return rows.descendingMap().values().iterator();

        return rows.values().iterator();
    }

    public Collection<RowState> rows(boolean reverse)
    {
        if (reverse)
            return rows.descendingMap().values();

        return rows.values();
    }

    public RowState staticRow()
    {
        return staticRow;
    }

    public String toString()
    {
        // TODO: display inices not LTS when doing tostring
        StringBuilder sb = new StringBuilder();

        sb.append("Visited LTS: " + visitedLts).append("\n");
        sb.append("Skipped LTS: " + skippedLts).append("\n");

        if (staticRow != null)
        {
            sb.append("Static row:\n" + staticRow.toString(valueGenerators)).append("\n");
            sb.append("\n");
        }

        for (RowState row : rows.values())
            sb.append(row.toString(valueGenerators)).append("\n");

        return sb.toString();
    }

    public static class RowState
    {
        public boolean hasPrimaryKeyLivenessInfo = false;

        public final PartitionState partitionState;
        public final long cd;
        public final long[] vds;
        public final long[] lts;

        public RowState(PartitionState partitionState,
                        long cd,
                        long[] vds,
                        long[] lts)
        {
            this.partitionState = partitionState;
            this.cd = cd;
            this.vds = vds;
            this.lts = lts;
        }

        public RowState clone()
        {
            RowState rowState = new RowState(partitionState, cd, Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length));
            rowState.hasPrimaryKeyLivenessInfo = hasPrimaryKeyLivenessInfo;
            return rowState;
        }

        private static String toString(long[] descriptors, List<Bijections.IndexedBijection<Object>> gens)
        {
            int[] idxs = new int[gens.size()];
            for (int i = 0; i < descriptors.length; i++)
                idxs[i] = descrToIdxForToString(gens.get(i), descriptors[i]);
            return StringUtils.toString(idxs);
        }

        public static int descrToIdxForToString(Bijections.IndexedBijection<?> gen, long descr)
        {
            if (descr == MagicConstants.UNSET_DESCR)
                return MagicConstants.UNSET_IDX;

            if (descr == MagicConstants.NIL_DESCR)
                return MagicConstants.NIL_IDX;

            return gen.idxFor(descr);
        }

        public String toString(ValueGenerators valueGenerators)
        {
            if (cd == STATIC_CLUSTERING)
            {
                return " rowStateRow("
                       + valueGenerators.pkGen.idxFor(partitionState.pd) +
                       ", STATIC" +
                       ", statics(" + toString(partitionState.staticRow.vds, valueGenerators.staticColumnGens) + ")" +
                       ", lts(" + StringUtils.toString(partitionState.staticRow.lts) + ")";
            }
            else
            {
                return " rowStateRow("
                       + valueGenerators.pkGen.idxFor(partitionState.pd) +
                       ", " + descrToIdxForToString(valueGenerators.ckGen, cd) +
                       ", vds(" + toString(vds, valueGenerators.regularColumnGens) + ")" +
                       ", lts(" + StringUtils.toString(lts) + ")";
            }
        }
    }

    public static long[] arr(int length, long fill)
    {
        long[] arr = new long[length];
        Arrays.fill(arr, fill);
        return arr;
    }
}
