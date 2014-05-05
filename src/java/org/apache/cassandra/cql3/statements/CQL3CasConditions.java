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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASConditions;
import org.apache.cassandra.utils.Pair;

/**
 * Processed CAS conditions on potentially multiple rows of the same partition.
 */
public class CQL3CasConditions implements CASConditions
{
    private final CFMetaData cfm;
    private final long now;

    // We index RowCondition by the prefix of the row they applied to for 2 reasons:
    //   1) this allows to keep things sorted to build the ColumnSlice array below
    //   2) this allows to detect when contradictory conditions are set (not exists with some other conditions on the same row)
    private final SortedMap<ByteBuffer, RowCondition> conditions;

    public CQL3CasConditions(CFMetaData cfm, long now)
    {
        this.cfm = cfm;
        // We will use now for Column.isLive() which expects milliseconds but the argument is in microseconds.
        this.now = now / 1000;
        this.conditions = new TreeMap<>(cfm.comparator);
    }

    public void addNotExist(ColumnNameBuilder prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix.build(), new NotExistCondition(prefix, now));
        if (previous != null && !(previous instanceof NotExistCondition))
        {
            // these should be prevented by the parser, but it doesn't hurt to check
            if (previous instanceof ExistCondition)
                throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            else
                throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
    }

    public void addExist(ColumnNameBuilder prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix.build(), new ExistCondition(prefix, now));
        // this should be prevented by the parser, but it doesn't hurt to check
        if (previous != null && previous instanceof NotExistCondition)
            throw new InvalidRequestException("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
    }

    public void addConditions(ColumnNameBuilder prefix, Collection<ColumnCondition> conds, List<ByteBuffer> variables) throws InvalidRequestException
    {
        ByteBuffer b = prefix.build();
        RowCondition condition = conditions.get(b);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix, now);
            conditions.put(b, condition);
        }
        else if (!(condition instanceof ColumnsConditions))
        {
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
        }
        ((ColumnsConditions)condition).addConditions(conds, variables);
    }

    public IDiskAtomFilter readFilter()
    {
        assert !conditions.isEmpty();
        ColumnSlice[] slices = new ColumnSlice[conditions.size()];
        int i = 0;
        // We always read CQL rows entirely as on CAS failure we want to be able to distinguish between "row exists
        // but all values on why there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Map.Entry<ByteBuffer, RowCondition> entry : conditions.entrySet())
            slices[i++] = new ColumnSlice(entry.getKey(), entry.getValue().rowPrefix.buildAsEndOfRange());

        int toGroup = cfm.getCfDef().isCompact ? -1 : cfm.clusteringKeyColumns().size();
        return new SliceQueryFilter(slices, false, slices.length, toGroup);
    }

    public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
    {
        for (RowCondition condition : conditions.values())
        {
            if (!condition.appliesTo(current))
                return false;
        }
        return true;
    }

    private static abstract class RowCondition
    {
        public final ColumnNameBuilder rowPrefix;
        protected final long now;

        protected RowCondition(ColumnNameBuilder rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public abstract boolean appliesTo(ColumnFamily current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(ColumnNameBuilder rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return true;

            Iterator<Column> iter = current.iterator(new ColumnSlice[]{ new ColumnSlice(rowPrefix.build(), rowPrefix.buildAsEndOfRange()) });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return false;
            return true;
        }
    }

    private static class ExistCondition extends RowCondition
    {
        private ExistCondition(ColumnNameBuilder rowPrefix, long now)
        {
            super (rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return false;

            Iterator<Column> iter = current.iterator(new ColumnSlice[]{ new ColumnSlice(rowPrefix.build(), rowPrefix.buildAsEndOfRange())});
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return true;
            return false;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Map<Pair<ColumnIdentifier, ByteBuffer>, ColumnCondition.Bound> conditions = new HashMap<>();

        private ColumnsConditions(ColumnNameBuilder rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public void addConditions(Collection<ColumnCondition> conds, List<ByteBuffer> variables) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                // We will need the variables in appliesTo but with protocol batches, each condition in this object can have a
                // different list of variables.
                ColumnCondition.Bound current = condition.bind(variables);
                ColumnCondition.Bound previous = conditions.put(Pair.create(condition.column.name, current.getCollectionElementValue()), current);
                // If 2 conditions are actually equal, let it slide
                if (previous != null && !previous.equals(current))
                    throw new InvalidRequestException("Duplicate and incompatible conditions for column " + condition.column.name);
            }
        }

        public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition.Bound condition : conditions.values())
                if (!condition.appliesTo(rowPrefix, current, now))
                    return false;
            return true;
        }
    }
}
