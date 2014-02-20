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
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.CASConditions;

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
    private final SortedMap<Composite, RowCondition> conditions;

    public CQL3CasConditions(CFMetaData cfm, long now)
    {
        this.cfm = cfm;
        this.now = now;
        this.conditions = new TreeMap<>(cfm.comparator);
    }

    public void addNotExist(Composite prefix) throws InvalidRequestException
    {
        RowCondition previous = conditions.put(prefix, new NotExistCondition(prefix, now));
        if (previous != null && !(previous instanceof NotExistCondition))
            throw new InvalidRequestException("Cannot mix IF conditions and IF NOT EXISTS for the same row");
    }

    public void addConditions(Composite prefix, Collection<ColumnCondition> conds, List<ByteBuffer> variables) throws InvalidRequestException
    {
        RowCondition condition = conditions.get(prefix);
        if (condition == null)
        {
            condition = new ColumnsConditions(prefix, now);
            conditions.put(prefix, condition);
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
        // but all values for which there were conditions are null" and "row doesn't exists", and we can't rely on the
        // row marker for that (see #6623)
        for (Composite prefix : conditions.keySet())
            slices[i++] = prefix.slice();

        return new SliceQueryFilter(slices, false, slices.length, cfm.clusteringColumns().size());
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
        public final Composite rowPrefix;
        protected final long now;

        protected RowCondition(Composite rowPrefix, long now)
        {
            this.rowPrefix = rowPrefix;
            this.now = now;
        }

        public abstract boolean appliesTo(ColumnFamily current) throws InvalidRequestException;
    }

    private static class NotExistCondition extends RowCondition
    {
        private NotExistCondition(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public boolean appliesTo(ColumnFamily current)
        {
            if (current == null)
                return true;

            Iterator<Cell> iter = current.iterator(new ColumnSlice[]{ rowPrefix.slice() });
            while (iter.hasNext())
                if (iter.next().isLive(now))
                    return false;
            return true;
        }
    }

    private static class ColumnsConditions extends RowCondition
    {
        private final Map<ColumnIdentifier, ColumnCondition> conditions = new HashMap<>();

        private ColumnsConditions(Composite rowPrefix, long now)
        {
            super(rowPrefix, now);
        }

        public void addConditions(Collection<ColumnCondition> conds, List<ByteBuffer> variables) throws InvalidRequestException
        {
            for (ColumnCondition condition : conds)
            {
                // We will need the variables in appliesTo but with protocol batches, each condition in this object can have a
                // different list of variables. So attach them to the condition directly, it's not particulary elegant but its simpler
                ColumnCondition previous = conditions.put(condition.column.name, condition.attach(variables));
                // If 2 conditions are actually equal, let it slide
                if (previous != null && !previous.equalsTo(condition))
                    throw new InvalidRequestException("Duplicate and incompatible conditions for column " + condition.column.name);
            }
        }

        public boolean appliesTo(ColumnFamily current) throws InvalidRequestException
        {
            if (current == null)
                return conditions.isEmpty();

            for (ColumnCondition condition : conditions.values())
                if (!condition.appliesTo(rowPrefix, current, now))
                    return false;
            return true;
        }
    }
}
