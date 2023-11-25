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

package org.apache.cassandra.index.sai.view;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The View is an immutable, point in time, view of the avalailable {@link SSTableIndex}es for an index.
 * <p>
 * The view maintains a {@link RangeTermTree} for querying the view by value range. This is used by the
 * {@link org.apache.cassandra.index.sai.plan.QueryViewBuilder} to select the set of {@link SSTableIndex}es
 * to perform a query without needing to query indexes that are known not to contain to the requested
 * expression value range.
 */
public class View implements Iterable<SSTableIndex>
{
    private final Map<Descriptor, SSTableIndex> view;

    private final RangeTermTree rangeTermTree;

    public View(IndexTermType indexTermType, Collection<SSTableIndex> indexes)
    {
        this.view = new HashMap<>();

        RangeTermTree.Builder rangeTermTreeBuilder = new RangeTermTree.Builder(indexTermType);

        for (SSTableIndex sstableIndex : indexes)
        {
            this.view.put(sstableIndex.getSSTable().descriptor, sstableIndex);
            rangeTermTreeBuilder.add(sstableIndex);
        }

        this.rangeTermTree = rangeTermTreeBuilder.build();
    }

    /**
     * Search for a list of {@link SSTableIndex}es that contain values within
     * the value range requested in the {@link Expression}
     */
    public Collection<SSTableIndex> match(Expression expression)
    {
        if (expression.getIndexOperator() == Expression.IndexOperator.ANN)
            return getIndexes();

        return rangeTermTree.search(expression);
    }

    @Override
    public Iterator<SSTableIndex> iterator()
    {
        return view.values().iterator();
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    public boolean containsSSTable(SSTableReader sstable)
    {
        return view.containsKey(sstable.descriptor);
    }

    public int size()
    {
        return view.size();
    }

    @Override
    public String toString()
    {
        return String.format("View{view=%s}", view);
    }
}
