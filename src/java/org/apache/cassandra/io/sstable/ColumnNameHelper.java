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
package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;

import static org.apache.cassandra.utils.ByteBufferUtil.minimalBufferFor;

public class ColumnNameHelper
{
    private static List<ByteBuffer> maybeGrow(List<ByteBuffer> l, int size)
    {
        if (l.size() >= size)
            return l;

        List<ByteBuffer> nl = new ArrayList<>(size);
        nl.addAll(l);
        for (int i = l.size(); i < size; i++)
            nl.add(null);
        return nl;
    }

    private static List<ByteBuffer> getComponents(Composite prefix, int size)
    {
        List<ByteBuffer> l = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            l.add(prefix.get(i));
        return l;
    }

    /**
     * finds the max cell name component(s)
     *
     * Note that this method *can modify maxSeen*.
     *
     * @param maxSeen the max columns seen so far
     * @param candidate the candidate column(s)
     * @param comparator the comparator to use
     * @return a list with the max column(s)
     */
    public static List<ByteBuffer> maxComponents(List<ByteBuffer> maxSeen, Composite candidate, CellNameType comparator)
    {
        // For a cell name, no reason to look more than the clustering prefix
        // (and comparing the collection element would actually crash)
        int size = Math.min(candidate.size(), comparator.clusteringPrefixSize());

        if (maxSeen.isEmpty())
            return getComponents(candidate, size);

        // In most case maxSeen is big enough to hold the result so update it in place in those cases
        maxSeen = maybeGrow(maxSeen, size);

        for (int i = 0; i < size; i++)
            maxSeen.set(i, max(maxSeen.get(i), candidate.get(i), comparator.subtype(i)));

        return maxSeen;
    }

    /**
     * finds the min cell name component(s)
     *
     * Note that this method *can modify maxSeen*.
     *
     * @param minSeen the max columns seen so far
     * @param candidate the candidate column(s)
     * @param comparator the comparator to use
     * @return a list with the min column(s)
     */
    public static List<ByteBuffer> minComponents(List<ByteBuffer> minSeen, Composite candidate, CellNameType comparator)
    {
        // For a cell name, no reason to look more than the clustering prefix
        // (and comparing the collection element would actually crash)
        int size = Math.min(candidate.size(), comparator.clusteringPrefixSize());

        if (minSeen.isEmpty())
            return getComponents(candidate, size);

        // In most case maxSeen is big enough to hold the result so update it in place in those cases
        minSeen = maybeGrow(minSeen, size);

        for (int i = 0; i < size; i++)
            minSeen.set(i, min(minSeen.get(i), candidate.get(i), comparator.subtype(i)));

        return minSeen;
    }

    /**
     * return the min column
     *
     * note that comparator should not be of CompositeType!
     *
     * @param b1 lhs
     * @param b2 rhs
     * @param comparator the comparator to use
     * @return the smallest column according to comparator
     */
    private static ByteBuffer min(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator)
    {
        if (b1 == null)
            return b2;
        if (b2 == null)
            return b1;

        if (comparator.compare(b1, b2) >= 0)
            return b2;
        return b1;
    }

    /**
     * return the max column
     *
     * note that comparator should not be of CompositeType!
     *
     * @param b1 lhs
     * @param b2 rhs
     * @param comparator the comparator to use
     * @return the biggest column according to comparator
     */
    private static ByteBuffer max(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator)
    {
        if (b1 == null)
            return b2;
        if (b2 == null)
            return b1;

        if (comparator.compare(b1, b2) >= 0)
            return b1;
        return b2;
    }

    /**
     * Merge 2 lists of min cell name components.
     *
     * @param minColumnNames lhs
     * @param candidates rhs
     * @param comparator comparator to use
     * @return a list with smallest column names according to (sub)comparator
     */
    public static List<ByteBuffer> mergeMin(List<ByteBuffer> minColumnNames, List<ByteBuffer> candidates, CellNameType comparator)
    {
        if (minColumnNames.isEmpty())
            return minimalBuffersFor(candidates);

        if (candidates.isEmpty())
            return minColumnNames;

        List<ByteBuffer> biggest = minColumnNames.size() > candidates.size() ? minColumnNames : candidates;
        List<ByteBuffer> smallest = minColumnNames.size() > candidates.size() ? candidates : minColumnNames;

        // We want to always copy the smallest list, and maybeGrow does it only if it's actually smaller
        List<ByteBuffer> retList = smallest.size() == biggest.size()
                                 ? new ArrayList<>(smallest)
                                 : maybeGrow(smallest, biggest.size());

        for (int i = 0; i < biggest.size(); i++)
            retList.set(i, minimalBufferFor(min(retList.get(i), biggest.get(i), comparator.subtype(i))));

        return retList;
    }

    private static List<ByteBuffer> minimalBuffersFor(List<ByteBuffer> candidates)
    {
        List<ByteBuffer> minimalBuffers = new ArrayList<ByteBuffer>(candidates.size());
        for (ByteBuffer byteBuffer : candidates)
            minimalBuffers.add(minimalBufferFor(byteBuffer));
        return minimalBuffers;
    }

    /**
     * Merge 2 lists of max cell name components.
     *
     * @param maxColumnNames lhs
     * @param candidates rhs
     * @param comparator comparator to use
     * @return a list with biggest column names according to (sub)comparator
     */
    public static List<ByteBuffer> mergeMax(List<ByteBuffer> maxColumnNames, List<ByteBuffer> candidates, CellNameType comparator)
    {
        if (maxColumnNames.isEmpty())
            return minimalBuffersFor(candidates);

        if (candidates.isEmpty())
            return maxColumnNames;

        List<ByteBuffer> biggest = maxColumnNames.size() > candidates.size() ? maxColumnNames : candidates;
        List<ByteBuffer> smallest = maxColumnNames.size() > candidates.size() ? candidates : maxColumnNames;

        // We want to always copy the smallest list, and maybeGrow does it only if it's actually smaller
        List<ByteBuffer> retList = smallest.size() == biggest.size()
                                 ? new ArrayList<>(smallest)
                                 : maybeGrow(smallest, biggest.size());

        for (int i = 0; i < biggest.size(); i++)
            retList.set(i, minimalBufferFor(max(retList.get(i), biggest.get(i), comparator.subtype(i))));

        return retList;
    }

    /**
     * Checks if the given min/max column names could overlap (i.e they could share some column names based on the max/min column names in the sstables)
     */
    public static boolean overlaps(List<ByteBuffer> minColumnNames1, List<ByteBuffer> maxColumnNames1, List<ByteBuffer> minColumnNames2, List<ByteBuffer> maxColumnNames2, CellNameType comparator)
    {
        if (minColumnNames1.isEmpty() || maxColumnNames1.isEmpty() || minColumnNames2.isEmpty() || maxColumnNames2.isEmpty())
            return true;

        return !(compare(maxColumnNames1, minColumnNames2, comparator) < 0 || compare(minColumnNames1, maxColumnNames2, comparator) > 0);
    }

    private static int compare(List<ByteBuffer> columnNames1, List<ByteBuffer> columnNames2, CellNameType comparator)
    {
        for (int i = 0; i < Math.min(columnNames1.size(), columnNames2.size()); i++)
        {
            int cmp = comparator.subtype(i).compare(columnNames1.get(i), columnNames2.get(i));
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }
}
