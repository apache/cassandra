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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ColumnToCollectionType;
import org.apache.cassandra.db.marshal.CompositeType;

import static org.apache.cassandra.utils.ByteBufferUtil.minimalBufferFor;

public class ColumnNameHelper
{
    /**
     * finds the max column name(s)
     *
     * if comparator is of CompositeType, candidate will be split into its components, and each
     * component is compared to the component on the same place in maxSeen, and then returning the list
     * with the max columns.
     *
     * will collect at most the number of types in the comparator.
     *
     * if comparator is not CompositeType, maxSeen is assumed to be of size 1 and the item there is
     * compared to the candidate.
     *
     * @param maxSeen the max columns seen so far
     * @param candidate the candidate column(s)
     * @param comparator the comparator to use
     * @return a list with the max column(s)
     */
    public static List<ByteBuffer> maxComponents(List<ByteBuffer> maxSeen, ByteBuffer candidate, AbstractType<?> comparator)
    {
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)comparator;
            if (maxSeen.isEmpty())
                return Arrays.asList(ct.split(candidate));

            int typeCount = getTypeCount(ct);

            List<ByteBuffer> components = Arrays.asList(ct.split(candidate));
            List<ByteBuffer> biggest = maxSeen.size() > components.size() ? maxSeen : components;
            // if typecount is less than both the components and maxseen, we only keep typecount columns.
            int minSize = Math.min(typeCount, Math.min(components.size(), maxSeen.size()));
            int maxSize = Math.min(typeCount, biggest.size());
            List<ByteBuffer> retList = new ArrayList<ByteBuffer>(maxSize);

            for (int i = 0; i < minSize; i++)
                retList.add(ColumnNameHelper.max(maxSeen.get(i), components.get(i), ct.types.get(i)));
            for (int i = minSize; i < maxSize; i++)
                retList.add(biggest.get(i));

            return retList;
        }
        else
        {
            if (maxSeen.size() == 0)
                return Collections.singletonList(candidate);
            return Collections.singletonList(ColumnNameHelper.max(maxSeen.get(0), candidate, comparator));
        }
    }
    /**
     * finds the min column name(s)
     *
     * if comparator is of CompositeType, candidate will be split into its components, and each
     * component is compared to the component on the same place in minSeen, and then returning the list
     * with the min columns.
     *
     * if comparator is not CompositeType, maxSeen is assumed to be of size 1 and the item there is
     * compared to the candidate.
     *
     * @param minSeen the max columns seen so far
     * @param candidate the candidate column(s)
     * @param comparator the comparator to use
     * @return a list with the min column(s)
     */
    public static List<ByteBuffer> minComponents(List<ByteBuffer> minSeen, ByteBuffer candidate, AbstractType<?> comparator)
    {
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)comparator;
            if (minSeen.isEmpty())
                return Arrays.asList(ct.split(candidate));

            int typeCount = getTypeCount(ct);

            List<ByteBuffer> components = Arrays.asList(ct.split(candidate));
            List<ByteBuffer> biggest = minSeen.size() > components.size() ? minSeen : components;
            // if typecount is less than both the components and maxseen, we only collect typecount columns.
            int minSize = Math.min(typeCount, Math.min(components.size(), minSeen.size()));
            int maxSize = Math.min(typeCount, biggest.size());
            List<ByteBuffer> retList = new ArrayList<ByteBuffer>(maxSize);

            for (int i = 0; i < minSize; i++)
                retList.add(ColumnNameHelper.min(minSeen.get(i), components.get(i), ct.types.get(i)));
            for (int i = minSize; i < maxSize; i++)
                retList.add(biggest.get(i));

            return retList;
        }
        else
        {
            if (minSeen.size() == 0)
                return Collections.singletonList(candidate);
            return Collections.singletonList(ColumnNameHelper.min(minSeen.get(0), candidate, comparator));

        }
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
        if (comparator.compare(b1, b2) >= 0)
            return b1;
        return b2;
    }

    /**
     * if columnNameComparator is CompositeType the columns are compared by components using the subcomparator
     * on the same position.
     *
     * if comparator is not CompositeType, the lists are assumed to be of max size 1 and compared using the comparator
     * directly.
     *
     * @param minColumnNames lhs
     * @param candidates rhs
     * @param columnNameComparator comparator to use
     * @return a list with smallest column names according to (sub)comparator
     */
    public static List<ByteBuffer> mergeMin(List<ByteBuffer> minColumnNames, List<ByteBuffer> candidates, AbstractType<?> columnNameComparator)
    {
        if (minColumnNames.isEmpty())
            return minimalBuffersFor(candidates);

        if (candidates.isEmpty())
            return minColumnNames;

        if (columnNameComparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)columnNameComparator;
            List<ByteBuffer> biggest = minColumnNames.size() > candidates.size() ? minColumnNames : candidates;
            int typeCount = getTypeCount(ct);
            int minSize = Math.min(typeCount, Math.min(minColumnNames.size(), candidates.size()));
            int maxSize = Math.min(typeCount, biggest.size());

            List<ByteBuffer> retList = new ArrayList<ByteBuffer>(maxSize);

            for (int i = 0; i < minSize; i++)
                retList.add(minimalBufferFor(min(minColumnNames.get(i), candidates.get(i), ct.types.get(i))));
            for (int i = minSize; i < maxSize; i++)
                retList.add(minimalBufferFor(biggest.get(i)));

            return retList;
        }
        else
        {
            return Collections.singletonList(minimalBufferFor(min(minColumnNames.get(0), candidates.get(0), columnNameComparator)));
        }
    }

    private static List<ByteBuffer> minimalBuffersFor(List<ByteBuffer> candidates)
    {
        List<ByteBuffer> minimalBuffers = new ArrayList<ByteBuffer>(candidates.size());
        for (ByteBuffer byteBuffer : candidates)
            minimalBuffers.add(minimalBufferFor(byteBuffer));
        return minimalBuffers;
    }

    /**
     * if columnNameComparator is CompositeType the columns are compared by components using the subcomparator
     * on the same position.
     *
     * if comparator is not CompositeType, the lists are assumed to be of max size 1 and compared using the comparator
     * directly.
     *
     * @param maxColumnNames lhs
     * @param candidates rhs
     * @param columnNameComparator comparator to use
     * @return a list with biggest column names according to (sub)comparator
     */
    public static List<ByteBuffer> mergeMax(List<ByteBuffer> maxColumnNames, List<ByteBuffer> candidates, AbstractType<?> columnNameComparator)
    {
        if (maxColumnNames.isEmpty())
            return minimalBuffersFor(candidates);

        if (candidates.isEmpty())
            return maxColumnNames;

        if (columnNameComparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)columnNameComparator;
            List<ByteBuffer> biggest = maxColumnNames.size() > candidates.size() ? maxColumnNames : candidates;
            int typeCount = getTypeCount(ct);
            int minSize = Math.min(typeCount, Math.min(maxColumnNames.size(), candidates.size()));
            int maxSize = Math.min(typeCount, biggest.size());
            List<ByteBuffer> retList = new ArrayList<ByteBuffer>(maxSize);

            for (int i = 0; i < minSize; i++)
                retList.add(minimalBufferFor(max(maxColumnNames.get(i), candidates.get(i), ct.types.get(i))));
            for (int i = minSize; i < maxSize; i++)
                retList.add(minimalBufferFor(biggest.get(i)));

            return retList;
        }
        else
        {
            return Collections.singletonList(minimalBufferFor(max(maxColumnNames.get(0), candidates.get(0), columnNameComparator)));
        }

    }

    private static int getTypeCount(CompositeType ct)
    {
        return ct.types.get(ct.types.size() - 1) instanceof ColumnToCollectionType ? ct.types.size() - 1 : ct.types.size();
    }
}
