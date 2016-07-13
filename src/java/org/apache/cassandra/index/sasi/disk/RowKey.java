/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyclustering ownership.  The ASF licenses this file
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

package org.apache.cassandra.index.sasi.disk;

import java.util.*;

import org.apache.cassandra.db.*;

/**
 * Primary key of the found row, a combination of the Partition Key
 * and clustering that belongs to the row.
 */
public class RowKey implements Comparable<RowKey>
{

    public final DecoratedKey decoratedKey;
    public final Clustering clustering;

    private final ClusteringComparator comparator;

    public RowKey(DecoratedKey primaryKey, Clustering clustering, ClusteringComparator comparator)
    {
        this.decoratedKey = primaryKey;
        this.clustering = clustering;
        this.comparator = comparator;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowKey rowKey = (RowKey) o;

        if (decoratedKey != null ? !decoratedKey.equals(rowKey.decoratedKey) : rowKey.decoratedKey != null)
            return false;
        return clustering != null ? clustering.equals(rowKey.clustering) : rowKey.clustering == null;
    }

    public int hashCode()
    {
        int result = decoratedKey != null ? decoratedKey.hashCode() : 0;
        result = 31 * result + (clustering != null ? clustering.hashCode() : 0);
        return result;
    }

    public int compareTo(RowKey other)
    {
        int cmp = this.decoratedKey.compareTo(other.decoratedKey);
        if (cmp == 0)
        {
            // Both clustering and rows should match
            if (clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING || other.clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
                return 0;

            return comparator.compare(this.clustering, other.clustering);
        }
        else
        {
            return cmp;
        }
    }

    public static RowKeyComparator COMPARATOR = new RowKeyComparator();

    private static class RowKeyComparator implements Comparator<RowKey>
    {
        public int compare(RowKey o1, RowKey o2)
        {
            return o1.compareTo(o2);
        }
    }

}
