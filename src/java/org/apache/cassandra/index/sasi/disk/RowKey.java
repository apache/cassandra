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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.*;

/**
 * Primary key of the found row, a combination of the Partition Key
 * and clustering that belongs to the row.
 */
public class RowKey implements Comparable<RowKey>
{

    public DecoratedKey decoratedKey;
    public ClusteringPrefix clustering;

    public RowKey(DecoratedKey primaryKey, ClusteringPrefix clustering)
    {
        this.decoratedKey = primaryKey;
        this.clustering = clustering;
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

    public int compareTo(RowKey o2)
    {
        RowKey o1 = this;
        int cmp = o1.decoratedKey.compareTo(o2.decoratedKey);
        if (cmp == 0 && o1.clustering != null && o2.clustering != null && o1.clustering.kind() == ClusteringPrefix.Kind.CLUSTERING && o2.clustering.kind() == ClusteringPrefix.Kind.CLUSTERING)
        {
            ByteBuffer[] bbs1 = o1.clustering.getRawValues();
            ByteBuffer[] bbs2 = o2.clustering.getRawValues();

            for (int i = 0; i < bbs1.length; i++)
            {
                int cmpc = ByteBufferUtil.compareUnsigned(bbs1[i], bbs2[i]);
                if (cmpc != 0)
                {
                    return cmpc;
                }
            }
        }
        else
        {
            return cmp;
        }
        return 0;
    }
}
