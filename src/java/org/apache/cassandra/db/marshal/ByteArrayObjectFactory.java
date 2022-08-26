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

package org.apache.cassandra.db.marshal;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.ArrayCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

class ByteArrayObjectFactory implements ValueAccessor.ObjectFactory<byte[]>
{
    private static final Clustering<byte[]> EMPTY_CLUSTERING = new ArrayClustering(AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY)
    {
        public String toString(TableMetadata metadata)
        {
            return "EMPTY";
        }
    };

    public static final Clustering<byte[]> STATIC_CLUSTERING = new ArrayClustering(AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY)
    {
        @Override
        public Kind kind()
        {
            return Kind.STATIC_CLUSTERING;
        }

        @Override
        public String toString()
        {
            return "STATIC";
        }

        @Override
        public String toString(TableMetadata metadata)
        {
            return toString();
        }
    };

    static final ValueAccessor.ObjectFactory<byte[]> instance = new ByteArrayObjectFactory();

    private ByteArrayObjectFactory() {}

    /** The smallest start bound, i.e. the one that starts before any row. */
    private static final ArrayClusteringBound BOTTOM_BOUND = new ArrayClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND,
                                                                                      AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY);
    /** The biggest end bound, i.e. the one that ends after any row. */
    private static final ArrayClusteringBound TOP_BOUND = new ArrayClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND,
                                                                                   AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY);

    /** The biggest start bound, i.e. the one that starts after any row. */
    private static final ArrayClusteringBound MAX_START_BOUND = new ArrayClusteringBound(ClusteringPrefix.Kind.EXCL_START_BOUND,
                                                                                      AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY);
    /** The smallest end bound, i.e. the one that end before any row. */
    private static final ArrayClusteringBound MIN_END_BOUND = new ArrayClusteringBound(ClusteringPrefix.Kind.EXCL_END_BOUND,
                                                                                   AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY);

    public Cell<byte[]> cell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, byte[] value, CellPath path)
    {
        return new ArrayCell(column, timestamp, ttl, localDeletionTime, value, path);
    }

    public Clustering<byte[]> clustering(byte[]... values)
    {
        return new ArrayClustering(values);
    }

    public Clustering<byte[]> clustering()
    {
        return EMPTY_CLUSTERING;
    }

    public Clustering<byte[]> staticClustering()
    {
        return STATIC_CLUSTERING;
    }

    public ClusteringBound<byte[]> bound(ClusteringPrefix.Kind kind, byte[]... values)
    {
        return new ArrayClusteringBound(kind, values);
    }

    public ClusteringBound<byte[]> bound(ClusteringPrefix.Kind kind)
    {
        switch (kind)
        {
            case EXCL_END_BOUND: return MIN_END_BOUND;
            case INCL_START_BOUND: return BOTTOM_BOUND;
            case INCL_END_BOUND: return TOP_BOUND;
            case EXCL_START_BOUND: return MAX_START_BOUND;
            default:
                throw new AssertionError(String.format("Unexpected kind %s for empty bound or boundary", kind));
        }
    }

    public ClusteringBoundary<byte[]> boundary(ClusteringPrefix.Kind kind, byte[]... values)
    {
        return new ArrayClusteringBoundary(kind, values);
    }
}