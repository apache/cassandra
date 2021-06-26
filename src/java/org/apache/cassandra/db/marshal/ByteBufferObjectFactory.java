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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferClusteringBoundary;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

class ByteBufferObjectFactory implements ValueAccessor.ObjectFactory<ByteBuffer>
{
    /** Empty clustering for tables having no clustering columns. */
    private static final Clustering<ByteBuffer> EMPTY_CLUSTERING = new BufferClustering()
    {
        @Override
        public String toString(TableMetadata metadata)
        {
            return "EMPTY";
        }
    };

    /** The smallest start bound, i.e. the one that starts before any row. */
    private static final BufferClusteringBound BOTTOM_BOUND = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, new ByteBuffer[0]);
    /** The biggest end bound, i.e. the one that ends after any row. */
    private static final BufferClusteringBound TOP_BOUND = new BufferClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, new ByteBuffer[0]);

    static final ValueAccessor.ObjectFactory<ByteBuffer> instance = new ByteBufferObjectFactory();

    private ByteBufferObjectFactory() {}

    public Cell<ByteBuffer> cell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, ByteBuffer value, CellPath path)
    {
        return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
    }

    public Clustering<ByteBuffer> clustering(ByteBuffer... values)
    {
        return new BufferClustering(values);
    }

    public Clustering<ByteBuffer> clustering()
    {
        return EMPTY_CLUSTERING;
    }

    public ClusteringBound<ByteBuffer> bound(ClusteringPrefix.Kind kind, ByteBuffer... values)
    {
        return new BufferClusteringBound(kind, values);
    }

    public ClusteringBound<ByteBuffer> bound(ClusteringPrefix.Kind kind)
    {
        return kind.isStart() ? BOTTOM_BOUND : TOP_BOUND;
    }

    public ClusteringBoundary<ByteBuffer> boundary(ClusteringPrefix.Kind kind, ByteBuffer... values)
    {
        return new BufferClusteringBoundary(kind, values);
    }
}
