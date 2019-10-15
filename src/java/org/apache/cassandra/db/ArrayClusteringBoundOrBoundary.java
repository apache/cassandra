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

package org.apache.cassandra.db;

public abstract class ArrayClusteringBoundOrBoundary extends AbstractArrayClusteringPrefix implements ClusteringBoundOrBoundary<byte[]>
{
    public ArrayClusteringBoundOrBoundary(Kind kind, byte[][] values)
    {
        super(kind, values);
    }
    public static ClusteringBoundOrBoundary<byte[]> create(Kind kind, byte[][] values)
    {
        return kind.isBoundary()
               ? new ArrayClusteringBoundary(kind, values)
               : new ArrayClusteringBound(kind, values);
    }

    public static ClusteringBound<byte[]> inclusiveOpen(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBound(reversed ? ClusteringPrefix.Kind.INCL_END_BOUND : ClusteringPrefix.Kind.INCL_START_BOUND, boundValues);
    }

    public static ClusteringBound<byte[]> exclusiveOpen(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBound(reversed ? ClusteringPrefix.Kind.EXCL_END_BOUND : ClusteringPrefix.Kind.EXCL_START_BOUND, boundValues);
    }

    public static ClusteringBound<byte[]> inclusiveClose(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBound(reversed ? ClusteringPrefix.Kind.INCL_START_BOUND : ClusteringPrefix.Kind.INCL_END_BOUND, boundValues);
    }

    public static ClusteringBound<byte[]> exclusiveClose(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBound(reversed ? ClusteringPrefix.Kind.EXCL_START_BOUND : ClusteringPrefix.Kind.EXCL_END_BOUND, boundValues);
    }

    public static ClusteringBoundary<byte[]> inclusiveCloseExclusiveOpen(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBoundary(reversed ? ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY : ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
    }

    public static ClusteringBoundary<byte[]> exclusiveCloseInclusiveOpen(boolean reversed, byte[][] boundValues)
    {
        return new ArrayClusteringBoundary(reversed ? ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY : ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
    }

    public ClusteringPrefix<byte[]> minimize()
    {
        return this;
    }
}
