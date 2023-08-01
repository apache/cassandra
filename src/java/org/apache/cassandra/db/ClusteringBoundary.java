/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db;

/**
 * The threshold between two different ranges, i.e. a shortcut for the combination of two ClusteringBounds -- one
 * specifying the end of one of the ranges, and its (implicit) complement specifying the beginning of the other.
 */
public interface ClusteringBoundary<V> extends ClusteringBoundOrBoundary<V>
{
    @Override
    public ClusteringBoundary<V> invert();

    public ClusteringBound<V> openBound(boolean reversed);

    public ClusteringBound<V> closeBound(boolean reversed);

    public static <V> ClusteringBoundary<V> create(ClusteringBound.Kind kind, ClusteringPrefix<V> from)
    {
        return from.accessor().factory().boundary(kind, from.getRawValues());
    }

    @Override
    default ClusteringBound<V> asStartBound()
    {
        return openBound(false);
    }

    @Override
    default ClusteringBound<V> asEndBound()
    {
        return closeBound(false);
    }
}