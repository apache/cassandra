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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;

public abstract class AbstractRangeTombstoneMarker<B extends ClusteringBoundOrBoundary> implements RangeTombstoneMarker
{
    protected final B bound;

    protected AbstractRangeTombstoneMarker(B bound)
    {
        this.bound = bound;
    }

    public B clustering()
    {
        return bound;
    }

    public Unfiltered.Kind kind()
    {
        return Unfiltered.Kind.RANGE_TOMBSTONE_MARKER;
    }

    public boolean isBoundary()
    {
        return bound.isBoundary();
    }

    public boolean isOpen(boolean reversed)
    {
        return bound.isOpen(reversed);
    }

    public boolean isClose(boolean reversed)
    {
        return bound.isClose(reversed);
    }

    public void validateData(TableMetadata metadata)
    {
        ClusteringBoundOrBoundary bound = clustering();
        for (int i = 0; i < bound.size(); i++)
        {
            ByteBuffer value = bound.get(i);
            if (value != null)
                metadata.comparator.subtype(i).validate(value);
        }
    }

    public String toString(TableMetadata metadata, boolean fullDetails)
    {
        return toString(metadata);
    }
    public String toString(TableMetadata metadata, boolean includeClusteringKeys, boolean fullDetails)
    {
        return toString(metadata);
    }
}
