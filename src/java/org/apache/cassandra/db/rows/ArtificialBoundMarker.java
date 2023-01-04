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

import java.util.Objects;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.schema.TableMetadata;

public class ArtificialBoundMarker extends RangeTombstoneBoundMarker
{
    public ArtificialBoundMarker(ClusteringBound<?> bound)
    {
        super(bound, DeletionTime.LIVE);
        assert bound.isArtificial();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if (!(other instanceof ArtificialBoundMarker))
            return false;

        ArtificialBoundMarker that = (ArtificialBoundMarker) other;
        return Objects.equals(bound, that.bound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bound);
    }

    @Override
    public String toString(TableMetadata metadata)
    {
        return String.format("LowerBoundMarker %s", bound.toString(metadata));
    }
}