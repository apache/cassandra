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

package org.apache.cassandra.db.tries;

import java.util.Objects;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

class CombinedDataPoint implements DataPoint
{
    final LivePoint livePoint;
    final DeletionMarker marker;

    public CombinedDataPoint(LivePoint livePoint, DeletionMarker marker)
    {
        this.livePoint = livePoint;
        this.marker = marker;
    }

    @Override
    public DeletionMarker marker()
    {
        return marker;
    }

    @Override
    public LivePoint live()
    {
        return livePoint;
    }

    @Override
    public ByteComparable position()
    {
        return livePoint.position();
    }

    @Override
    public DataPoint withMarker(DeletionMarker newMarker)
    {
        if (newMarker == null)
            return livePoint;
        else
            return new CombinedDataPoint(livePoint, newMarker);
    }

    @Override
    public DataPoint remap(ByteComparable newKey)
    {
        return new CombinedDataPoint(livePoint.remap(newKey), marker.remap(newKey));
    }

    @Override
    public String toString()
    {
        return marker.toString() + 'L' + livePoint.timestamp;
    }

    public DataPoint toContent()
    {
        if (marker.isBoundary)
            return this;
        return livePoint;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CombinedDataPoint that = (CombinedDataPoint) o;
        return Objects.equals(livePoint, that.livePoint) && Objects.equals(marker, that.marker);
    }
}
