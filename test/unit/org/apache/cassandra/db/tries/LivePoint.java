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

import java.util.Collection;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

class LivePoint implements DataPoint
{
    final ByteComparable position;
    final int timestamp;

    public LivePoint(ByteComparable position, int timestamp)
    {
        this.position = position;
        this.timestamp = timestamp;
    }

    public LivePoint delete(int timestamp)
    {
        return this.timestamp < timestamp ? null : this;
    }

    @Override
    public DeletionMarker marker()
    {
        return null;
    }

    @Override
    public LivePoint live()
    {
        return this;
    }

    @Override
    public ByteComparable position()
    {
        return position;
    }

    @Override
    public DataPoint withMarker(DeletionMarker newMarker)
    {
        if (newMarker == null)
            return this;
        else
            return new CombinedDataPoint(this, newMarker);
    }

    @Override
    public LivePoint remap(ByteComparable newKey)
    {
        return new LivePoint(newKey, timestamp);
    }

    @Override
    public DataPoint toContent()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return '{' + DataPoint.toString(position) + "}L" + timestamp;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LivePoint livePoint = (LivePoint) o;
        return timestamp == livePoint.timestamp
               && ByteComparable.compare(this.position, livePoint.position, TrieUtil.VERSION) == 0;
    }

    static LivePoint combine(LivePoint a, LivePoint b)
    {
        return a.timestamp >= b.timestamp ? a : b;
    }

    static LivePoint combineCollection(Collection<LivePoint> values)
    {
        return values.stream().reduce(LivePoint::combine).orElseThrow();
    }
}
