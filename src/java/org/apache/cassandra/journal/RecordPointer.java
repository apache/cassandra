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

package org.apache.cassandra.journal;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

// TODO: make this available in the accord table as an ID
public class RecordPointer implements Comparable<RecordPointer>
{
    public final long segment; // unique segment id
    public final int position; // record start position within the segment
    public final long writtenAt; // only set for periodic mode

    public RecordPointer(long segment, int position)
    {
        this(segment, position, 0);
    }

    public RecordPointer(long segment, int position, long writtenAt)
    {
        this.segment = segment;
        this.position = position;
        this.writtenAt = writtenAt;
    }

    public RecordPointer(RecordPointer pointer)
    {
        this(pointer.segment, pointer.position, pointer.writtenAt);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof RecordPointer))
            return false;
        RecordPointer that = (RecordPointer) other;
        return this.segment == that.segment
               && this.position == that.position;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(segment) + position * 31;
    }

    @Override
    public String toString()
    {
        return "(" + segment + ", " + position + ')';
    }

    @Override
    public int compareTo(RecordPointer that)
    {
        int cmp = Longs.compare(this.segment, that.segment);
        return cmp != 0 ? cmp : Ints.compare(this.position, that.position);
    }
}