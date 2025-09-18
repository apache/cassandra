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

import org.apache.cassandra.db.commitlog.CommitLogPosition;

// TODO: make this available in the accord table as an ID
public class RecordPointer extends CommitLogPosition
{
    public final int length;     // full size of the record
    public final long writtenAt; // only set for periodic mode

    public RecordPointer(long segment, int position, int length)
    {
        this(segment, position, length, 0);
    }

    public RecordPointer(long segment, int position, int length, long writtenAt)
    {
        super(segment, position);
        this.length = length;
        this.writtenAt = writtenAt;
    }

    public RecordPointer(RecordPointer pointer)
    {
        this(pointer.segmentId, pointer.position, pointer.length, pointer.writtenAt);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof RecordPointer))
            return false;
        RecordPointer that = (RecordPointer) other;
        return this.segmentId == that.segmentId
               && this.position == that.position;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(segmentId) + position * 31;
    }

    @Override
    public String toString()
    {
        return "(" + segmentId + ", " + position + ')';
    }
}