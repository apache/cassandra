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
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.text.ParseException;
import java.util.Comparator;

import com.google.common.base.Strings;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Contains a segment id and a position for CommitLogSegment identification.
 * Used for both replay and general CommitLog file reading.
 */
public class CommitLogPosition implements Comparable<CommitLogPosition>
{
    public static final CommitLogPositionSerializer serializer = new CommitLogPositionSerializer();

    // NONE is used for SSTables that are streamed from other nodes and thus have no relationship
    // with our local commitlog. The values satisfy the criteria that
    //  - no real commitlog segment will have the given id
    //  - it will sort before any real CommitLogPosition, so it will be effectively ignored by getCommitLogPosition
    public static final CommitLogPosition NONE = new CommitLogPosition(-1, 0);

    public final long segmentId;
    // Indicates the end position of the mutation in the CommitLog
    public final int position;

    public static final Comparator<CommitLogPosition> comparator = new Comparator<CommitLogPosition>()
    {
        public int compare(CommitLogPosition o1, CommitLogPosition o2)
        {
            if (o1.segmentId != o2.segmentId)
            	return Long.compare(o1.segmentId,  o2.segmentId);

            return Integer.compare(o1.position, o2.position);
        }
    };

    public CommitLogPosition(long segmentId, int position)
    {
        this.segmentId = segmentId;
        assert position >= 0;
        this.position = position;
    }

    public int compareTo(CommitLogPosition other)
    {
        return comparator.compare(this, other);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommitLogPosition that = (CommitLogPosition) o;

        if (position != that.position) return false;
        return segmentId == that.segmentId;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (segmentId ^ (segmentId >>> 32));
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString()
    {
        return "CommitLogPosition(" +
               "segmentId=" + segmentId +
               ", position=" + position +
               ')';
    }

    public CommitLogPosition clone()
    {
        return new CommitLogPosition(segmentId, position);
    }


    public static class CommitLogPositionSerializer implements ISerializer<CommitLogPosition>
    {
        public void serialize(CommitLogPosition clsp, DataOutputPlus out) throws IOException
        {
            out.writeLong(clsp.segmentId);
            out.writeInt(clsp.position);
        }

        public CommitLogPosition deserialize(DataInputPlus in) throws IOException
        {
            return new CommitLogPosition(in.readLong(), in.readInt());
        }

        public long serializedSize(CommitLogPosition clsp)
        {
            return TypeSizes.sizeof(clsp.segmentId) + TypeSizes.sizeof(clsp.position);
        }

        public CommitLogPosition fromString(String position) throws ParseException
        {
            if (Strings.isNullOrEmpty(position))
                return NONE;
            String[] parts = position.split(",");
            if (parts.length != 2)
                throw new ParseException("Commit log position must be given as <segment>,<position>", 0);
            return new CommitLogPosition(Long.parseLong(parts[0].trim()), Integer.parseInt(parts[1].trim()));
        }

        public String toString(CommitLogPosition position)
        {
            return position == NONE ? "" : String.format("%d, %d", position.segmentId, position.position);
        }
    }
}
