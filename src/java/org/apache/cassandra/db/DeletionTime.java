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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;

import org.apache.cassandra.io.ISerializer;

public class DeletionTime implements Comparable<DeletionTime>
{
    public static final DeletionTime LIVE = new DeletionTime(Long.MIN_VALUE, Integer.MAX_VALUE);

    public final long markedForDeleteAt;
    public final int localDeletionTime;

    public static final ISerializer<DeletionTime> serializer = new Serializer();

    DeletionTime(long markedForDeleteAt, int localDeletionTime)
    {
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionTime))
            return false;
        DeletionTime that = (DeletionTime)o;
        return markedForDeleteAt == that.markedForDeleteAt && localDeletionTime == that.localDeletionTime;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(markedForDeleteAt, localDeletionTime);
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt, localDeletionTime);
    }

    public int compareTo(DeletionTime dt)
    {
        if (markedForDeleteAt < dt.markedForDeleteAt)
            return -1;
        else if (markedForDeleteAt > dt.markedForDeleteAt)
            return 1;
        else if (localDeletionTime < dt.localDeletionTime)
            return -1;
        else if (localDeletionTime > dt.localDeletionTime)
            return -1;
        else
            return 0;
    }

    public boolean isGcAble(int gcBefore)
    {
        return localDeletionTime < gcBefore;
    }

    public boolean isDeleted(IColumn column)
    {
        return column.isMarkedForDelete() && column.getMarkedForDeleteAt() <= markedForDeleteAt;
    }

    private static class Serializer implements ISerializer<DeletionTime>
    {
        public void serialize(DeletionTime delTime, DataOutput out) throws IOException
        {
            out.writeInt(delTime.localDeletionTime);
            out.writeLong(delTime.markedForDeleteAt);
        }

        public DeletionTime deserialize(DataInput in) throws IOException
        {
            int ldt = in.readInt();
            long mfda = in.readLong();
            if (mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE)
                return LIVE;
            else
                return new DeletionTime(mfda, ldt);
        }

        public long serializedSize(DeletionTime delTime, TypeSizes typeSizes)
        {
            return typeSizes.sizeof(delTime.localDeletionTime)
                 + typeSizes.sizeof(delTime.markedForDeleteAt);
        }
    }
}
