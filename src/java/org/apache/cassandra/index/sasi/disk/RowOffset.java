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

package org.apache.cassandra.index.sasi.disk;

import java.nio.ByteBuffer;

public class RowOffset
{
    public final long partitionOffset;
    public final long rowOffset;

    public RowOffset(long partitionOffset, long rowOffset)
    {
        this.partitionOffset = partitionOffset;
        this.rowOffset = rowOffset;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowOffset rowOffset1 = (RowOffset) o;

        if (partitionOffset != rowOffset1.partitionOffset) return false;
        return rowOffset == rowOffset1.rowOffset;
    }

    public int hashCode()
    {
        int result = (int) (partitionOffset ^ (partitionOffset >>> 32));
        result = 31 * result + (int) (rowOffset ^ (rowOffset >>> 32));
        return result;
    }

    public void serialize(ByteBuffer buffer)
    {
        buffer.putLong(partitionOffset);
        buffer.putLong(rowOffset);
    }

    public String toString()
    {
        return "RowOffset{" +
               "partitionOffset=" + partitionOffset +
               ", rowOffset=" + rowOffset +
               '}';
    }
}

