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

package org.apache.cassandra.service.writes.thresholds;

import java.util.Objects;

/**
 * Immutable snapshot of write warnings. Simpler than WarningsSnapshot since writes never abort (warnings only).
 */
public class WriteWarningsSnapshot
{
    private static final WriteWarningsSnapshot EMPTY = new WriteWarningsSnapshot(WriteThresholdCounter.empty(), WriteThresholdCounter.empty());

    public final WriteThresholdCounter writeSize;
    public final WriteThresholdCounter writeTombstone;

    private WriteWarningsSnapshot(WriteThresholdCounter writeSize, WriteThresholdCounter writeTombstone)
    {
        this.writeSize = writeSize;
        this.writeTombstone = writeTombstone;
    }

    public static WriteWarningsSnapshot empty()
    {
        return EMPTY;
    }

    public static WriteWarningsSnapshot create(WriteThresholdCounter writeSize, WriteThresholdCounter writeTombstone)
    {
        if (writeSize.isEmpty() && writeTombstone.isEmpty())
            return EMPTY;
        return new WriteWarningsSnapshot(writeSize, writeTombstone);
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    public WriteWarningsSnapshot merge(WriteWarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        if (this == EMPTY)
            return other;
        return WriteWarningsSnapshot.create(writeSize.merge(other.writeSize), writeTombstone.merge(other.writeTombstone));
    }

    public static String writeSizeWarnMessage(long bytes)
    {
        return String.format("Write to large partition; estimated size is %d bytes (see write_size_warn_threshold)", bytes);
    }

    public static String writeTombstoneWarnMessage(long tombstones)
    {
        return String.format("Write to partition with many tombstones; estimated count is %d (see write_tombstone_warn_threshold)", tombstones);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WriteWarningsSnapshot that = (WriteWarningsSnapshot) o;
        return Objects.equals(writeSize, that.writeSize) && Objects.equals(writeTombstone, that.writeTombstone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(writeSize, writeTombstone);
    }

    @Override
    public String toString()
    {
        return "WriteWarningsSnapshot{" +
               "writeSize=" + writeSize +
               ", writeTombstone=" + writeTombstone +
               '}';
    }
}
