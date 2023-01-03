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
package org.apache.cassandra.io.sstable.format.bti;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TailOverridingRebufferer;

/**
 * Early-opened partition index. Part of the data is already written to file, but some nodes, including the ones in the
 * chain leading to the last entry in the index, are in the supplied byte buffer and are attached as a tail at the given
 * position to form a view over the partially-written data.
 */
class PartitionIndexEarly extends PartitionIndex
{
    final long cutoff;
    final ByteBuffer tail;

    public PartitionIndexEarly(FileHandle fh, long trieRoot, long keyCount, DecoratedKey first, DecoratedKey last,
                               long cutoff, ByteBuffer tail)
    {
        super(fh, trieRoot, keyCount, first, last);
        this.cutoff = cutoff;
        this.tail = tail;
    }

    @Override
    protected Rebufferer instantiateRebufferer()
    {
        return new TailOverridingRebufferer(super.instantiateRebufferer(), cutoff, tail);
    }
}
