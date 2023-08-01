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

package org.apache.cassandra.io.sstable;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

public class EmptySSTableScanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner
{
    private final SSTableReader sstable;

    public EmptySSTableScanner(SSTableReader sstable)
    {
        this.sstable = sstable;
    }

    public long getBytesScanned()
    {
        return 0;
    }

    public long getLengthInBytes()
    {
        return 0;
    }

    public long getCompressedLengthInBytes()
    {
        return 0;
    }

    public Set<SSTableReader> getBackingSSTables()
    {
        return ImmutableSet.of(sstable);
    }

    public long getCurrentPosition()
    {
        return 0;
    }

    public TableMetadata metadata()
    {
        return sstable.metadata();
    }

    public boolean hasNext()
    {
        return false;
    }

    public UnfilteredRowIterator next()
    {
        return null;
    }
}