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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;

public class KeyFetcher
{
    public static final long NO_OFFSET = -1;

    private final SSTableReader sstable;

    public KeyFetcher(SSTableReader sstable)
    {
        this.sstable = sstable;
    }

    public RandomAccessReader createReader()
    {
        return sstable.openKeyComponentReader();
    }

    public DecoratedKey apply(RandomAccessReader reader, long keyOffset)
    {
        assert reader != null : "RandomAccessReader null";

        // If the returned offset is the sentinel value, we've seen this offset
        // before or we've run out of valid keys due to ZCS:
        if (keyOffset == NO_OFFSET)
            return null;

        try
        {
            // can return null
            return sstable.keyAt(reader, keyOffset);
        }
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("sstable", sstable).toString();
    }

    @Override
    public int hashCode()
    {
        return sstable.descriptor.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (other == this)
        {
            return true;
        }
        if (other.getClass() != getClass())
        {
            return false;
        }
        KeyFetcher rhs = (KeyFetcher) other;
        return sstable.descriptor.equals(rhs.sstable.descriptor);
    }
}
