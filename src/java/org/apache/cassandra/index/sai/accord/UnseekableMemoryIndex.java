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

package org.apache.cassandra.index.sai.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Unseekable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class UnseekableMemoryIndex extends MemoryIndex
{
    protected UnseekableMemoryIndex(StorageAttachedIndex index)
    {
        super(index);
    }

    @Override
    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        Route<?> route;
        try
        {
            route = AccordKeyspace.deserializeRouteOrNull(value);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        if (!isSupported(route.domain()))
            return 0;

        var pk = index.keyFactory().create(key);
        long sum = 0;
        for (var keyOrRange : route)
            sum += add(pk, keyOrRange);

        return sum;
    }

    protected abstract boolean isSupported(Routable.Domain domain);

    protected abstract long add(PrimaryKey pk, Unseekable keyOrRange);

    @Override
    public long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        throw new UnsupportedOperationException();
    }
}
