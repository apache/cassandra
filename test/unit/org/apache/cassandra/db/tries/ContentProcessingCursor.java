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

package org.apache.cassandra.db.tries;

import java.util.function.Function;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class ContentProcessingCursor<T, V> implements Cursor<V>
{
    final Function<T, V> processor;
    final Cursor<T> source;

    public ContentProcessingCursor(Function<T, V> processor, Cursor<T> source)
    {
        this.processor = processor;
        this.source = source;
    }


    @Override
    public V content()
    {
        T sourceContent = source.content();
        if (sourceContent == null)
            return null;
        return processor.apply(sourceContent); // this can become null
    }


    @Override
    public int depth()
    {
        return source.depth();
    }

    @Override
    public int incomingTransition()
    {
        return source.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return source.direction();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    @Override
    public int advance()
    {
        return source.advance();
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        return source.advanceMultiple(receiver);
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        return source.skipTo(skipDepth, skipTransition);
    }

    @Override
    public Cursor<V> tailCursor(Direction direction)
    {
        return new ContentProcessingCursor<>(processor, source.tailCursor(direction));
    }
}
