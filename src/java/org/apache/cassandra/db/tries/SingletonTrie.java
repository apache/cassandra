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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Singleton trie, mapping the given key to value.
 */
class SingletonTrie<T> extends Trie<T>
{
    private final ByteComparable key;
    private final ByteComparable.Version byteComparableVersion;
    private final T value;

    SingletonTrie(ByteComparable key, ByteComparable.Version byteComparableVersion, T value)
    {
        this.byteComparableVersion = byteComparableVersion;
        this.key = key;
        this.value = value;
    }

    public Cursor cursor(Direction direction)
    {
        return new Cursor(direction);
    }

    class Cursor implements Trie.Cursor<T>
    {
        private final Direction direction;
        private ByteSource src = key.asComparableBytes(byteComparableVersion);
        private int currentDepth = 0;
        private int currentTransition = -1;
        private int nextTransition = src.next();

        public Cursor(Direction direction)
        {
            this.direction = direction;
        }

        @Override
        public int advance()
        {
            currentTransition = nextTransition;
            if (currentTransition != ByteSource.END_OF_STREAM)
            {
                nextTransition = src.next();
                return ++currentDepth;
            }
            else
            {
                return currentDepth = -1;
            }
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (nextTransition == ByteSource.END_OF_STREAM)
                return currentDepth = -1;
            int current = nextTransition;
            int depth = currentDepth;
            int next = src.next();
            while (next != ByteSource.END_OF_STREAM)
            {
                if (receiver != null)
                    receiver.addPathByte(current);
                current = next;
                next = src.next();
                ++depth;
            }
            currentTransition = current;
            nextTransition = next;
            return currentDepth = ++depth;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (skipDepth <= currentDepth)
            {
                assert skipDepth < currentDepth || direction.gt(skipTransition, currentTransition);
                return currentDepth = -1;  // no alternatives
            }
            if (direction.gt(skipTransition, nextTransition))
                return currentDepth = -1;   // request is skipping over our path

            return advance();
        }

        @Override
        public int depth()
        {
            return currentDepth;
        }

        @Override
        public T content()
        {
            return nextTransition == ByteSource.END_OF_STREAM ? value : null;
        }

        @Override
        public int incomingTransition()
        {
            return currentTransition;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return byteComparableVersion;
        }

        @Override
        public Trie<T> tailTrie()
        {
            if (!(src instanceof ByteSource.Duplicatable))
                src = ByteSource.duplicatable(src);
            ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) src;

            return new SingletonTrie(v -> duplicatableSource.duplicate(), byteComparableVersion, value);
        }
    }
}
