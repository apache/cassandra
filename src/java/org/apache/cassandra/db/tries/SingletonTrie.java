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
    private final T value;

    SingletonTrie(ByteComparable key, T value)
    {
        this.key = key;
        this.value = value;
    }

    public Cursor cursor()
    {
        return new Cursor();
    }

    class Cursor implements Trie.Cursor<T>
    {
        ByteSource src = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        int currentDepth = 0;
        int currentTransition = -1;
        int nextTransition = src.next();

        public int advance()
        {
            currentTransition = nextTransition;
            if (currentTransition != ByteSource.END_OF_STREAM)
            {
                nextTransition = src.next();
                return ++currentDepth;
            }
            else
                return currentDepth = -1;
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

        public int skipChildren()
        {
            return currentDepth = -1;  // no alternatives
        }

        public int depth()
        {
            return currentDepth;
        }

        public T content()
        {
            return nextTransition == ByteSource.END_OF_STREAM ? value : null;
        }

        public int incomingTransition()
        {
            return currentTransition;
        }
    }
}
