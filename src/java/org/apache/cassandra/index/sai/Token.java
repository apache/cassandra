/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.memory.InMemoryToken;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.MergeIterator;

/**
 * A container that exposes an iterator of {@link DecoratedKey} from disk. It exists primarily
 * because multiple partition keys may hash to the same token.
 */
@NotThreadSafe
public abstract class Token implements Comparable<Token>
{
    protected long token;

    public Token(long token)
    {
        this.token = token;
    }

    /**
     * Using Long instead long, because {@link RangeIterator} is based on Long and uses null to represent non-existing min/max.
     */
    public Long get()
    {
        return token;
    }

    @VisibleForTesting
    public long getLong()
    {
        return token;
    }

    public abstract Iterator<DecoratedKey> keys();

    @Override
    public int compareTo(Token o)
    {
        return Long.compare(token, o.token);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("token", token).toString();
    }

    /**
     * This interface makes it possible for us to inject custom tokens and mergers in our tests.
     */
    public interface TokenMerger
    {
        void add(Token other);

        Token merge();

        default void reset() {}
    }

    @NotThreadSafe
    public static class ReusableTokenMerger implements TokenMerger
    {
        private final MergeIterator.Reducer<DecoratedKey, DecoratedKey> reducer = MergeIterator.getIdentity();
        private final List<Iterator<DecoratedKey>> keyIterators;
        private final List<Token> tokens;

        private Token firstToken;

        public ReusableTokenMerger(int capacity)
        {
            keyIterators = new ArrayList<>(capacity);
            tokens = new ArrayList<>(capacity);
        }

        @Override
        public void add(Token token)
        {
            if (token == null) return;

            if (tokens.isEmpty())
            {
                firstToken = token;
            }
            else
            {
                assert firstToken.token == token.token : "Adding keys with a different token!";
            }

            tokens.add(token);
        }

        @Override
        public Token merge()
        {
            assert firstToken != null : "No tokens have been added to this merger!";

            if (tokens.size() == 1) return firstToken;

            // We don't materialize keys until we know a merge is necessary.
            for (Token token : tokens)
            {
                keyIterators.add(token.keys());
            }

            return new InMemoryToken(firstToken.token, MergeIterator.get(keyIterators, DecoratedKey.comparator, reducer));
        }

        /**
         * Clears the state of the merger, preparing it to consume a new group of {@link Token}s.
         */
        public void reset()
        {
            keyIterators.clear();
            tokens.clear();
            firstToken = null;
            reducer.onKeyChange();
        }
    }
}
