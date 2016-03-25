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
package org.apache.cassandra.index.sasi.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.AbstractType;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.suffix.ConcurrentSuffixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;
import com.googlecode.concurrenttrees.radix.node.Node;
import org.apache.cassandra.utils.FBUtilities;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.index.sasi.memory.SkipListMemIndex.CSLM_OVERHEAD;

public class TrieMemIndex extends MemIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemIndex.class);

    private final ConcurrentTrie index;

    public TrieMemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex)
    {
        super(keyValidator, columnIndex);

        switch (columnIndex.getMode().mode)
        {
            case CONTAINS:
                index = new ConcurrentSuffixTrie(columnIndex.getDefinition());
                break;

            case PREFIX:
                index = new ConcurrentPrefixTrie(columnIndex.getDefinition());
                break;

            default:
                throw new IllegalStateException("Unsupported mode: " + columnIndex.getMode().mode);
        }
    }

    public long add(DecoratedKey key, ByteBuffer value)
    {
        AbstractAnalyzer analyzer = columnIndex.getAnalyzer();
        analyzer.reset(value.duplicate());

        long size = 0;
        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();

            if (term.remaining() >= OnDiskIndexBuilder.MAX_TERM_SIZE)
            {
                logger.info("Can't add term of column {} to index for key: {}, term size {}, max allowed size {}, use analyzed = true (if not yet set) for that column.",
                            columnIndex.getColumnName(),
                            keyValidator.getString(key.getKey()),
                            FBUtilities.prettyPrintMemory(term.remaining()),
                            FBUtilities.prettyPrintMemory(OnDiskIndexBuilder.MAX_TERM_SIZE));
                continue;
            }

            size += index.add(columnIndex.getValidator().getString(term), key);
        }

        return size;
    }

    public RangeIterator<Long, Token> search(Expression expression)
    {
        return index.search(expression);
    }

    private static abstract class ConcurrentTrie
    {
        public static final SizeEstimatingNodeFactory NODE_FACTORY = new SizeEstimatingNodeFactory();

        protected final ColumnDefinition definition;

        public ConcurrentTrie(ColumnDefinition column)
        {
            definition = column;
        }

        public long add(String value, DecoratedKey key)
        {
            long overhead = CSLM_OVERHEAD;
            ConcurrentSkipListSet<DecoratedKey> keys = get(value);
            if (keys == null)
            {
                ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet<>(DecoratedKey.comparator);
                keys = putIfAbsent(value, newKeys);
                if (keys == null)
                {
                    overhead += CSLM_OVERHEAD + value.length();
                    keys = newKeys;
                }
            }

            keys.add(key);

            // get and reset new memory size allocated by current thread
            overhead += NODE_FACTORY.currentUpdateSize();
            NODE_FACTORY.reset();

            return overhead;
        }

        public RangeIterator<Long, Token> search(Expression expression)
        {
            ByteBuffer prefix = expression.lower == null ? null : expression.lower.value;

            Iterable<ConcurrentSkipListSet<DecoratedKey>> search = search(expression.getOp(), definition.cellValueType().getString(prefix));

            RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
            for (ConcurrentSkipListSet<DecoratedKey> keys : search)
            {
                if (!keys.isEmpty())
                    builder.add(new KeyRangeIterator(keys));
            }

            return builder.build();
        }

        protected abstract ConcurrentSkipListSet<DecoratedKey> get(String value);
        protected abstract Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Op operator, String value);
        protected abstract ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> key);
    }

    protected static class ConcurrentPrefixTrie extends ConcurrentTrie
    {
        private final ConcurrentRadixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentPrefixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentRadixTree<>(NODE_FACTORY);
        }

        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Op operator, String value)
        {
            switch (operator)
            {
                case EQ:
                case MATCH:
                    ConcurrentSkipListSet<DecoratedKey> keys = trie.getValueForExactKey(value);
                    return keys == null ? Collections.emptyList() : Collections.singletonList(keys);

                case PREFIX:
                    return trie.getValuesForKeysStartingWith(value);

                default:
                    throw new UnsupportedOperationException(String.format("operation %s is not supported.", operator));
            }
        }
    }

    protected static class ConcurrentSuffixTrie extends ConcurrentTrie
    {
        private final ConcurrentSuffixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentSuffixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentSuffixTree<>(NODE_FACTORY);
        }

        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(Op operator, String value)
        {
            switch (operator)
            {
                case EQ:
                case MATCH:
                    ConcurrentSkipListSet<DecoratedKey> keys = trie.getValueForExactKey(value);
                    return keys == null ? Collections.emptyList() : Collections.singletonList(keys);

                case SUFFIX:
                    return trie.getValuesForKeysEndingWith(value);

                case PREFIX:
                case CONTAINS:
                    return trie.getValuesForKeysContaining(value);

                default:
                    throw new UnsupportedOperationException(String.format("operation %s is not supported.", operator));
            }
        }
    }

    // This relies on the fact that all of the tree updates are done under exclusive write lock,
    // method would overestimate in certain circumstances e.g. when nodes are replaced in place,
    // but it's still better comparing to underestimate since it gives more breathing room for other memory users.
    private static class SizeEstimatingNodeFactory extends SmartArrayBasedNodeFactory
    {
        private final ThreadLocal<Long> updateSize = ThreadLocal.withInitial(() -> 0L);

        public Node createNode(CharSequence edgeCharacters, Object value, List<Node> childNodes, boolean isRoot)
        {
            Node node = super.createNode(edgeCharacters, value, childNodes, isRoot);
            updateSize.set(updateSize.get() + measure(node));
            return node;
        }

        public long currentUpdateSize()
        {
            return updateSize.get();
        }

        public void reset()
        {
            updateSize.set(0L);
        }

        private long measure(Node node)
        {
            // node with max overhead is CharArrayNodeLeafWithValue = 24B
            long overhead = 24;

            // array of chars (2 bytes) + CharSequence overhead
            overhead += 24 + node.getIncomingEdge().length() * 2;

            if (node.getOutgoingEdges() != null)
            {
                // 16 bytes for AtomicReferenceArray
                overhead += 16;
                overhead += 24 * node.getOutgoingEdges().size();
            }

            return overhead;
        }
    }
}
