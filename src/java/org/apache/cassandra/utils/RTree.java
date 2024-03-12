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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.CheckForNull;

import com.google.common.collect.AbstractIterator;

public class RTree<Token, Range, Value> implements RangeTree<Token, Range, Value>
{
    /**
     * Tuning size target can be tricky as it is based on expected access patterns and expected matche sizes.  There is also
     * a memory cost to account for as large tree sizes will have far more nodes with a small target than a large target.
     *
     * If matching most of the data then larger sizes leads to fewer hops
     * If matching few elements then tree depth maters the most, if walking a long tree is more costly than walking the
     * element list, then shrinking depth (by having larger size target) can improve performance.
     */
    private static final int DEFAULT_SIZE_TARGET = 1 << 7;
    private static final int DEFAULT_NUMBER_OF_CHILDREN = 6;

    private final Comparator<Token> comparator;
    private final Accessor<Token, Range> accessor;
    private final int sizeTarget;
    private final int numChildren;
    private Node node = new Node();

    public RTree(Comparator<Token> comparator, Accessor<Token, Range> accessor)
    {
        this(comparator, accessor, DEFAULT_SIZE_TARGET, DEFAULT_NUMBER_OF_CHILDREN);
    }

    public RTree(Comparator<Token> comparator, Accessor<Token, Range> accessor, int sizeTarget, int numChildren)
    {
        if (sizeTarget <= 1)
            throw new IllegalArgumentException("size target must be 2 or more");
        if (numChildren <= 1)
            throw new IllegalArgumentException("Number of children must be 2 or more");
        if (sizeTarget < numChildren)
            throw new IllegalArgumentException("Size target (" + sizeTarget + ") was less than number of children (" + numChildren + ")");
        this.comparator = comparator;
        this.accessor = accessor;
        this.sizeTarget = sizeTarget;
        this.numChildren = numChildren;
    }

    public static <Token extends Comparable<? super Token>, Range, Value> RTree<Token, Range, Value> create(Accessor<Token, Range> accessor)
    {
        return new RTree<>(Comparator.naturalOrder(), accessor);
    }

    @Override
    public List<Value> get(Range range)
    {
        List<Value> matches = new ArrayList<>();
        get(range, e -> matches.add(e.getValue()));
        return matches;
    }

    @Override
    public void get(Range range, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        node.search(range, onMatch, e -> e.getKey().equals(range), Function.identity());
    }

    @Override
    public List<Map.Entry<Range, Value>> search(Range range)
    {
        List<Map.Entry<Range, Value>> matches = new ArrayList<>();
        search(range, matches::add);
        return matches;
    }

    @Override
    public void search(Range range, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        node.search(range, onMatch, ignore -> true, Function.identity());
    }

    public List<Value> find(Range range)
    {
        List<Value> matches = new ArrayList<>();
        find(range, matches::add);
        return matches;
    }

    public void find(Range range, Consumer<Value> onMatch)
    {
        node.search(range, onMatch, ignore -> true, Map.Entry::getValue);
    }

    @Override
    public List<Map.Entry<Range, Value>> searchToken(Token token)
    {
        List<Map.Entry<Range, Value>> matches = new ArrayList<>();
        searchToken(token, matches::add);
        return matches;
    }

    @Override
    public void searchToken(Token token, Consumer<Map.Entry<Range, Value>> onMatch)
    {
        node.searchToken(token, onMatch, ignore -> true, Function.identity());
    }

    public List<Value> findToken(Token token)
    {
        List<Value> matches = new ArrayList<>();
        findToken(token, matches::add);
        return matches;
    }

    public void findToken(Token token, Consumer<Value> onMatch)
    {
        node.searchToken(token, onMatch, ignore -> true, Map.Entry::getValue);
    }

    @Override
    public boolean add(Range key, Value value)
    {
        node.add(key, value);
        return true;
    }

    @Override
    public int remove(Range key)
    {
        return node.removeIf(e -> e.getKey().equals(key));
    }

    public int remove(Range key, Value value)
    {
        var match = Map.entry(key, value);
        return node.removeIf(match::equals);
    }

    @Override
    public void clear()
    {
        node = new Node();
    }

    @Override
    public int size()
    {
        return node.size;
    }

    @Override
    public boolean isEmpty()
    {
        return node.size == 0;
    }

    public String displayTree()
    {
        StringBuilder sb = new StringBuilder();
        node.displayTree(0, sb);
        return sb.toString();
    }

    @Override
    public Iterator<Map.Entry<Range, Value>> iterator()
    {
        return node.iterator();
    }

    @Override
    public Stream<Map.Entry<Range, Value>> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    private class Node implements Iterable<Map.Entry<Range, Value>>
    {
        private List<Map.Entry<Range, Value>> values = new ArrayList<>();
        private List<Node> children = null;
        private int size = 0;
        private Token minStart, maxStart, minEnd, maxEnd;

        int removeIf(Predicate<Map.Entry<Range, Value>> condition)
        {
            if (minStart == null)
                return 0;
            if (children != null)
            {
                int sum = 0;
                for (Node node : children)
                    sum += node.removeIf(condition);
                size -= sum;
                return sum;
            }
            class Counter {int value;}
            Counter counter = new Counter();
            values.removeIf(e -> {
                if (condition.test(e))
                {
                    counter.value++;
                    return true;
                }
                return false;
            });
            size -= counter.value;
            if (values.isEmpty())
                minStart = maxStart = minEnd = maxEnd = null;
            return counter.value;
        }

        void add(Range range, Value value)
        {
            size++;
            if (minStart == null)
            {
                minStart = maxStart = accessor.start(range);
                minEnd = maxEnd = accessor.end(range);
            }
            else
            {
                Token start = accessor.start(range);
                minStart = min(minStart, start);
                maxStart = max(maxStart, start);
                Token end = accessor.end(range);
                minEnd = min(minEnd, end);
                maxEnd = max(maxEnd, end);
            }
            if (children != null)
            {
                findBestMatch(range).add(range, value);
                return;
            }
            values.add(new MutableEntry(range, value));
            if (shouldSplit())
                split();
        }

        private Node findBestMatch(Range range)
        {
            int topIdx = 0;
            Node node = children.get(0);
            int topScore = node.score(range);
            int size = node.size;
            for (int i = 1; i < children.size(); i++)
            {
                node = children.get(i);
                int score = node.score(range);
                if (score > topScore || (score == topScore && size > node.size))
                {
                    topIdx = i;
                    size = node.size;
                }
            }
            return children.get(topIdx);
        }

        private int score(Range range)
        {
            if (minStart == null)
                return 0;
            if (!intersects(range))
                return -10;
            int score = 5; // overlapps
            if (values != null) // is leaf
                score += 5;

            int startScore = 0;
            if (comparator.compare(maxStart, accessor.start(range)) <= 0)
                startScore += 10;
            else if (comparator.compare(minStart, accessor.start(range)) <= 0)
                startScore += 5;

            int endScore = 0;
            if (comparator.compare(minEnd, accessor.end(range)) >= 0)
                endScore += 10;
            else if (comparator.compare(maxEnd, accessor.end(range)) >= 0)
                endScore += 5;
            // if fully contained, then add the scores: 10 for largest bounds, 20 for smallest bounds
            if (!(startScore == 0 || endScore == 0))
                score += startScore + endScore;
            return score;
        }

        boolean shouldSplit()
        {
            return values.size() > sizeTarget
                   // if the same range is used over and over again, splitting doesn't do much
                   && !(comparator.compare(minStart, maxStart) == 0
                        && comparator.compare(minEnd, maxEnd) == 0);
        }

        List<List<Map.Entry<Range, Value>>> partitionByEnd()
        {
            List<Token> allEndpoints = new ArrayList<>(values.size() * 2);
            for (Map.Entry<Range, Value> a : values)
            {
                allEndpoints.add(accessor.start(a.getKey()));
                allEndpoints.add(accessor.end(a.getKey()));
            }
            allEndpoints.sort(comparator);
            List<Token> maxToken = new ArrayList<>(numChildren);
            int tick = allEndpoints.size() / numChildren;
            int offset = tick;
            for (int i = 0; i < numChildren; i++)
            {
                maxToken.add(allEndpoints.get(offset));
                offset += tick;
                if (offset >= allEndpoints.size())
                {
                    maxToken.add(allEndpoints.get(allEndpoints.size() - 1));
                    break;
                }
            }

            List<List<Map.Entry<Range, Value>>> partitions = new ArrayList<>(numChildren);
            for (int i = 0; i < numChildren; i++)
                partitions.add(new ArrayList<>());

            for (Map.Entry<Range, Value> a : values)
            {
                Token end = accessor.end(a.getKey());
                List<Map.Entry<Range, Value>> selected = null;
                for (int i = 0; i < numChildren; i++)
                {
                    if (comparator.compare(end, maxToken.get(i)) < 0)
                    {
                        selected = partitions.get(i);
                        break;
                    }
                }
                if (selected == null)
                    selected = partitions.get(partitions.size() - 1);
                selected.add(a);
            }
            int[] sizes = partitions.stream().mapToInt(List::size).toArray();
            return goodEnough(sizes) ? partitions : null;
        }

        private boolean goodEnough(int[] sizes)
        {
            double sum = 0.0;
            for (int i : sizes)
                sum += i;
            double mean = sum / sizes.length;
            double stddev = 0.0;
            for (int i : sizes)
                stddev += Math.pow(i - mean, 2);
            stddev = Math.sqrt(stddev / sizes.length);
            return stddev < 1.5;
        }

        void split()
        {
            children = new ArrayList<>(numChildren);
            for (int i = 0; i < numChildren; i++)
                children.add(new Node());

            List<List<Map.Entry<Range, Value>>> partitions = partitionByEnd();
            if (partitions == null)
                partitions = partitionEven();
            for (int i = 0; i < children.size(); i++)
            {
                Node c = children.get(i);
                List<Map.Entry<Range, Value>> entries = partitions.get(i);
                entries.forEach(e -> c.add(e.getKey(), e.getValue()));
            }

            values.clear();
            values = null;
        }

        private List<List<Map.Entry<Range, Value>>> partitionEven()
        {
            values.sort((a, b) -> {
                Range left = a.getKey();
                Range right = b.getKey();
                int rc = comparator.compare(accessor.start(left), accessor.start(right));
                if (rc == 0)
                    rc = comparator.compare(accessor.end(left), accessor.end(right));
                return rc;
            });
            List<List<Map.Entry<Range, Value>>> partition = new ArrayList<>(numChildren);
            int size = Math.max(1, values.size() / numChildren);
            int offset = 0;
            for (int i = 0; i < numChildren - 1; i++)
            {
                int total = size;
                partition.add(new ArrayList<>(values.subList(offset, offset + total)));
                offset += total;
            }
            partition.add(new ArrayList<>(values.subList(offset, values.size())));
            return partition;
        }

        <T> void search(Range range, Consumer<T> matches, Predicate<Map.Entry<Range, Value>> predicate, Function<Map.Entry<Range, Value>, T> transformer)
        {
            if (minStart == null)
                return;
            if (!intersects(range))
                return;
            if (children != null)
            {
                children.forEach(n -> n.search(range, matches, predicate, transformer));
                return;
            }
            values.forEach(e -> {
                if (accessor.intersects(e.getKey(), range) && predicate.test(e))
                    matches.accept(transformer.apply(e));
            });
        }

        <T> void searchToken(Token token, Consumer<T> matches, Predicate<Map.Entry<Range, Value>> predicate, Function<Map.Entry<Range, Value>, T> transformer)
        {
            if (minStart == null)
                return;
            if (!contains(minStart, maxEnd, token))
                return;
            if (children != null)
            {
                for (int i = 0, size = children.size(); i < size; i++)
                {
                    Node node = children.get(i);
                    node.searchToken(token, matches, predicate, transformer);
                }
                return;
            }
            values.forEach(e -> {
                if (accessor.contains(e.getKey(), token) && predicate.test(e))
                    matches.accept(transformer.apply(e));
            });
        }

        boolean intersects(Range range)
        {
            return accessor.intersects(range, minStart, maxEnd);
        }

        boolean contains(Token start, Token end, Token value)
        {
            return accessor.contains(start, end, value);
        }

        private void displayTree(int level, StringBuilder sb)
        {
            for (int i = 0; i < level; i++)
                sb.append('\t');
            sb.append("start:(").append(minStart).append(", ").append(maxStart).append("), end:(").append(minEnd).append(", ").append(maxEnd).append("):");
            if (children != null)
            {
                sb.append('\n');
                children.forEach(n -> n.displayTree(level + 1, sb));
            }
            else
            {
                sb.append(' ').append(size).append('\n');
            }
        }

        @Override
        public String toString()
        {
            return "Node{" +
                   "minStart=" + minStart +
                   ", maxStart=" + maxStart +
                   ", minEnd=" + minEnd +
                   ", maxEnd=" + maxEnd +
                   ", values=" + values +
                   ", children=" + children +
                   '}';
        }

        private Token min(Token a, Token b)
        {
            return comparator.compare(a, b) < 0 ? a : b;
        }

        private Token max(Token a, Token b)
        {
            return comparator.compare(a, b) < 0 ? b : a;
        }

        @Override
        public Iterator<Map.Entry<Range, Value>> iterator()
        {
            if (values != null)
                return values.iterator();
            return new AbstractIterator<>()
            {
                private int index = 0;
                private Iterator<Map.Entry<Range, Value>> it = null;
                @CheckForNull
                @Override
                protected Map.Entry<Range, Value> computeNext()
                {
                    while (true)
                    {
                        if (it == null)
                        {
                            if (index == children.size())
                                return endOfData();
                            it = children.get(index++).iterator();
                        }
                        if (it.hasNext())
                            return it.next();
                        it = null;
                    }
                }
            };
        }
    }
}
