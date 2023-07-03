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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ImmutableIntersectingPointValuesTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldTraversePointsInTermEnumOrder() throws IOException
    {
        final int minTerm = 0, maxTerm = 10;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableIntersectingPointValues pointValues = ImmutableIntersectingPointValues
                .fromTermEnum(termEnum, Int32Type.instance);

        pointValues.intersect(assertingVisitor(minTerm));
    }

    @Test
    public void shouldFailOnSorting()
    {
        final int minTerm = 3, maxTerm = 13;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableIntersectingPointValues pointValues = ImmutableIntersectingPointValues
                .fromTermEnum(termEnum, Int32Type.instance);

        expectedException.expect(IllegalStateException.class);
        pointValues.swap(0, 1);
    }

    @Test
    public void shouldSkipLuceneSorting() throws IOException
    {
        final int minTerm = 2, maxTerm = 7;
        final TermsIterator termEnum = buildDescTermEnum(minTerm, maxTerm);
        final ImmutableIntersectingPointValues pointValues = ImmutableIntersectingPointValues.fromTermEnum(termEnum, Int32Type.instance);

        assertFalse(pointValues.needsSorting());

        pointValues.intersect(assertingVisitor(minTerm));
    }

    private IntersectingPointValues.IntersectVisitor assertingVisitor(int minTerm)
    {
        return new IntersectingPointValues.IntersectVisitor()
        {
            int term = minTerm;
            int postingCounter = 0;

            @Override
            public void visit(long rowID, byte[] packedValue)
            {
                final ByteComparable actualTerm = ByteComparable.fixedLength(packedValue);
                final ByteComparable expectedTerm = ByteComparable.of(term);

                assertEquals(0, ByteComparable.compare(actualTerm, expectedTerm, ByteComparable.Version.OSS50));
                assertEquals(postingCounter, rowID);

                if (postingCounter >= 2)
                {
                    postingCounter = 0;
                    term++;
                }
                else
                {
                    postingCounter++;
                }
            }
        };
    }

    private TermsIterator buildDescTermEnum(int from, int to)
    {
        final ByteBuffer minTerm = Int32Type.instance.decompose(from);
        final ByteBuffer maxTerm = Int32Type.instance.decompose(to);

        final AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>> iterator = new AbstractGuavaIterator<Pair<ByteComparable, LongArrayList>>()
        {
            private int currentTerm = from;

            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                if (currentTerm <= to)
                {
                    return endOfData();
                }
                final ByteBuffer term = Int32Type.instance.decompose(currentTerm++);
                LongArrayList postings = new LongArrayList();
                postings.add(0, 1, 2);
                return Pair.create(ByteComparable.fixedLength(term), postings);
            }
        };

        return new MemtableTermsIterator(minTerm, maxTerm, iterator);
    }
}
