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

package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reproducer: ReverseValueIterator leaks Rebufferer on constructor exception
 *
 * <p>If initializeWithRightBound() or initializeNoRightBound() throws after the Walker
 * super-constructor acquires a Rebufferer, the Rebufferer is never closed. Compare with
 * ValueIterator which wraps initialization in try-catch with super.close().
 *
 * Expected: Rebufferer.closeReader() is called when initialization throws.
 * Actual (buggy): Rebufferer leaks — closeReader() never called.
 * Failure criterion: closeReader() not called after constructor exception.
 */
@SuppressWarnings({"unchecked", "RedundantSuppression"})
public class ReverseValueIteratorLeakTest extends AbstractTrieTestBase
{
    // ORACLE: Rebufferer.closeReader() must be called if constructor throws
    @Test
    public void testRebuffererClosedOnInitWithRightBoundFailure() throws IOException
    {
        // HARNESS: build a valid trie, wrap its Rebufferer with close tracking,
        //          then trigger a failure during initializeWithRightBound
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, buf);
        builder.add(source("aaa"), 1);
        builder.add(source("bbb"), 2);
        long root = builder.complete();

        AtomicBoolean closeReaderCalled = new AtomicBoolean(false);
        Rebufferer trackingSource = new TrackingRebufferer(buf.asNewBuffer(), closeReaderCalled);

        // TRIGGER: provide a ByteComparable whose ByteSource throws during iteration,
        //          causing initializeWithRightBound to fail after Walker acquires the Rebufferer
        ByteComparable throwingEnd = v -> new ByteSource()
        {
            private int calls = 0;

            @Override
            public int next()
            {
                if (++calls > 1)
                    throw new RuntimeException("simulated failure during trie traversal");
                return 'b';
            }
        };

        try
        {
            new ReverseValueIterator<>(trackingSource, root, source("aaa"), throwingEnd, false);
            fail("Constructor should have thrown");
        }
        catch (RuntimeException e)
        {
            assertTrue("Expected simulated failure", e.getMessage().contains("simulated failure"));
        }

        // ORACLE: Rebufferer must have been closed despite the constructor failure
        assertTrue("Rebufferer.closeReader() must be called when ReverseValueIterator " +
                   "constructor fails during initializeWithRightBound — without the fix, " +
                   "the Rebufferer leaks",
                   closeReaderCalled.get());
    }

    private static class TrackingRebufferer extends ByteBufRebufferer
    {
        private final AtomicBoolean closeReaderCalled;

        TrackingRebufferer(ByteBuffer buffer, AtomicBoolean closeReaderCalled)
        {
            super(buffer);
            this.closeReaderCalled = closeReaderCalled;
        }

        @Override
        public void closeReader()
        {
            closeReaderCalled.set(true);
        }
    }
}
