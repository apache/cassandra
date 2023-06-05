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

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.TailOverridingRebufferer;

import static org.junit.Assert.assertEquals;

public class TrieBuilderTest extends AbstractTrieTestBase
{
    @Test
    public void testPartialBuildRecalculationBug() throws IOException
    {
        DataOutputBuffer buf = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, buf);
        long count = 0;

        count += addUntilBytesWritten(buf, builder, "a", 1);            // Make a node whose children are written
        long reset = count;
        count += addUntilBytesWritten(buf, builder, "c", 64 * 1024);    // Finalize it and write long enough to grow its pointer size

        dump = true;
        IncrementalTrieWriter.PartialTail tail = builder.makePartialRoot();
        // The line above hit an assertion as that node's parent had a pre-calculated branch size which was no longer
        // correct, and we didn't bother to reset it.
        dump = false;

        // Check that partial representation has the right content.
        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());
        source = new TailOverridingRebufferer(source, tail.cutoff(), tail.tail());
        verifyContent(count, source, tail.root(), reset);

        long reset2 = count;

        // Also check the completed trie.
        count += addUntilBytesWritten(buf, builder, "e", 16 * 1024);
        dump = true;
        long root = builder.complete();
        // The line above hit another size assertion as the size of a node's branch growing caused it to need to switch
        // format, but we didn't bother to recalculate its size.
        dump = false;

        source = new ByteBufRebufferer(buf.asNewBuffer());
        verifyContent(count, source, root, reset, reset2);
    }

    public void verifyContent(long count, Rebufferer source, long root, long... resets)
    {
        ValueIterator<?> iter = new ValueIterator<>(source, root);
        long found = 0;
        long ofs = 0;
        int rpos = 0;
        long pos;
        while ((pos = iter.nextPayloadedNode()) != -1)
        {
            iter.go(pos);
            assertEquals(valueFor(found - ofs), iter.payloadFlags());
            ++found;
            if (rpos < resets.length && found >= resets[rpos])
            {
                ofs = resets[rpos];
                ++rpos;
            }
        }
        assertEquals(count, found);
    }

    private long addUntilBytesWritten(DataOutputBuffer buf,
                                      IncrementalTrieWriter<Integer> builder,
                                      String prefix,
                                      long howMany) throws IOException
    {
        long pos = buf.position();
        long idx = 0;
        while (pos + howMany > buf.position())
        {
            builder.add(source(String.format("%s%8s", prefix, toBase(idx))), valueFor(idx));
            logger.info("Adding {} : {}", String.format("%s%8s", prefix, toBase(idx)), valueFor(idx));
            ++idx;
        }
        logger.info(String.format("%s%8s", prefix, toBase(idx - 1)));
        return idx;
    }
}
