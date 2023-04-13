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

package org.apache.cassandra.index.sai.disk.v1.trie;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.tries.ValueIterator;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe specialized value -> long trie prefix searcher.
 *
 * This is a specialization of the {@link Walker} that will look for a prefix or
 * a complete term and return the first payload associated with the term.
 *
 * TODO Make generic to handle any payload type.
 * TODO Extend search to return first payload or all sub-payloads as iterator (LIKE support?)
 */
@NotThreadSafe
public class TriePrefixSearcher extends ValueIterator<TriePrefixSearcher>
{
    public static final long NOT_FOUND = -1L;

    public TriePrefixSearcher(Rebufferer source, long root)
    {
        super(source, root, false);
    }

    public long prefixSearch(ByteSource startStream)
    {
        IterationPosition prev = null;
        int childIndex;
        long payloadedNode = -1;

        try
        {
            // Follow start position while we still have a prefix, stacking path and saving prefixes.
            go(root);
            while (true)
            {
                int s = startStream.next();
                childIndex = search(s);

                // For a separator trie the latest payload met along the prefix is a potential match for start
                if (childIndex == 0 || childIndex == -1)
                {
                    if (hasPayload())
                        payloadedNode = position;
                }
                else
                    payloadedNode = -1;

                if (childIndex < 0)
                    break;

                prev = new IterationPosition(position, childIndex, 256, prev);
                go(transition(childIndex));
            }

            childIndex = -1 - childIndex - 1;

            stack = new IterationPosition(position, childIndex, 256, prev);

            // Advancing now gives us first match if we didn't find one already.
            if (payloadedNode == -1)
                advanceNode();

            if (stack == null || !hasPayload())
                return NOT_FOUND;

            return SizedInts.read(buf, payloadPosition(), payloadFlags());
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }
}