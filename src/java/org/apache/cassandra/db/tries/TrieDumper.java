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

import org.agrona.DirectBuffer;

/// Simple utility class for dumping the structure of a trie to string.
abstract class TrieDumper<T> implements Cursor.Walker<T, String>
{
    protected final StringBuilder b;
    int needsIndent = -1;
    int currentLength = 0;
    int depthAdjustment = 0;

    TrieDumper()
    {
        this.b = new StringBuilder();
    }

    protected void endLineAndSetIndent(int newIndent)
    {
        needsIndent = newIndent;
    }

    @Override
    public void resetPathLength(int newLength)
    {
        currentLength = newLength + depthAdjustment;
        endLineAndSetIndent(currentLength);
    }

    protected void maybeIndent()
    {
        if (needsIndent >= 0)
        {
            b.append('\n');
            for (int i = 0; i < needsIndent; ++i)
                b.append("  ");
            needsIndent = -1;
        }
    }

    @Override
    public void addPathByte(int nextByte)
    {
        maybeIndent();
        ++currentLength;
        b.append(String.format("%02x", nextByte));
    }

    @Override
    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        maybeIndent();
        for (int i = 0; i < count; ++i)
            b.append(String.format("%02x", buffer.getByte(pos + i) & 0xFF));
        currentLength += count;
    }

    @Override
    public String complete()
    {
        return b.toString();
    }

    static class Plain<T> extends TrieDumper<T>
    {
        protected final Function<T, String> contentToString;

        public Plain(Function<T, String> contentToString)
        {
            super();
            this.contentToString = contentToString;
        }

        @Override
        public void content(T content)
        {
            b.append(" -> ");
            b.append(contentToString.apply(content));
            endLineAndSetIndent(currentLength);
        }
    }

    static class DeletionAware<T, D extends RangeState<D>> extends Plain<T>
    implements DeletionAwareTrie.DeletionAwareWalker<T, D, String>
    {
        final Function<D, String> rangeToString;

        public DeletionAware(Function<T, String> contentToString,
                             Function<D, String> rangeToString)
        {
            super(contentToString);
            this.rangeToString = rangeToString;
        }

        @Override
        public void deletionMarker(D content)
        {
            b.append(" -> ");
            b.append(rangeToString.apply(content));
            endLineAndSetIndent(currentLength);
        }

        @Override
        public boolean enterDeletionsBranch()
        {
            b.append("*** Start deletion branch");
            endLineAndSetIndent(currentLength);
            depthAdjustment = currentLength;
            currentLength = 0;
            return true;
        }

        @Override
        public void exitDeletionsBranch()
        {
            endLineAndSetIndent(depthAdjustment);
            maybeIndent();
            b.append("*** End deletion branch");
            resetPathLength(0);
            depthAdjustment = 0;
        }
    }
}
