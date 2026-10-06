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

import org.agrona.DirectBuffer;

/// Simple utility class for dumping the structure of a trie to string.
///
/// This version is in the form of abstract classes so that the implementation of the conversion to string can access
/// the key bytes and store additional information.
public abstract class TrieDumperWithPath<T> extends TriePathReconstructor implements Cursor.Walker<T, String>
{
    protected final StringBuilder b;
    int needsIndent = -1;
    int currentLength = 0;
    int depthAdjustment = 0;

    TrieDumperWithPath()
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
        super.resetPathLength(currentLength);
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
        super.addPathByte(nextByte);
        maybeIndent();
        ++currentLength;
        b.append(String.format("%02x", nextByte));
    }

    @Override
    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        super.addPathBytes(buffer, pos, count);
        maybeIndent();
        for (int i = 0; i < count; ++i)
            b.append(String.format("%02x", buffer.getByte(pos + i) & 0xFF));
        currentLength += count;
    }

    @Override
    public void onReturnPath()
    {
        super.onReturnPath();
        maybeIndent();
        b.append('↑');
    }

    @Override
    public String complete()
    {
        return b.toString();
    }

    public static abstract class Plain<T> extends TrieDumperWithPath<T>
    {
        /// Convert the given content to string. This method can make use of [#keyBytes] and [#keyPos].
        public abstract String contentToString(T content);

        @Override
        public void content(T content)
        {
            b.append(" -> ");
            b.append(contentToString(content));
            endLineAndSetIndent(currentLength);
        }
    }

    public static abstract class DeletionAware<T, D extends RangeState<D>> extends Plain<T>
    implements DeletionAwareCursor.DeletionAwareWalker<T, D, String>
    {
        /// Convert the given deletion marker to string. This method can make use of [#keyBytes] and [#keyPos].
        public abstract String deletionToString(D deletionMarker);

        @Override
        public void deletionMarker(D content)
        {
            b.append(" -> ");
            b.append(deletionToString(content));
            endLineAndSetIndent(currentLength);
        }

        @Override
        public boolean enterDeletionsBranch()
        {
            maybeIndent();
            b.append("*** Start deletion branch");
            endLineAndSetIndent(currentLength);
            depthAdjustment = currentLength;
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
