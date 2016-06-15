/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.transform;

import java.util.Arrays;

class Stack
{
    public static final Transformation[] EMPTY_TRANSFORMATIONS = new Transformation[0];
    public static final MoreContentsHolder[] EMPTY_MORE_CONTENTS_HOLDERS = new MoreContentsHolder[0];
    static final Stack EMPTY = new Stack();

    Transformation[] stack;
    int length; // number of used stack entries
    MoreContentsHolder[] moreContents; // stack of more contents providers (if any; usually zero or one)

    // an internal placeholder for a MoreContents, storing the associated stack length at time it was applied
    static class MoreContentsHolder
    {
        final MoreContents moreContents;
        int length;
        private MoreContentsHolder(MoreContents moreContents, int length)
        {
            this.moreContents = moreContents;
            this.length = length;
        }
    }

    Stack()
    {
        stack = EMPTY_TRANSFORMATIONS;
        moreContents = EMPTY_MORE_CONTENTS_HOLDERS;
    }

    Stack(Stack copy)
    {
        stack = copy.stack;
        length = copy.length;
        moreContents = copy.moreContents;
    }

    void add(Transformation add)
    {
        if (length == stack.length)
            stack = resize(stack);
        stack[length++] = add;
    }

    void add(MoreContents more)
    {
        this.moreContents = Arrays.copyOf(moreContents, moreContents.length + 1);
        this.moreContents[moreContents.length - 1] = new MoreContentsHolder(more, length);
    }

    private static <E> E[] resize(E[] array)
    {
        int newLen = array.length == 0 ? 5 : array.length * 2;
        return Arrays.copyOf(array, newLen);
    }

    // reinitialise the transformations after a moreContents applies
    void refill(Stack prefix, MoreContentsHolder holder, int index)
    {
        // drop the transformations that were present when the MoreContents was attached,
        // and prefix any transformations in the new contents (if it's a transformer)
        moreContents = splice(prefix.moreContents, prefix.moreContents.length, moreContents, index, moreContents.length);
        stack = splice(prefix.stack, prefix.length, stack, holder.length, length);
        length += prefix.length - holder.length;
        holder.length = prefix.length;
    }

    private static <E> E[] splice(E[] prefix, int prefixCount, E[] keep, int keepFrom, int keepTo)
    {
        int keepCount = keepTo - keepFrom;
        int newCount = prefixCount + keepCount;
        if (newCount > keep.length)
            keep = Arrays.copyOf(keep, newCount);
        if (keepFrom != prefixCount)
            System.arraycopy(keep, keepFrom, keep, prefixCount, keepCount);
        if (prefixCount != 0)
            System.arraycopy(prefix, 0, keep, 0, prefixCount);
        return keep;
    }
}

