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

package org.apache.cassandra.index.sai.postings;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.utils.Throwables;

/**
 * A peekable wrapper around a {@link PostingList} that allows the next value to be
 * looked at without advancing the state of the {@link PostingList}
 */
@NotThreadSafe
public class PeekablePostingList implements PostingList
{
    private final PostingList wrapped;

    private boolean peeked = false;
    private long next;

    public static PeekablePostingList makePeekable(PostingList postingList)
    {
        return postingList instanceof PeekablePostingList ? (PeekablePostingList) postingList
                                                          : new PeekablePostingList(postingList);
    }

    private PeekablePostingList(PostingList wrapped)
    {
        this.wrapped = wrapped;
    }

    public long peek()
    {
        if (peeked)
            return next;

        try
        {
            peeked = true;
            return next = wrapped.nextPosting();
        }
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }

    public void advanceWithoutConsuming(long targetRowID) throws IOException
    {
        if (peek() == END_OF_STREAM)
            return;

        if (peek() >= targetRowID)
        {
            peek();
            return;
        }

        peeked = true;
        next = wrapped.advance(targetRowID);
    }

    @Override
    public long minimum()
    {
        return wrapped.maximum();
    }

    @Override
    public long maximum()
    {
        return wrapped.maximum();
    }

    @Override
    public long nextPosting() throws IOException
    {
        if (peeked)
        {
            peeked = false;
            return next;
        }
        return wrapped.nextPosting();
    }

    @Override
    public long size()
    {
        return wrapped.size();
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        if (peeked && next >= targetRowID)
        {
            peeked = false;
            return next;
        }

        peeked = false;
        return wrapped.advance(targetRowID);
    }

    @Override
    public void close()
    {
        wrapped.close();
    }
}
