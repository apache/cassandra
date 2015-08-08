/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.PeekingIterator;

public abstract class AbstractIterator<V> implements Iterator<V>, PeekingIterator<V>
{

    private static enum State { MUST_FETCH, HAS_NEXT, DONE, FAILED }
    private State state = State.MUST_FETCH;
    private V next;

    protected V endOfData()
    {
        state = State.DONE;
        return null;
    }

    protected abstract V computeNext();

    public boolean hasNext()
    {
        switch (state)
        {
            case MUST_FETCH:
                state = State.FAILED;
                next = computeNext();

            default:
                if (state == State.DONE)
                    return false;

                state = State.HAS_NEXT;
                return true;

            case FAILED:
                throw new IllegalStateException();
        }
    }

    public V next()
    {
        if (state != State.HAS_NEXT && !hasNext())
            throw new NoSuchElementException();

        state = State.MUST_FETCH;
        V result = next;
        next = null;
        return result;
    }

    public V peek()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        return next;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
