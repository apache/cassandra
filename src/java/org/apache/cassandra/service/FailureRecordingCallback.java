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

package org.apache.cassandra.service;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.utils.concurrent.IntrusiveStack;

import static org.apache.cassandra.exceptions.RequestFailureReason.TIMEOUT;

public abstract class FailureRecordingCallback<T> implements RequestCallbackWithFailure<T>
{
    public static class FailureResponses extends IntrusiveStack<FailureResponses> implements Map.Entry<InetAddressAndPort, RequestFailureReason>
    {
        final InetAddressAndPort from;
        final RequestFailureReason reason;

        public FailureResponses(InetAddressAndPort from, RequestFailureReason reason)
        {
            this.from = from;
            this.reason = reason;
        }

        @Override
        public InetAddressAndPort getKey()
        {
            return from;
        }

        @Override
        public RequestFailureReason getValue()
        {
            return reason;
        }

        @Override
        public RequestFailureReason setValue(RequestFailureReason value)
        {
            throw new UnsupportedOperationException();
        }

        public static <O> void push(AtomicReferenceFieldUpdater<O, FailureResponses> headUpdater, O owner, InetAddressAndPort from, RequestFailureReason reason)
        {
            push(headUpdater, owner, new FailureResponses(from, reason));
        }

        public static <O> void pushExclusive(AtomicReferenceFieldUpdater<O, FailureResponses> headUpdater, O owner, InetAddressAndPort from, RequestFailureReason reason)
        {
            pushExclusive(headUpdater, owner, new FailureResponses(from, reason));
        }

        public static FailureResponses pushExclusive(FailureResponses head, InetAddressAndPort from, RequestFailureReason reason)
        {
            return IntrusiveStack.pushExclusive(head, new FailureResponses(from, reason));
        }

        public static int size(FailureResponses head)
        {
            return IntrusiveStack.size(head);
        }

        public static Iterator<FailureResponses> iterator(FailureResponses head)
        {
            return IntrusiveStack.iterator(head);
        }

        public static int failureCount(FailureResponses head)
        {
            return (int)IntrusiveStack.accumulate(head, (f, v) -> f.reason == TIMEOUT ? v : v + 1, 0);
        }
    }

    public static class AsMap extends AbstractMap<InetAddressAndPort, RequestFailureReason>
    {
        final FailureResponses head;
        int size = -1;

        AsMap(FailureResponses head)
        {
            this.head = head;
        }

        @Override
        public Set<Map.Entry<InetAddressAndPort, RequestFailureReason>> entrySet()
        {
            return new AbstractSet<Map.Entry<InetAddressAndPort, RequestFailureReason>>()
            {
                @Override
                public Iterator<Map.Entry<InetAddressAndPort, RequestFailureReason>> iterator()
                {
                    return (Iterator<Map.Entry<InetAddressAndPort, RequestFailureReason>>)
                            (Iterator<?>) FailureResponses.iterator(head);
                }

                @Override
                public int size()
                {
                    if (size < 0)
                        size = FailureResponses.size(head);
                    return size;
                }
            };
        }

        public int failureCount()
        {
            return FailureResponses.failureCount(head);
        }
    }

    private volatile FailureResponses failureResponses;
    private static final AtomicReferenceFieldUpdater<FailureRecordingCallback, FailureResponses> responsesUpdater = AtomicReferenceFieldUpdater.newUpdater(FailureRecordingCallback.class, FailureResponses.class, "failureResponses");

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        FailureResponses.push(responsesUpdater, this, from, failureReason);
    }

    protected void onFailureWithMutex(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        FailureResponses.pushExclusive(responsesUpdater, this, from, failureReason);
    }

    protected AsMap failureReasonsAsMap()
    {
        return new AsMap(failureResponses);
    }
}
