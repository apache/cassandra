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

package org.apache.cassandra.service.accord;

import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.api.DataStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public enum AccordDataStore implements DataStore
{
    INSTANCE;

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        //TODO (implement): do real work
        callback.starting(ranges);
        callback.fetched(ranges);
        return new FakeFetchResult(ranges);
    }

    private static final class FakeFetchResult implements FetchResult
    {
        private final AsyncResult<Ranges> delegate;

        private FakeFetchResult(Ranges ranges)
        {
            delegate = AsyncResults.success(ranges);
        }

        @Override
        public void abort(Ranges ranges)
        {
            // ignore
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super Ranges, ? extends T> mapper)
        {
            return delegate.map(mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super Ranges, ? extends AsyncChain<T>> mapper)
        {
            return delegate.flatMap(mapper);
        }

        @Override
        public AsyncResult<Ranges> addCallback(BiConsumer<? super Ranges, Throwable> callback)
        {
            return delegate.addCallback(callback);
        }

        @Override
        public boolean isDone()
        {
            return delegate.isDone();
        }

        @Override
        public boolean isSuccess()
        {
            return delegate.isSuccess();
        }
    }
}
