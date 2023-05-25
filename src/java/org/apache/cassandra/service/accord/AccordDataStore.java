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

import accord.api.DataStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncResults;

public enum AccordDataStore implements DataStore
{
    INSTANCE;

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        //TODO (implement): do real work
        callback.starting(ranges).started(Timestamp.NONE);
        callback.fetched(ranges);
        return new ImmediateFetchFuture(ranges);
    }

    private static class ImmediateFetchFuture extends AsyncResults.SettableResult<Ranges> implements FetchResult
    {
        ImmediateFetchFuture(Ranges ranges) { setSuccess(ranges); }
        @Override public void abort(Ranges abort) { }
    }
}
