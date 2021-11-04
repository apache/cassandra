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
package org.apache.cassandra.net;

import io.netty.util.concurrent.Future; //checkstyle: permit this import

/**
 * An abstraction for yielding a result performed by an asynchronous task,
 * for whom we may wish to offer cancellation, but no other access to the underlying task
 */
public class FutureResult<V> extends FutureDelegate<V>
{
    private final Future<?> tryCancel;

    /**
     * @param result the Future that will be completed by {@link #cancel}
     * @param cancel the Future that is performing the work, and to whom any cancellation attempts will be proxied
     */
    public FutureResult(Future<V> result, Future<?> cancel)
    {
        super(result);
        this.tryCancel = cancel;
    }

    @Override
    public boolean cancel(boolean b)
    {
        tryCancel.cancel(true);
        return delegate.cancel(b);
    }
}
