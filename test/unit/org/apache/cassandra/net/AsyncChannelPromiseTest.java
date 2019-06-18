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

import org.junit.After;
import org.junit.Test;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

public class AsyncChannelPromiseTest extends TestAbstractAsyncPromise
{
    @After
    public void shutdown()
    {
        exec.shutdownNow();
    }

    private ChannelPromise newPromise()
    {
        return new AsyncChannelPromise(new EmbeddedChannel());
    }

    @Test
    public void testSuccess()
    {
        for (boolean setUncancellable : new boolean[] { false, true })
            for (boolean tryOrSet : new boolean[]{ false, true })
                testOneSuccess(newPromise(), setUncancellable, tryOrSet, null, null);
    }

    @Test
    public void testFailure()
    {
        for (boolean setUncancellable : new boolean[] { false, true })
            for (boolean tryOrSet : new boolean[] { false, true })
                for (Throwable v : new Throwable[] { null, new NullPointerException() })
                    testOneFailure(newPromise(), setUncancellable, tryOrSet, v, null);
    }


    @Test
    public void testCancellation()
    {
        for (boolean interruptIfRunning : new boolean[] { true, false })
            testOneCancellation(newPromise(), interruptIfRunning, null);
    }


    @Test
    public void testTimeout()
    {
        for (boolean setUncancellable : new boolean[] { true, false })
            testOneTimeout(newPromise(), setUncancellable);
    }

}
