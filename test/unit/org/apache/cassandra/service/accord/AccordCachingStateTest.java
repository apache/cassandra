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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.service.accord.AccordCachingState.Status;

public class AccordCachingStateTest
{
    static class CachingState extends AccordCachingState<String, String>
    {
        public CachingState(String key)
        {
            super(key);
        }
    }

    static class InspectableCallback<V> implements BiConsumer<V, Throwable>
    {
        boolean called;
        V result;
        Throwable failure;

        @Override
        public void accept(V result, Throwable failure)
        {
            Assert.assertFalse(called);
            called = true;
            this.result = result;
            this.failure = failure;
        }
    }

    private static void assertIllegalState(Runnable runnable)
    {
        try
        {
            runnable.run();
            Assert.fail("Expected IllegalStateException");
        }
        catch (IllegalStateException ise)
        {
            // expected
        }
    }

    @Test
    public void loadSuccessTest()
    {
        ManualExecutor executor = new ManualExecutor();
        CachingState state = new CachingState("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::get);
        assertIllegalState(() -> state.set("VVVV"));
        assertIllegalState(state::loading);

        state.load(executor, k -> {
            Assert.assertEquals("K", k);
            return "V";
        });
        Assert.assertEquals(Status.LOADING, state.status());

        executor.runOne();
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertEquals("V", state.get());

        assertIllegalState(() -> state.load(executor, k -> "CCC"));
        assertIllegalState(state::loading);
    }

    @Test
    public void loadNullTest()
    {
        ManualExecutor executor = new ManualExecutor();
        CachingState state = new CachingState("K");
        Assert.assertEquals(Status.UNINITIALIZED, state.status());

        assertIllegalState(state::get);
        assertIllegalState(() -> state.set("VVVV"));
        assertIllegalState(state::loading);

        state.load(executor, k -> {
            Assert.assertEquals("K", k);
            return null;
        });
        Assert.assertEquals(Status.LOADING, state.status());

        executor.runOne();
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertNull(state.get());

        assertIllegalState(() -> state.load(executor, k -> "CCC"));
        assertIllegalState(state::loading);
    }

    @Test
    public void additionalCallbackTest()
    {
        ManualExecutor executor = new ManualExecutor();
        CachingState state = new CachingState("K");
        Assert.assertEquals(Status.UNINITIALIZED, state.status());

        assertIllegalState(state::get);
        assertIllegalState(() -> state.set("VVVV"));
        assertIllegalState(state::loading);

        state.load(executor, k -> {
            Assert.assertEquals("K", k);
            return "V";
        });
        Assert.assertEquals(Status.LOADING, state.status());

        // register other callbacks
        InspectableCallback<Object> callback1 = new InspectableCallback<>();
        InspectableCallback<Object> callback2 = new InspectableCallback<>();

        Assert.assertEquals(Status.LOADING, state.status());
        state.loading().addCallback(callback1);
        executor.runOne();
        state.loading().addCallback(callback2);

        Assert.assertTrue(callback1.called);
        Assert.assertNull(callback1.failure);

        Assert.assertTrue(callback2.called);
        Assert.assertNull(callback2.failure);

        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertEquals("V", state.get());

        assertIllegalState(() -> state.load(executor, k -> "CCC"));
        assertIllegalState(state::loading);
    }

    @Test
    public void loadFailureTest()
    {
        ManualExecutor executor = new ManualExecutor();
        CachingState state = new CachingState("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::get);
        assertIllegalState(() -> state.set("VVVV"));
        assertIllegalState(state::loading);

        state.load(executor, k -> {
            throw new RuntimeException();
        });
        Assert.assertEquals(Status.LOADING, state.status());

        executor.runOne();
        Assert.assertEquals(Status.FAILED_TO_LOAD, state.status());
        assertIllegalState(state::get);
        Assert.assertTrue(state.failure() instanceof RuntimeException);

        assertIllegalState(() -> state.load(executor, k -> "CCC"));
        assertIllegalState(state::loading);
    }
}
