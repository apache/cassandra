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

import org.apache.cassandra.service.accord.AccordLoadingState.LoadingState;

public class AccordLoadingStateTest
{
    static class LoadableState extends AccordLoadingState<String, String>
    {
        public LoadableState(String key)
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
        LoadableState state = new LoadableState("K");

        Assert.assertEquals(LoadingState.UNINITIALIZED, state.state());
        assertIllegalState(() -> state.value());
        assertIllegalState(() -> state.value("VVVV"));
        assertIllegalState(() -> state.listen());

        Runnable runnable = state.load(k -> {
            Assert.assertEquals("K", k);
            return "V";
        });
        Assert.assertEquals(LoadingState.PENDING, state.state());

        runnable.run();
        Assert.assertEquals(LoadingState.LOADED, state.state());
        Assert.assertEquals("V", state.value());

        assertIllegalState(() -> state.load(k -> "CCC"));
        assertIllegalState(() -> state.listen());
    }

    @Test
    public void loadNullTest()
    {
        LoadableState state = new LoadableState("K");
        Assert.assertEquals(LoadingState.UNINITIALIZED, state.state());

        assertIllegalState(() -> state.value());
        assertIllegalState(() -> state.value("VVVV"));
        assertIllegalState(() -> state.listen());
        Runnable runnable = state.load(k -> {
            Assert.assertEquals("K", k);
            return null;
        });

        Assert.assertEquals(LoadingState.PENDING, state.state());

        runnable.run();
        Assert.assertEquals(LoadingState.LOADED, state.state());
        Assert.assertEquals(null, state.value());

        assertIllegalState(() -> state.load(k -> "CCC"));
        assertIllegalState(() -> state.listen());
    }

    @Test
    public void additionalCallbackTest()
    {
        LoadableState state = new LoadableState("K");
        Assert.assertEquals(LoadingState.UNINITIALIZED, state.state());

        assertIllegalState(() -> state.value());
        assertIllegalState(() -> state.value("VVVV"));
        assertIllegalState(() -> state.listen());
        Runnable runnable = state.load(k -> {
            Assert.assertEquals("K", k);
            return "V";
        });

        Assert.assertEquals(LoadingState.PENDING, state.state());

        // register other callbacks
        InspectableCallback<Object> callback1 = new InspectableCallback<>();
        InspectableCallback<Object> callback2 = new InspectableCallback<>();


        Assert.assertEquals(LoadingState.PENDING, state.state());
        state.listen().addCallback(callback1);
        runnable.run();
        state.listen().addCallback(callback2);

        Assert.assertTrue(callback1.called);
        Assert.assertNull(callback1.failure);

        Assert.assertTrue(callback2.called);
        Assert.assertNull(callback2.failure);

        Assert.assertEquals(LoadingState.LOADED, state.state());
        Assert.assertEquals("V", state.value());

        assertIllegalState(() -> state.load(k -> "CCC"));
        assertIllegalState(() -> state.listen());
    }

    @Test
    public void loadFailureTest()
    {
        LoadableState state = new LoadableState("K");

        Assert.assertEquals(LoadingState.UNINITIALIZED, state.state());
        assertIllegalState(() -> state.value());
        assertIllegalState(() -> state.value("VVVV"));
        assertIllegalState(() -> state.listen());

        Runnable runnable = state.load(k -> {
            throw new RuntimeException();
        });
        Assert.assertEquals(LoadingState.PENDING, state.state());

        runnable.run();
        Assert.assertEquals(LoadingState.FAILED, state.state());
        assertIllegalState(() -> state.value());
        Assert.assertTrue(state.failure() instanceof RuntimeException);

        assertIllegalState(() -> state.load(k -> "CCC"));
        assertIllegalState(() -> state.listen());
    }
}
