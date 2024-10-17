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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.accord.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.AccordCache.Type;

public class AccordCacheEntryTest
{
    static class CacheEntry extends AccordCacheEntry<String, String>
    {
        public CacheEntry(String key, Type<String, String, ?>.Instance instance)
        {
            super(key, instance);
        }

        public CacheEntry(String key)
        {
            this(key, null);
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
        CacheEntry state = new CacheEntry("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        Assert.assertEquals(Status.LOADING, state.status());

        state.testLoaded("V");
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertEquals("V", state.getExclusive());

        assertIllegalState(state::testLoad);
        assertIllegalState(() -> state.loaded(null));
        assertIllegalState(state::loading);
    }

    @Test
    public void loadNullTest()
    {
        CacheEntry state = new CacheEntry("K");
        Assert.assertEquals(Status.UNINITIALIZED, state.status());

        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        Assert.assertEquals(Status.LOADING, state.status());

        // TODO (expected): this is sort of a pointless test now - remove it?
        state.testLoaded(null);
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertNull(state.getExclusive());

        assertIllegalState(state::testLoad);
        assertIllegalState(state::failedToLoad);
        assertIllegalState(state::loading);
    }

    @Test
    public void loadFailureTest()
    {
        CacheEntry state = new CacheEntry("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        state.failedToLoad();
        Assert.assertEquals(Status.FAILED_TO_LOAD, state.status());
        assertIllegalState(state::getExclusive);
    }
}
