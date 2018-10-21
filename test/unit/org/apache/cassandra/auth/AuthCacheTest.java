
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
package org.apache.cassandra.auth;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthCacheTest
{
    private boolean loadFuncCalled = false;
    private boolean isCacheEnabled = false;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCaching()
    {
        AuthCache<String, String> authCache = new AuthCache<>("TestCache",
                                                              DatabaseDescriptor::setCredentialsValidity,
                                                              DatabaseDescriptor::getCredentialsValidity,
                                                              DatabaseDescriptor::setCredentialsUpdateInterval,
                                                              DatabaseDescriptor::getCredentialsUpdateInterval,
                                                              DatabaseDescriptor::setCredentialsCacheMaxEntries,
                                                              DatabaseDescriptor::getCredentialsCacheMaxEntries,
                                                              this::load,
                                                              () -> true
        );

        // Test cacheloader is called if set
        loadFuncCalled = false;
        String result = authCache.get("test");
        assertTrue(loadFuncCalled);
        Assert.assertEquals("load", result);

        // value should be fetched from cache
        loadFuncCalled = false;
        String result2 = authCache.get("test");
        assertFalse(loadFuncCalled);
        Assert.assertEquals("load", result2);

        // value should be fetched from cache after complete invalidate
        authCache.invalidate();
        loadFuncCalled = false;
        String result3 = authCache.get("test");
        assertTrue(loadFuncCalled);
        Assert.assertEquals("load", result3);

        // value should be fetched from cache after invalidating key
        authCache.invalidate("test");
        loadFuncCalled = false;
        String result4 = authCache.get("test");
        assertTrue(loadFuncCalled);
        Assert.assertEquals("load", result4);

        // set cache to null and load function should be called
        loadFuncCalled = false;
        authCache.cache = null;
        String result5 = authCache.get("test");
        assertTrue(loadFuncCalled);
        Assert.assertEquals("load", result5);
    }

    @Test
    public void testInitCache()
    {
        // Test that a validity of <= 0 will turn off caching
        DatabaseDescriptor.setCredentialsValidity(0);
        AuthCache<String, String> authCache = new AuthCache<>("TestCache2",
                                                              DatabaseDescriptor::setCredentialsValidity,
                                                              DatabaseDescriptor::getCredentialsValidity,
                                                              DatabaseDescriptor::setCredentialsUpdateInterval,
                                                              DatabaseDescriptor::getCredentialsUpdateInterval,
                                                              DatabaseDescriptor::setCredentialsCacheMaxEntries,
                                                              DatabaseDescriptor::getCredentialsCacheMaxEntries,
                                                              this::load,
                                                              () -> true);
        assertNull(authCache.cache);
        authCache.setValidity(2000);
        authCache.cache = authCache.initCache(null);
        assertNotNull(authCache.cache);

        // Test enableCache works as intended
        authCache = new AuthCache<>("TestCache3",
                                    DatabaseDescriptor::setCredentialsValidity,
                                    DatabaseDescriptor::getCredentialsValidity,
                                    DatabaseDescriptor::setCredentialsUpdateInterval,
                                    DatabaseDescriptor::getCredentialsUpdateInterval,
                                    DatabaseDescriptor::setCredentialsCacheMaxEntries,
                                    DatabaseDescriptor::getCredentialsCacheMaxEntries,
                                    this::load,
                                    () -> isCacheEnabled);
        assertNull(authCache.cache);
        isCacheEnabled = true;
        authCache.cache = authCache.initCache(null);
        assertNotNull(authCache.cache);

        // Ensure at a minimum these policies have been initialised by default
        assertTrue(authCache.cache.policy().expireAfterWrite().isPresent());
        assertTrue(authCache.cache.policy().refreshAfterWrite().isPresent());
        assertTrue(authCache.cache.policy().eviction().isPresent());
    }

    private String load(String test)
    {
        loadFuncCalled = true;
        return "load";
    }

}
