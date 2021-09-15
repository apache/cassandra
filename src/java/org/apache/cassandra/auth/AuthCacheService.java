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

import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthCacheService
{
    private static final Logger logger = LoggerFactory.getLogger(AuthCacheService.class);
    public static final AuthCacheService instance = new AuthCacheService();

    private final Set<AuthCache<?, ?>> caches = new HashSet<>();

    public synchronized void register(AuthCache<?, ?> cache)
    {
        Preconditions.checkNotNull(cache);
        caches.add(cache);
    }

    public synchronized void unregister(AuthCache<?, ?> cache)
    {
        Preconditions.checkNotNull(cache);
        caches.remove(cache);
    }

    public void warmCaches()
    {
        logger.info("Initiating bulk load of {} auth cache(s)", caches.size());
        for (AuthCache<?, ?> cache : caches)
        {
            cache.warm();
        }
    }

    /**
     * We have a couple of static initializer functions to create caches scattered across various classes, some solo
     * and some with multiple member variables. As we expect these caches to be created and initialized in one logical
     * block, we tie them together and use them here.
     *
     * Note: We also register the PasswordAuthenticator cache here with an instance member call to:
     * {@link PasswordAuthenticator#setup}
     */
    @VisibleForTesting
    public static void initializeAndRegisterCaches()
    {
        AuthenticatedUser.init();
        Roles.init();
    }
}