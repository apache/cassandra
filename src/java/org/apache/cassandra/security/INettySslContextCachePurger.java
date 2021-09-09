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

package org.apache.cassandra.security;

/**
 * Cassandra maintains cache for Netty's SslContext objects for client and server socket types. This interface allows
 * {@link ISslContextFactory}'s implementation to clear that cache upon special needs.
 */
public interface INettySslContextCachePurger
{
    /**
     * Purges the Netty SslContext cache.
     */
    void purge();

    /**
     * Allows peek into size of the current cache. Mainly used for testing only.
     * @return the current size of the Netty SslContext cache.
     */
    int cacheSize();
}
