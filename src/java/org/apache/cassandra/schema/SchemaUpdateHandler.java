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

package org.apache.cassandra.schema;

import java.time.Duration;

import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.utils.concurrent.Awaitable;

/**
 * Schema update handler is responsible for maintaining the shared schema and synchronizing it with other nodes in
 * the cluster, which means pushing and pulling changes, as well as tracking the current version in the cluster.
 * <p/>
 * The interface has been extracted to abstract out that functionality. It allows for various implementations like
 * Gossip based (the default), ETCD, offline, etc., and make it easier for mocking in unit tests.
 */
public interface SchemaUpdateHandler
{
    /**
     * Starts actively synchronizing schema with the rest of the cluster. It is called in the very beginning of the
     * node startup. It is not expected to block - to await for the startup completion we have another method
     * {@link #waitUntilReady(Duration)}.
     */
    void start();

    /**
     * Waits until the schema update handler is ready and returns the result. If the method returns {@code false} it
     * means that readiness could not be achieved within the specified period of time. The method can be used just to
     * check if schema is ready by passing {@link Duration#ZERO} as the timeout - in such case it returns immediately.
     *
     * @param timeout the maximum time to wait for schema readiness
     * @return whether readiness is achieved
     */
    boolean waitUntilReady(Duration timeout);

    /**
     * Applies schema transformation in the underlying storage and synchronizes with other nodes.
     *
     * @param transformation schema transformation to be performed
     * @param local          if true, the caller does not require synchronizing schema with other nodes - in practise local is
     *                       used only in some tests
     * @return transformation result
     */
    SchemaTransformationResult apply(SchemaTransformation transformation, boolean local);

    /**
     * Resets the schema either by reloading data from the local storage or from the other nodes. Once the schema is
     * refreshed, the callbacks provided in the factory method are executed, and the updated schema version is announced.
     *
     * @param local whether we should reset with locally stored schema or fetch the schema from other nodes
     */
    void reset(boolean local);

    /**
     * Marks the local schema to be cleared and refreshed. Since calling this method, the update handler tries to obtain
     * a fresh schema definition from a remote source. Once the schema definition is received, the local schema is
     * replaced (instead of being merged which usually happens when the update is received).
     * <p/>
     * The returned awaitable is fulfilled when the schema is received and applied.
     */
    Awaitable clear();
}
