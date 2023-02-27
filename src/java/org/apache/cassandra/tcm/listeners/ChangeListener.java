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

package org.apache.cassandra.tcm.listeners;

import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Invoked when cluster metadata changes
 *
 * `next` epoch is not guaranteed to directly follow `prev` epoch
 */
public interface ChangeListener
{
    /**
     * Called before updating ClusterMetadata.current() - it is still `prev` here.
     */
    default void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next) {}

    /**
     * Called after updating ClusterMetadata.current() - it is now `next`
     */
    default void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next) {}
}
