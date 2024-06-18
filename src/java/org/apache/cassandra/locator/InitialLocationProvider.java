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

package org.apache.cassandra.locator;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

public interface InitialLocationProvider
{
    /**
     * Provides the Location with which a new node should register itself with ClusterMetadata.
     * After registration, this is no longer necessary and Location is always sourced from
     * ClusterMetadata
     * @return the datacenter and rack to register with
     */
    Location initialLocation();

    /**
     * Validate that the locations of previously registered peers are considered valid by the locally configured
     * InitialLocationProvider. In practice, of the in-tree implementations only Ec2LocationProvider overrides this and
     * uses it to ensure that the same ec2 naming scheme is used across all peers.
     * See CASSANDRA-7839 for origins.
     * @param metadata ClusterMetadata at the time of registering
     * @return true if the implementation considers the locations of existing nodes compatible with its own
     *         configuration, false otherwise
     */
    default void validate(ClusterMetadata metadata) {}
}
