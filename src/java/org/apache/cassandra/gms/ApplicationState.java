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
package org.apache.cassandra.gms;

/**
 * The various "states" exchanged through Gossip.
 *
 * <p><b>Important Note:</b> Gossip uses the ordinal of this enum in the messages it exchanges, so values in that enum
 * should <i>not</i> be re-ordered or removed. The end of this enum should also always include some "padding" so that
 * if newer versions add new states, old nodes that don't know about those new states don't "break" deserializing those
 * states.
 */
public enum ApplicationState
{
    // never remove a state here, ordering matters.
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") STATUS, //Deprecated and unsued in 4.0, stop publishing in 5.0, reclaim in 6.0
    LOAD,
    SCHEMA,
    DC,
    RACK,
    RELEASE_VERSION,
    REMOVAL_COORDINATOR,
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") INTERNAL_IP, //Deprecated and unused in 4.0, stop publishing in 5.0, reclaim in 6.0
    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") RPC_ADDRESS, // ^ Same
    X_11_PADDING, // padding specifically for 1.1
    SEVERITY,
    NET_VERSION,
    HOST_ID,
    TOKENS,
    RPC_READY,
    // pad to allow adding new states to existing cluster
    INTERNAL_ADDRESS_AND_PORT, //Replacement for INTERNAL_IP with up to two ports
    NATIVE_ADDRESS_AND_PORT, //Replacement for RPC_ADDRESS
    STATUS_WITH_PORT, //Replacement for STATUS
    /**
     * The set of sstable versions on this node. This will usually be only the "current" sstable format (the one with
     * which new sstables are written), but may contain more on newly upgraded nodes before `upgradesstable` has been
     * run.
     *
     * <p>The value (a set of sstable {@link org.apache.cassandra.io.sstable.format.Version}) is serialized as
     * a comma-separated list.
     **/
    SSTABLE_VERSIONS,
    DISK_USAGE,
    INDEX_STATUS,
    // DO NOT EDIT OR REMOVE PADDING STATES BELOW - only add new states above.  See CASSANDRA-16484
    X1,
    X2,
    X3,
    X4,
    X5,
    X6,
    X7,
    X8,
    X9,
    X10,
}
