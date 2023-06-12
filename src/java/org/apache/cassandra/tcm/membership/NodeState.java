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

package org.apache.cassandra.tcm.membership;

import java.util.EnumSet;
import java.util.Set;

public enum NodeState
{
    REGISTERED,
    BOOTSTRAPPING,
    BOOT_REPLACING,
    JOINED,
    LEAVING,
    LEFT,
    MOVING;
    private static final Set<NodeState> PRE_JOIN_STATES = EnumSet.of(REGISTERED, BOOTSTRAPPING, BOOT_REPLACING);
    private static final Set<NodeState> BOOTSTRAP_STATES = EnumSet.of(BOOTSTRAPPING, BOOT_REPLACING);

    public static boolean isPreJoin(NodeState state)
    {
        return (state == null || PRE_JOIN_STATES.contains(state));
    }

    public static boolean isBootstrap(NodeState state)
    {
        return (state != null && BOOTSTRAP_STATES.contains(state));
    }
    // TODO: probably we can make these states even more nuanced, and track which step each node is on to have a simpler representation of transition states
}
