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

import org.junit.Test;

import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.tcm.listeners.ClientNotificationListener.ChangeType.*;
import static org.apache.cassandra.tcm.listeners.ClientNotificationListener.fromNodeStateTransition;
import static org.apache.cassandra.tcm.membership.NodeState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClientNotificationListenerTest
{
    @Test
    public void testTransitions()
    {
        assertEquals(fromNodeStateTransition(null, JOINED), JOIN);
        assertEquals(fromNodeStateTransition(REGISTERED, JOINED), JOIN);
        assertEquals(fromNodeStateTransition(BOOTSTRAPPING, JOINED), JOIN);

        for (NodeState ns : NodeState.values())
        {
            if (ns == LEFT)
                continue;
            assertEquals(ns.toString(), fromNodeStateTransition(ns, LEFT), LEAVE);
        }

        assertEquals(fromNodeStateTransition(null, LEFT), LEAVE);

        // no client notifications when leaving/moving start:
        assertNull(fromNodeStateTransition(JOINED, LEAVING));
        assertNull(fromNodeStateTransition(JOINED, MOVING));

        // no client notifications until JOINED
        assertNull(fromNodeStateTransition(null, BOOTSTRAPPING));
        assertNull(fromNodeStateTransition(null, REGISTERED));

        // if LEAVING/MOVING is the first state we see for a node, assume it was JOINED before;
        assertEquals(fromNodeStateTransition(null, LEAVING), JOIN);
        assertEquals(fromNodeStateTransition(null, MOVING), JOIN);

        assertEquals(fromNodeStateTransition(MOVING, JOINED), MOVE);
    }
}
