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

package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleCASTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();
    }

    @Test
    public void casOnSystemTable()
    {
        // in CASSANDRA-21112 there was a NPE caused by a missing return.  This happened when you tried to do
        // CAS on a local system table, the logic to figure out the protocol is expected to choose paxos but lacked
        // the return and instead tried to infer from TCM, but local system tables are not in TCM and not allowed
        // to be used in accord, so failed with a NPE.
        executeNet("insert into system.peers(peer, data_center) values('0.0.0.0', 'moo') if not exists");
    }
}
