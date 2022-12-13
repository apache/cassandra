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

package org.apache.cassandra.simulator.test;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.simulator.paxos.PaxosSimulationRunner;

public class ShortPaxosSimulationTest
{
    @Test
    public void simulationTest() throws IOException
    {
        PaxosSimulationRunner.main(new String[] { "run", "-n", "3..6", "-t", "1000", "-c", "2", "--cluster-action-limit", "2", "-s", "30" });
    }

    @Test
    @Ignore("fails due to OOM DirectMemory - unclear why")
    public void selfReconcileTest() throws IOException
    {
        PaxosSimulationRunner.main(new String[] { "reconcile", "-n", "3..6", "-t", "1000", "-c", "2", "--cluster-action-limit", "2", "-s", "30", "--with-self" });
    }
}

