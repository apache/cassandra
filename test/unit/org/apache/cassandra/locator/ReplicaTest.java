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

import org.junit.Assert;
import org.junit.Test;

public class ReplicaTest
{
    @Test
    public void equalityTest() throws Exception
    {
        Replica e1Full = Replica.full(InetAddressAndPort.getByName("127.0.0.1"));
        Replica e2Full = Replica.full(InetAddressAndPort.getByName("127.0.0.1"));

        Replica e1Trans = Replica.trans(InetAddressAndPort.getByName("127.0.0.1"));
        Replica e2Trans = Replica.trans(InetAddressAndPort.getByName("127.0.0.1"));

        Assert.assertEquals(e1Full, e1Trans);
        Assert.assertEquals(e1Full, e1Full);
        Assert.assertEquals(e2Full, e2Trans);

        Assert.assertNotEquals(e1Full, e2Full);
        Assert.assertNotEquals(e1Full, e2Trans);
    }
}
