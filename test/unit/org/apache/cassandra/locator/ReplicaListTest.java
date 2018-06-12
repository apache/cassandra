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

public class ReplicaListTest extends ReplicaCollectionTest
{

    @Test
    public void removeEndpoint()
    {
        ReplicaList rlist = new ReplicaList();
        rlist.add(ReplicaUtils.full(EP1, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP2, range(0, 100)));
        rlist.add(ReplicaUtils.full(EP3, range(0, 100)));

        Assert.assertTrue(rlist.containsEndpoint(EP1));
        Assert.assertEquals(3, rlist.size());
        rlist.removeEndpoint(EP1);
        Assert.assertFalse(rlist.containsEndpoint(EP1));
        Assert.assertEquals(2, rlist.size());
    }
}
