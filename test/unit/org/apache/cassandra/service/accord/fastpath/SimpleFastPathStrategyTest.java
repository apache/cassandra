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

package org.apache.cassandra.service.accord.fastpath;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import accord.local.Node;

import static org.apache.cassandra.service.accord.AccordTestUtils.idList;
import static org.apache.cassandra.service.accord.AccordTestUtils.idSet;

public class SimpleFastPathStrategyTest
{
    private static final Map<Node.Id, String> DCMAP = Collections.emptyMap();

    @Test
    public void testCalculation()
    {
        FastPathStrategy strategy = SimpleFastPathStrategy.instance;
        Assert.assertEquals(idSet(1, 2, 3, 4, 5), strategy.calculateFastPath(idList(1, 2, 3, 4, 5), idSet(), DCMAP));
        Assert.assertEquals(idSet(3, 4, 5), strategy.calculateFastPath(idList(1, 2, 3, 4, 5), idSet(1, 2, 3), DCMAP));
    }
}
