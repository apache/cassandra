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

package org.apache.cassandra.utils.btree;

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class BTreeSetTest
{
    @Test
    public void toArrayRegressionTest()
    {
        BTreeSet<Integer> range = BTreeSet.of(Arrays.asList(1, 2, 3, 4)).subSet(2, true, 4, true);
        assertArrayEquals(new Object[]{ 2, 3, 4 }, range.toArray());
    }

    @Test
    public void toArrayWithOffsetUnderAllocatesRegressionTest()
    {
        BTreeSet<Integer> set = BTreeSet.of(1);
        Object[] out = set.toArray(new Object[0], 1);
        assertArrayEquals(new Object[]{ null, 1 }, out);
    }
}
