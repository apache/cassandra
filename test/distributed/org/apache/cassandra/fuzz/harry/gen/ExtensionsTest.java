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

package org.apache.cassandra.fuzz.harry.gen;

import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generators;

public class ExtensionsTest
{

    @Test
    public void testPick()
    {
        Supplier<Integer> gen = Generators.pick(101, 102, 103, 104, 105).bind(EntropySource.forTests());

        int[] counts = new int[5];
        for (int i = 0; i < 1000; i++)
            counts[gen.get() - 101] += 1;

        // It is possible, however very improbable we won't hit each one at least once
        for (int count: counts) Assert.assertTrue(count > 0);
    }
}
