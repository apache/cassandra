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

package org.apache.cassandra.index.sai.cql.intersection;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.cassandra.index.sai.cql.intersection.RandomIntersectionTester.Mode.REGULAR_STATIC;

@RunWith(Parameterized.class)
public class RandomMixedIntersectionTest extends RandomIntersectionTester
{
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new LinkedList<>();

        parameters.add(new Object[] { "Large partition unrestricted, high, high", false, true, true, true, REGULAR_STATIC});
        parameters.add(new Object[] { "Large partition unrestricted, low, low", false, true, false, false, REGULAR_STATIC});
        parameters.add(new Object[] { "Large partition unrestricted, high, low", false, true, true, false, REGULAR_STATIC});
        parameters.add(new Object[] { "Small partition unrestricted, high, high", false, false, true, true, REGULAR_STATIC});
        parameters.add(new Object[] { "Small partition unrestricted, low, low", false, false, false, false, REGULAR_STATIC});
        parameters.add(new Object[] { "Small partition unrestricted, high, low", false, false, true, false, REGULAR_STATIC});

        return parameters;
    }

    @Test
    public void testMixedIntersection() throws Throwable
    {
        runUnrestrictedQueries();
    }
}
