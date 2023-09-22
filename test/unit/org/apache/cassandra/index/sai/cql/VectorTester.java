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

package org.apache.cassandra.index.sai.cql;

import org.junit.Before;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

public class VectorTester extends SAITester
{
    @Before
    public void setup() throws Throwable
    {
        // override maxBruteForceRows to a random number between 0 and 4 so that we make sure
        // the non-brute-force path gets called during tests (which mostly involve small numbers of rows)
        var n = getRandom().nextIntBetween(0, 4);
        var ipb = InvokePointBuilder.newInvokePoint()
                                    .onClass("org.apache.cassandra.index.sai.disk.v1.VectorIndexSearcher")
                                    .onMethod("limitToTopResults")
                                    .atEntry();
        var ab = ActionBuilder.newActionBuilder()
                              .actions()
                              .doAction("$this.globalBruteForceRows = " + n);
        var changeBruteForceThreshold = Injections.newCustom("force_non_bruteforce_queries")
                                                  .add(ipb)
                                                  .add(ab)
                                                  .build();
        Injections.inject(changeBruteForceThreshold);
    }
}
