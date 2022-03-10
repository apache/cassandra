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

package org.apache.cassandra.index.sai.utils;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.v1.PartitionAwarePrimaryKeyFactory;

public class TokenAwarePrimaryKeyTest extends AbstractPrimaryKeyTest
{
    @Test
    public void simplePartitionTokenAwareTest() throws Throwable
    {
        PrimaryKey.Factory factory = new PartitionAwarePrimaryKeyFactory();

        PrimaryKey first = factory.createTokenOnly(makeKey(simplePartition, "1").getToken());
        PrimaryKey firstToken = factory.createTokenOnly(first.token());
        PrimaryKey second = factory.createTokenOnly(makeKey(simplePartition, "2").getToken());
        PrimaryKey secondToken = factory.createTokenOnly(second.token());

        assertCompareToAndEquals(first, second, -1);
        assertCompareToAndEquals(second, first, 1);
        assertCompareToAndEquals(first, first, 0);
        assertCompareToAndEquals(first, firstToken, 0);
        assertCompareToAndEquals(firstToken, secondToken, -1);
    }

}
