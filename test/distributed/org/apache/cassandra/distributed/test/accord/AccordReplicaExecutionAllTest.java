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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;

import org.junit.BeforeClass;

/**
 * As AccordReplicaExecutionTest, but with accord.replica_execution: ALL, so that replicas also execute (single key)
 * transactions that read, such as read-modify-writes.
 *
 * @author LLM
 */
public class AccordReplicaExecutionAllTest extends AccordReplicaExecutionTest
{
    @BeforeClass
    public static void setUp() throws IOException
    {
        setUp(true, true);
    }
}
