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
import org.junit.Test;

import org.apache.cassandra.service.accord.serializers.CommandSerializers;

import static org.junit.Assert.assertFalse;

/**
 * As AccordReplicaExecutionTest, but serializing Writes with only the keys whose writes cannot be derived
 * from the transaction definition (accord.alpha_compatibility_serialize_write_keys: false), so that every replica must derive
 * the keys it applies from its own definition, whether it executes the transaction itself or receives an Apply.
 *
 * @author LLM
 */
public class AccordReplicaExecutionReducedWriteKeysTest extends AccordReplicaExecutionTest
{
    @BeforeClass
    public static void setUp() throws IOException
    {
        setUp(false, false);
    }

    @Test
    public void reducedWriteKeysInEffect()
    {
        SHARED_CLUSTER.forEach(instance -> instance.runOnInstance(() -> assertFalse(CommandSerializers.WritesSerializer.serializeKeys())));
    }
}
