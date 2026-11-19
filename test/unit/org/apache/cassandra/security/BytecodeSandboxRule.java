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

package org.apache.cassandra.security;

import org.junit.rules.ExternalResource;

import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;
import static org.junit.Assert.assertEquals;

/**
 * Selects the bytecode UDF sandbox for a test class. Use it as a static {@code @ClassRule}: it then applies before
 * any {@code @BeforeClass} method, including the ones inherited from {@code CQLTester} that initialize the server.
 * The mechanism is resolved once per JVM, and unit test classes run in their own JVM.
 */
public class BytecodeSandboxRule extends ExternalResource
{
    private WithProperties properties;

    @Override
    protected void before()
    {
        properties = new WithProperties().set(UDF_SECURITY_MECHANISM, UDFSecurityMechanism.BYTECODE.value);
        assertEquals(UDFSecurityMechanism.BYTECODE, UDFSecurityMechanism.current());
    }

    @Override
    protected void after()
    {
        properties.close();
    }
}
