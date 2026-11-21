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

package org.apache.cassandra.cql3.validation.entities;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;
import static org.junit.Assert.assertNotNull;

/** Runs the security and timeout tests with the explicit security manager setting. */
public class UFSecurityManagerTest extends UFSecurityTest
{
    @BeforeClass
    public static void setUpClass()
    {
        Assume.assumeTrue(Runtime.version().feature() < 24);
        UDF_SECURITY_MECHANISM.setString("securitymanager");
        CQLTester.setUpClass();
    }

    @Test
    public void securityManagerInstalled()
    {
        assertNotNull(System.getSecurityManager());
    }
}
