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

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.security.UDFSecurityMechanism.BYTECODE;
import static org.apache.cassandra.security.UDFSecurityMechanism.SECURITY_MANAGER;
import static org.apache.cassandra.security.UDFSecurityMechanism.resolve;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class UDFSecurityMechanismTest
{
    @Test
    public void testDefaultDependsOnJavaVersion()
    {
        assertEquals(SECURITY_MANAGER, resolve(null, 11));
        assertEquals(SECURITY_MANAGER, resolve(null, 21));
        assertEquals(SECURITY_MANAGER, resolve(null, 23));
        assertEquals(BYTECODE, resolve(null, 24));
        assertEquals(BYTECODE, resolve(null, 25));
        for (int feature : new int[]{ 11, 17, 21, 23, 24, 25 })
        {
            assertEquals(resolve(null, feature), resolve("auto", feature));
            assertEquals(resolve(null, feature), resolve(" AUTO ", feature));
        }
    }

    @Test
    public void testExplicitValues()
    {
        assertEquals(BYTECODE, resolve("bytecode", 11));
        assertEquals(BYTECODE, resolve("ByteCode", 25));
        assertEquals(SECURITY_MANAGER, resolve(" SecurityManager ", 17));
        assertEquals(SECURITY_MANAGER, resolve("securitymanager", 23));
    }

    @Test
    public void testSecurityManagerRejectedFromJava24()
    {
        for (int feature : new int[]{ 24, 25, 26 })
        {
            assertThatThrownBy(() -> resolve("securitymanager", feature))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("not supported on Java " + feature)
            .hasMessageContaining("bytecode");
        }
    }

    @Test
    public void testInvalidValues()
    {
        for (String value : new String[]{ "", " ", "both", "none", "security_manager" })
        {
            for (int feature : new int[]{ 11, 21, 25 })
            {
                assertThatThrownBy(() -> resolve(value, feature))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("cassandra.udf.security_mechanism")
                .hasMessageContaining("auto, securitymanager or bytecode");
            }
        }
    }

    @Test
    public void testInstallWhereTheSecurityManagerIsDisabled()
    {
        Runnable disabled = () -> {
            throw new UnsupportedOperationException("The Security Manager is deprecated");
        };
        assertThatThrownBy(() -> UDFSecurityMechanism.install(SECURITY_MANAGER, disabled))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("-Djava.security.manager=allow")
        .hasMessageContaining("cassandra.udf.security_mechanism=bytecode");
    }

    @Test
    public void testInstallRefusedByAnotherSecurityManager()
    {
        SecurityException refusal = new SecurityException("setSecurityManager denied");
        Runnable refused = () -> {
            throw refusal;
        };
        assertThatThrownBy(() -> UDFSecurityMechanism.install(SECURITY_MANAGER, refused))
        .isInstanceOf(ConfigurationException.class)
        .hasCause(refusal)
        .hasMessageContaining("setSecurityManager denied");
    }
}
