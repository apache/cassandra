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

package org.apache.cassandra.db.guardrails;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class GuardrailMinimumCmsSizeTest
{
    private static final int DISABLED = -1;

    /**
     * Returns a proxy that invokes the Guardrails MBean through the platform MBeanServer, so the tests exercise the
     * real (local, no-network) JMX path rather than a direct method call. The MBean is registered defensively in case
     * MBean registration is disabled in the unit test JVM.
     */
    private static GuardrailsMBean guardrailsJmxProxy()
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(Guardrails.MBEAN_NAME);
            if (!mbs.isRegistered(name))
                mbs.registerMBean(Guardrails.instance, name);
            return JMX.newMBeanProxy(mbs, name, GuardrailsMBean.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidValueAcceptedTest
    {
        @Parameterized.Parameter
        public int validValue;

        @Parameterized.Parameters(name = "value={0}")
        public static Collection<Object[]> validValues()
        {
            return Arrays.asList(new Object[][]{ { DISABLED }, { 3 }, { 5 }, { 7 } });
        }

        @BeforeClass
        public static void setupClass()
        {
            DatabaseDescriptor.daemonInitialization();
        }

        @After
        public void teardown()
        {
            DatabaseDescriptor.getGuardrailsConfig().setMinimumCmsSizeFailThreshold(DISABLED);
        }

        @Test
        public void testConfigValidationValueIsAccepted()
        {
            GuardrailsOptions guardrails = DatabaseDescriptor.getGuardrailsConfig();
            guardrails.setMinimumCmsSizeFailThreshold(validValue);
            assertEquals(validValue, guardrails.getMinimumCmsSizeFailThreshold());
        }

        @Test
        public void testJmxValidationValueIsAccepted()
        {
            GuardrailsMBean jmx = guardrailsJmxProxy();
            jmx.setMinimumCmsSizeFailThreshold(validValue);
            assertEquals(validValue, jmx.getMinimumCmsSizeFailThreshold());
        }
    }

    @RunWith(Parameterized.class)
    public static class BelowMinimumRejectedTest
    {
        @Parameterized.Parameter
        public int invalidValue;

        @Parameterized.Parameters(name = "value={0}")
        public static Collection<Object[]> invalidValues()
        {
            return Arrays.asList(new Object[][]{ { 2 }, { 1 }, { 0 } });
        }

        @BeforeClass
        public static void setupClass()
        {
            DatabaseDescriptor.daemonInitialization();
        }

        @Test
        public void testConfigValidationBelowMinimumIsRejected()
        {
            GuardrailsOptions guardrails = DatabaseDescriptor.getGuardrailsConfig();
            assertThatThrownBy(() -> guardrails.setMinimumCmsSizeFailThreshold(invalidValue))
                      .isInstanceOf(IllegalArgumentException.class)
                      .hasMessageContaining("minimum allowed value is 3");
        }

        @Test
        public void testJmxValidationBelowMinimumIsRejected()
        {
            GuardrailsMBean jmx = guardrailsJmxProxy();
            // The setter throws IllegalArgumentException; the MBeanServer wraps it in RuntimeMBeanException, but the
            // JMX proxy (MBeanServerInvocationHandler) unwraps it back to the original exception.
            assertThatThrownBy(() -> jmx.setMinimumCmsSizeFailThreshold(invalidValue))
                      .isInstanceOf(IllegalArgumentException.class)
                      .hasMessageContaining("minimum allowed value is 3");
        }
    }
}
