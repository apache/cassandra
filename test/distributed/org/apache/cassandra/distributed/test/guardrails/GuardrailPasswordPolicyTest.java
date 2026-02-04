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

package org.apache.cassandra.distributed.test.guardrails;

import java.util.Map;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import com.fasterxml.jackson.core.type.TypeReference;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.CassandraPasswordGenerator;
import org.apache.cassandra.db.guardrails.CassandraPasswordValidator;
import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.db.guardrails.Guardrails.MBEAN_NAME;
import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.utils.JsonUtils.JSON_OBJECT_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailPasswordPolicyTest extends TestBaseImpl
{
    @Test
    public void testInvalidConfigurationPreventsNodeFromStart()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("validator_class_name", CassandraPasswordValidator.class.getName());
        config.put("generator_class_name", CassandraPasswordGenerator.class.getName());
        config.put("illegal_sequence_length", -1);

        try (Cluster ignored = build(1)
                               .withConfig(c -> c.set("password_policy", config))
                               .start())
        {
            fail("should throw ConfigurationException");
        }
        catch (Exception ex)
        {
            assertEquals(ConfigurationException.class.getName(), ex.getClass().getName());
        }
    }

    @Test
    public void testDisabledReconfiguration() throws Throwable
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("validator_class_name", CassandraPasswordValidator.class.getName());
        config.put("generator_class_name", CassandraPasswordGenerator.class.getName());

        try (Cluster cluster = build(1)
                               .withConfig(c -> c.with(JMX)
                                                 .set("password_policy_reconfiguration_enabled", false)
                                                 .set("password_policy", config))
                               .start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();

            config.put("illegal_sequence_length", -1);

            mbsc.setAttribute(ObjectName.getInstance(MBEAN_NAME), new Attribute("PasswordPolicy", JSON_OBJECT_MAPPER.writeValueAsString(config)));

            assertFalse(cluster.get(1)
                               .logs()
                               .watchFor("It is not possible to reconfigure password_policy guardrail because " +
                                         "property 'password_policy_reconfiguration_enabled' is set to false.")
                               .getResult()
                               .isEmpty());
        }
    }

    @Test
    public void testEnabledReconfiguration() throws Throwable
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("validator_class_name", CassandraPasswordValidator.class.getName());
        config.put("generator_class_name", CassandraPasswordGenerator.class.getName());

        try (Cluster cluster = build(1)
                               .withConfig(c -> c.with(JMX)
                                                 .set("password_policy_reconfiguration_enabled", true)
                                                 .set("password_policy", config))
                               .start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();

            config.put("illegal_sequence_length", 2);

            try
            {
                mbsc.setAttribute(ObjectName.getInstance(MBEAN_NAME), new Attribute("PasswordPolicy", JSON_OBJECT_MAPPER.writeValueAsString(config)));
                fail();
            }
            catch (Throwable t)
            {
                assertTrue(t.getMessage() != null &&
                           t.getMessage().contains("Unable to create instance of validator of class " +
                                                   CassandraPasswordValidator.class.getName() + ": " +
                                                   "Illegal sequence length can not be lower than 3."));
            }

            config.put("illegal_sequence_length", 4);

            mbsc.setAttribute(ObjectName.getInstance(MBEAN_NAME), new Attribute("PasswordPolicy", JSON_OBJECT_MAPPER.writeValueAsString(config)));

            assertFalse(cluster.get(1)
                               .logs()
                               .watchFor("illegal_sequence_length=4")
                               .getResult()
                               .isEmpty());

            Integer illegalSequenceLength = cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>)
                                                                           () -> {
                                                                               String passwordPolicyConfig = Guardrails.instance.getPasswordPolicy();
                                                                               try
                                                                               {
                                                                                   return JSON_OBJECT_MAPPER.readTree(passwordPolicyConfig).get("illegal_sequence_length").asInt();
                                                                               }
                                                                               catch (Throwable t)
                                                                               {
                                                                                   throw new RuntimeException(t);
                                                                               }
                                                                           }).call();

            assertEquals(4, illegalSequenceLength.intValue());

            String retrievedConfig = (String) mbsc.getAttribute(ObjectName.getInstance(MBEAN_NAME), "PasswordPolicy");
            Map<String, Object> configMap = JSON_OBJECT_MAPPER.readValue(retrievedConfig, new TypeReference<>() {});
            mbsc.setAttribute(ObjectName.getInstance(MBEAN_NAME), new Attribute("PasswordPolicy", JSON_OBJECT_MAPPER.writeValueAsString(configMap)));

            assertEquals(4,
                         JSON_OBJECT_MAPPER.readTree((String) mbsc.getAttribute(ObjectName.getInstance(MBEAN_NAME), "PasswordPolicy"))
                                           .get("illegal_sequence_length").asInt());
        }
    }
}
