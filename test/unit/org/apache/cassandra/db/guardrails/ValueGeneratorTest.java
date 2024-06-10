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

import java.util.Arrays;
import javax.annotation.Nonnull;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;
import static org.apache.cassandra.db.guardrails.ValueGenerator.GENERATOR_CLASS_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ValueGeneratorTest
{
    @Test
    public void testValueGenerator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(GENERATOR_CLASS_NAME_KEY, BooleanGenerator.class.getName());
        config.put("expecting_true", true);
        config.put("default_size", 10);
        ValueGenerator<Boolean[]> booleanGenerator = ValueGenerator.getGenerator("boolean generator", config);
        ValueValidator<Boolean[]> booleanValidator = ValueValidator.getValidator("boolean validator", config);

        Boolean[] defaultBooleans = booleanGenerator.generate(booleanValidator);
        assertNotNull(defaultBooleans);
        assertEquals(10, defaultBooleans.length);

        Boolean[] twentyBooleans = booleanGenerator.generate(20, booleanValidator);
        assertEquals(20, twentyBooleans.length);
        for (int i = 0; i < 20; i++)
            assertEquals(true, twentyBooleans[i]);
    }

    @Test
    public void testValidatorCreationWithInvalidConfiguration()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(GENERATOR_CLASS_NAME_KEY, BooleanGenerator.class.getName());

        assertThatThrownBy(() -> ValueGenerator.getGenerator("boolean generator", config))
        .isInstanceOf(ConfigurationException.class)
        .message().isEqualTo(format("Unable to create instance of generator of class %s: does not contain property 'expecting_true'", BooleanGenerator.class.getName()));
    }

    public static class BooleanGenerator extends ValueGenerator<Boolean[]>
    {
        private final boolean expectingTrue;
        private final int defaultSize;

        public BooleanGenerator(CustomGuardrailConfig config)
        {
            super(config);
            validateParameters();
            expectingTrue = (Boolean) config.get("expecting_true");
            defaultSize = (Integer) config.get("default_size");
        }

        @Override
        public Boolean[] generate(int size, ValueValidator<Boolean[]> validator)
        {
            Boolean[] booleans = new Boolean[size];
            Arrays.fill(booleans, expectingTrue);
            return booleans;
        }

        @Override
        public Boolean[] generate(ValueValidator<Boolean[]> validator)
        {
            Boolean[] booleans = new Boolean[defaultSize];
            Arrays.fill(booleans, expectingTrue);
            return booleans;
        }

        @Nonnull
        @Override
        public CustomGuardrailConfig getParameters()
        {
            return config;
        }

        @Override
        public void validateParameters() throws ConfigurationException
        {
            if (!config.containsKey("expecting_true"))
                throw new ConfigurationException("does not contain property 'expecting_true'");
        }
    }
}
