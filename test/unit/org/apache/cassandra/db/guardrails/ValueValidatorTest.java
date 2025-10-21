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

import java.util.Optional;
import javax.annotation.Nonnull;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.ValueValidator.ValidationViolation;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.Boolean.FALSE;
import static java.lang.String.format;
import static org.apache.cassandra.db.guardrails.ValueValidator.CLASS_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ValueValidatorTest
{
    @Test
    public void testValidator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put("expecting_true", FALSE);

        ValueValidator<Boolean> booleanValidator = new BooleanValidator(config);

        Optional<ValidationViolation> violationResult = booleanValidator.shouldWarn(true, true);
        assertTrue(violationResult.isPresent());
        assertEquals("Expecting true is false", violationResult.get().message);

        violationResult = booleanValidator.shouldWarn(false, true);
        assertTrue(violationResult.isEmpty());

        violationResult = booleanValidator.shouldWarn(true, true);
        assertTrue(violationResult.isPresent());
        assertEquals("Expecting true is false", violationResult.get().message);
    }

    @Test
    public void testValidatorCreation()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(CLASS_NAME_KEY, BooleanValidator.class.getName());
        config.put("expecting_true", FALSE);

        ValueValidator<Boolean> booleanValidator = ValueValidator.getValidator("boolean validator", config);
        assertNotNull(booleanValidator);

        Optional<ValidationViolation> violationResult = booleanValidator.shouldWarn(true, true);
        assertTrue(violationResult.isPresent());
        assertEquals("Expecting true is false", violationResult.get().message);
    }

    @Test
    public void testValidatorCreationWithInvalidConfiguration()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(CLASS_NAME_KEY, BooleanValidator.class.getName());

        assertThatThrownBy(() -> ValueValidator.getValidator("boolean validator", config))
        .isInstanceOf(ConfigurationException.class)
        .message().isEqualTo(format("Unable to create instance of validator of class %s: does not contain property 'expecting_true'", BooleanValidator.class.getName()));
    }

    public static class BooleanValidator extends ValueValidator<Boolean>
    {
        private final boolean expectingTrue;

        public BooleanValidator(CustomGuardrailConfig config)
        {
            super(config);
            validateParameters();
            expectingTrue = (Boolean) config.get("expecting_true");
        }

        @Override
        public Optional<ValidationViolation> shouldWarn(Boolean aBoolean, boolean calledBySuperuser)
        {
            return aBoolean == expectingTrue ? Optional.empty() : Optional.of(new ValidationViolation("Expecting true is " + expectingTrue));
        }

        @Override
        public Optional<ValidationViolation> shouldFail(Boolean aBoolean, boolean calledBySuperUser)
        {
            return aBoolean == expectingTrue ? Optional.empty() : Optional.of(new ValidationViolation("Expecting true is " + expectingTrue));
        }

        @Override
        public void validateParameters() throws ConfigurationException
        {
            if (!config.containsKey("expecting_true"))
                throw new ConfigurationException("does not contain property 'expecting_true'");
        }

        @Nonnull
        @Override
        public CustomGuardrailConfig getParameters()
        {
            return config;
        }
    }
}
