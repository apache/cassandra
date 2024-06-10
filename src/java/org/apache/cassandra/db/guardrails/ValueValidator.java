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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.Map.of;

/**
 * Validates a value by calling {@link ValueValidator#shouldFail} or {@link ValueValidator#shouldWarn} methods.
 * These methods are called from {@link CustomGuardrail} and CQL request either emits a failure when a value is invalid
 * or a warning when it is still not valid, but it validates against the least strict validation.
 *
 * @param <VALUE> type parameter of a value this validator validates.
 */
public abstract class ValueValidator<VALUE>
{
    private static final Logger logger = LoggerFactory.getLogger(ValueValidator.class);

    public static final String CLASS_NAME_KEY = "class_name";

    private static final ValueValidator<?> NO_OP_VALIDATOR =
    new NoOpValidator<>(new CustomGuardrailConfig(of(CLASS_NAME_KEY, NoOpValidator.class.getCanonicalName())));

    private static final String DEFAULT_VALIDATOR_IMPLEMENTATION_PACKAGE = ValueValidator.class.getPackage().getName();

    protected final CustomGuardrailConfig config;

    public ValueValidator(CustomGuardrailConfig config)
    {
        this.config = config;
    }

    public static class ValidationViolation
    {
        public final String message;
        public final String redactedMessage;

        public ValidationViolation(String message)
        {
            this(message, message);
        }

        public ValidationViolation(String message, String redactedMessage)
        {
            this.message = message;
            this.redactedMessage = redactedMessage;
        }
    }

    /**
     * Test a value to see if it emits warnings.
     *
     * @param value             value to validate
     * @param calledBySuperuser client state
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    public abstract Optional<ValidationViolation> shouldWarn(VALUE value, boolean calledBySuperuser);

    /**
     * Test a value to see if it emits failures.
     *
     * @param value             value to validate
     * @param calledBySuperUser whether this is called by a super-user or not
     * @return if optional is empty, value is valid, otherwise it returns failure violation message
     */
    public abstract Optional<ValidationViolation> shouldFail(VALUE value, boolean calledBySuperUser);

    /**
     * Validates parameters for this validator.
     *
     * @throws ConfigurationException in case configuration for this validator is invalid
     */
    public abstract void validateParameters() throws ConfigurationException;

    /**
     * @return parameters for this validator
     */
    @Nonnull
    public abstract CustomGuardrailConfig getParameters();

    /**
     * Returns an instance of a validator according to the parameters in {@code config}.
     *
     * @param name   name of a guardrail a validator is created for
     * @param config configuration to instantiate a validator with. After a validator is instantiated, it will
     *               validate this configuration internally and throw an exception if such configuration is invalid.
     * @return instance of a validator of class as specified under key {@code class_name} in {@code config}. If not set,
     * {@link NoOpValidator} will be used.
     * @throws ConfigurationException thrown in case {@code config} for the constructed validator is invalid, or it was
     *                                not possible to construct a validator.
     */
    public static <VALUE> ValueValidator<VALUE> getValidator(String name, @Nonnull CustomGuardrailConfig config)
    {
        String className = config.resolveString(CLASS_NAME_KEY);

        if (className == null || className.isEmpty())
        {
            logger.debug("Configuration for validator for guardrail '{}' does not contain key " +
                         "'class_name' or its value is null or empty string. No-op validator will be used.", name);
            return (ValueValidator<VALUE>) NO_OP_VALIDATOR;
        }

        if (!className.contains("."))
            className = DEFAULT_VALIDATOR_IMPLEMENTATION_PACKAGE + '.' + className;

        try
        {
            Class<? extends ValueValidator<VALUE>> validatorClass =
            FBUtilities.classForName(className, "validator");

            @SuppressWarnings("unchecked")
            ValueValidator<VALUE> validator = validatorClass.getConstructor(CustomGuardrailConfig.class)
                                                            .newInstance(config);
            logger.debug("Using {} validator for guardrail '{}' with parameters {}",
                         validator.getClass(), name, validator.getParameters());
            return validator;
        }
        catch (Exception ex)
        {
            String message;
            if (ex.getCause() instanceof ConfigurationException)
                message = ex.getCause().getMessage();
            else
                message = ex.getMessage();

            throw new ConfigurationException(format("Unable to create instance of validator of class %s: %s",
                                                    className, message), ex);
        }
    }
}
