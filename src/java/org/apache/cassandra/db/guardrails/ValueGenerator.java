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

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.Map.of;

/**
 * Generates a value which respective {@link ValueValidator} successfuly validates.
 */
public abstract class ValueGenerator<VALUE>
{
    private static final Logger logger = LoggerFactory.getLogger(ValueValidator.class);

    public static final String GENERATOR_CLASS_NAME_KEY = "generator_class_name";

    private static final ValueGenerator<?> NO_OP_GENERATOR =
    new NoOpGenerator<>(new CustomGuardrailConfig(of(GENERATOR_CLASS_NAME_KEY, NoOpGenerator.class.getCanonicalName())));

    private static final String DEFAULT_VALIDATOR_IMPLEMENTATION_PACKAGE = ValueGenerator.class.getPackage().getName();

    protected final CustomGuardrailConfig config;

    public ValueGenerator(CustomGuardrailConfig config)
    {
        this.config = config;
    }

    /**
     * Generates a value of given size.
     *
     * @param size      size of value to be generated
     * @param validator validator to validate generated value with
     * @return generated value of given size
     */
    public abstract VALUE generate(int size, ValueValidator<VALUE> validator);

    /**
     * Generates a valid value.
     *
     * @param validator validator to validate generated value with
     * @return generated and valid value
     */
    public abstract VALUE generate(ValueValidator<VALUE> validator);

    /**
     * @return parameters for this generator
     */
    @Nonnull
    public abstract CustomGuardrailConfig getParameters();

    /**
     * Validates parameters for this generator.
     *
     * @throws ConfigurationException in case configuration for this generator is invalid
     */
    public abstract void validateParameters() throws ConfigurationException;

    /**
     * Returns an instance of a validator according to the parameters in {@code config}.
     *
     * @param name   name of a guardrail a generator is created for
     * @param config configuration to instantiate a generator with. After a generator is instantiated, it will
     *               validate this configuration internally and throw an exception if such configuration is invalid.
     * @return instance of a generator of class as specified under key {@code class_name} in {@code config}. If not set,
     * {@link NoOpGenerator} will be used.
     * @throws ConfigurationException thrown in case {@code config} for the constructed generator is invalid, or it was
     *                                not possible to construct a generator.
     */
    public static <VALUE> ValueGenerator<VALUE> getGenerator(String name, @Nonnull CustomGuardrailConfig config)
    {
        String className = config.resolveString(GENERATOR_CLASS_NAME_KEY);

        if (className == null || className.isEmpty())
        {
            logger.debug("Configuration for generator for guardrail '{}' does not contain key " +
                         "'generator_class_name' or its value is null or empty string. No-op generator will be used.",
                         name);
            return (ValueGenerator<VALUE>) NO_OP_GENERATOR;
        }

        if (!className.contains("."))
            className = DEFAULT_VALIDATOR_IMPLEMENTATION_PACKAGE + '.' + className;

        try
        {
            Class<? extends ValueGenerator<VALUE>> generatorClass =
            FBUtilities.classForName(className, "generator");

            @SuppressWarnings("unchecked")
            ValueGenerator<VALUE> generator = generatorClass.getConstructor(CustomGuardrailConfig.class)
                                                            .newInstance(config);
            logger.debug("Using {} generator for guardrail '{}' with parameters {}",
                         generator.getClass(), name, generator.getParameters());
            return generator;
        }
        catch (Exception ex)
        {
            String message;
            if (ex.getCause() instanceof ConfigurationException)
                message = ex.getCause().getMessage();
            else
                message = ex.getMessage();

            throw new ConfigurationException(format("Unable to create instance of generator of class %s: %s",
                                                    className, message), ex);
        }
    }
}