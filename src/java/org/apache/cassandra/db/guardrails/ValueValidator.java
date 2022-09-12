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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class ValueValidator<VALUE>
{
    public static final String VALIDATE_AGAINST_HISTORICAL_VALUES_KEY = "validate_against_historical_values";
    public static final String MAX_HISTORICAL_VALUES_KEY = "max_historical_values";

    protected static final Set<String> VALID_PARAMETERS = ImmutableSet.of(VALIDATE_AGAINST_HISTORICAL_VALUES_KEY,
                                                                          MAX_HISTORICAL_VALUES_KEY);

    public static final int DEFAULT_MAX_HISTORICAL_VALUES = 5;

    protected final CustomGuardrailConfig config;

    public ValueValidator(CustomGuardrailConfig config)
    {
        this.config = config;
    }

    /**
     * Test a value to see if it emits warnings.
     *
     * @param value value to validate
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    public Optional<String> shouldWarn(VALUE value)
    {
        return shouldWarn(emptyList(), value);
    }

    /**
     * Test a value to see if it emits warnings.
     *
     * @param oldValue previous value
     * @param newValue value to validate
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    public Optional<String> shouldWarn(@Nullable VALUE oldValue, VALUE newValue)
    {
        return oldValue == null ? shouldWarn(newValue) : shouldWarn(singletonList(oldValue), newValue);
    }

    /**
     * Test a value to see if it emits warnings.
     *
     * @param oldValues list of previous values
     * @param newValue value to validate
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    public abstract Optional<String> shouldWarn(@Nonnull List<VALUE> oldValues, VALUE newValue);

    /**
     * Test a value to see if it emits failures.
     *
     * @param value value to validate
     * @return if optional is empty, value is valid, otherwise it returns failure violation message
     */
    public Optional<String> shouldFail(VALUE value)
    {
        return shouldFail(emptyList(), value);
    }

    /**
     * Test a value to see if it emits failures.
     *
     * @param oldValue previous value
     * @param newValue value to validate
     * @return if optional is empty, value is valid, otherwise it returns warning violation message
     */
    public Optional<String> shouldFail(@Nullable VALUE oldValue, VALUE newValue)
    {
        return oldValue == null ? shouldFail(newValue) : shouldFail(singletonList(oldValue), newValue);
    }

    /**
     * Test a value to see if it emits failures.
     *
     * @param oldValues list of previous values
     * @param newValue value to validate
     * @return if optional is empty, value is valid, otherwise it returns failure violation message
     */
    public abstract Optional<String> shouldFail(@Nonnull List<VALUE> oldValues, VALUE newValue);

    /**
     * Validates parameters for this validator.
     *
     * @throws ConfigurationException in case configuration for this specific validator is invalid
     */
    public abstract void validateParameters() throws ConfigurationException;

    /**
     * @return parameters for this validator according to which it validates
     */
    @Nonnull
    public CustomGuardrailConfig getParameters()
    {
        return new CustomGuardrailConfig(Collections.unmodifiableMap(config));
    }
}
