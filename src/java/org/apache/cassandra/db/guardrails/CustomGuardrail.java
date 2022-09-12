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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.validators.NoOpValidator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

/**
 * Custom guardrail represents a way how to validate arbitrary values. Values are validated by an instance of
 * a {@link ValueValidator}. A validator is instantiated upon node's start. If {@link Guardrails} enables it,
 * it is possible to reconfigure a custom guardrail via JMX. JMX reconfiguration
 * mechanism has to eventually call {@link CustomGuardrail#reconfigure(Map)} to achieve that.
 * <p>
 * Some custom guardrails are not meant to be reconfigurable in runtime. In that case, {@link Guardrails} should not
 * provide any way to do so.
 *
 * @param <VALUE> type of the value a validator for this guardrail validates.
 */
public class CustomGuardrail<VALUE> extends Guardrail
{
    private static final Logger logger = LoggerFactory.getLogger(CustomGuardrail.class);
    private static final ValueValidator<?> NO_OP_VALIDATOR = new NoOpValidator<>();

    private final AtomicReference<ValueValidator<VALUE>> validator;
    private final boolean guardWhileSuperuser;

    /**
     * @param name                  name of the custom guardrail
     * @param config                configuration of the custom guardrail
     * @param guardWhileSuperuser   when true, the guardrail will be executed even the caller is a superuser. If
     *                              false, this guardrail will be called only in case a caller is not a superuser.
     */
    public CustomGuardrail(String name,
                           CustomGuardrailConfig config,
                           boolean guardWhileSuperuser)
    {
        super(name);

        this.validator = new AtomicReference<>(CustomGuardrail.getValidator(name, config));
        this.guardWhileSuperuser = guardWhileSuperuser;
    }

    @Override
    public boolean enabled(@Nullable ClientState state)
    {
        return guardWhileSuperuser ? super.enabled(null) : super.enabled(state);
    }

    /**
     * @param value value to validate
     * @param state client's state
     */
    public void guard(VALUE value, ClientState state)
    {
        guard(Collections.emptyList(), value, state);
    }

    /**
     * @param oldValue previous value
     * @param newValue value to validate
     * @param state client's state
     */
    public void guard(VALUE oldValue, VALUE newValue, ClientState state)
    {
        guard(Collections.singletonList(oldValue), newValue, state);
    }

    /**
     * @param oldValues the list of previous values
     * @param newValue value to validate by the validator of this guardrail
     * @param state client's state
     */
    public void guard(@Nullable List<VALUE> oldValues, VALUE newValue, ClientState state)
    {
        if (!enabled(state))
            return;

        List<VALUE> oldValuesList = oldValues == null ? Collections.emptyList() : oldValues;

        ValueValidator<VALUE> currentValidator = validator.get();

        Optional<String> failMessage = currentValidator.shouldFail(oldValuesList, newValue);

        if (failMessage.isPresent())
            fail(failMessage.get(), state);
        else
            currentValidator.shouldWarn(oldValuesList, newValue).ifPresent(this::warn);
    }

    /**
     * Persists a state after this guardrail is invoked. This is guardrail-specific
     * as some guardrails do not need to persist anything but other do. The most typical
     * usecase would be to persist old value, so it is available upon next guardrail invocation when a new value is
     * being validated. The default implementation does not do anything. This method is meant to be called after
     * guard method has finished, either errorneously or not.
     *
     * @param state client's state
     * @param args abritrary arguments a caller can specify to use upon persistence
     */
    public void save(ClientState state, Object... args)
    {
    }

    /**
     * Returns historical values this validator successfully validated. The actual values to be returned
     * are up to implementator of a guardrail to resolve. By default, this method returns an empty, immutable list.
     *
     * @param args arguments necessary for the retrieval of historical values
     * @return historical values this validator successfully validated previously.
     */
    public List<VALUE> retrieveHistoricalValues(Object... args)
    {
        return Collections.emptyList();
    }

    /**
     * Returns true if this guardrail is supposed to take into account
     * previous values which were validated and saved by {@link CustomGuardrail#save(ClientState, Object...)} method
     * upon new validation. It is up to an implementation of a guardrail how these previous values are fetched.
     *
     * @return returns true if this guardrail is supposed to take into account
     * previous values which were validated and saved by it. Defaults to false.
     */
    public boolean isValidatingAgainstHistoricalValues()
    {
        return false;
    }

    /**
     * @return unmodifiable view of the configuration parameters of underlying value validator.
     */
    public CustomGuardrailConfig getConfig()
    {
        return validator.get().getParameters();
    }

    /**
     * Reconfigures this custom guardrail. After the successful finish of this method, every
     * new call to this guardrail will use new configuration for its validator.
     * <p>
     * New configuration is merged into the old one. Values for the keys in the old configuration
     * are replaced by the values of the same key in the new configuration.
     *
     * @param newConfig if null or the configuration is an empty map, no reconfiguration happens.
     */
    void reconfigure(@Nullable Map<String, Object> newConfig)
    {
        if (newConfig == null || newConfig.isEmpty())
            return;

        Map<String, Object> mergedMap = new HashMap<>(validator.get().getParameters());
        mergedMap.putAll(newConfig);

        ValueValidator<VALUE> newValidator = CustomGuardrail.getValidator(name, new CustomGuardrailConfig(mergedMap));
        validator.set(newValidator);
    }

    /**
     * Returns an instance of a validator according to the parameters in {@code config}.
     *
     * @param name                  name of a guardrail a validator is created for
     * @param config                configuration to instantiate a validator with. After a validator is instantiated, it will
     *                              validate this configuration internally and throws an exception if such configuration is invalid.
     * @return instance of a validator of class as specified under key {@code class_name} in {@code config}. If not set,
     * {@link NoOpValidator} will be used.
     * @throws IllegalStateException thrown in case {@code config} for the constructed validator is invalid, or it was
     *                               not possible to construct a validator.
     */
    private static <VALUE> ValueValidator<VALUE> getValidator(String name, CustomGuardrailConfig config)
    {
        String className = config.resolveString("class_name");

        if (className == null || className.isEmpty())
        {
            logger.debug("Configuration for validator for guardrail '{}' does not contain key 'class_name' or its value is null " +
                         "or empty string. No-op validator will be used.", name);
            return (ValueValidator<VALUE>) NO_OP_VALIDATOR;
        }

        if (!className.contains("."))
            className = "org.apache.cassandra.db.guardrails.validators." + className;

        try
        {
            Class<? extends ValueValidator<VALUE>> validatorClass = FBUtilities.classForName(className, "validator");

            @SuppressWarnings("unchecked")
            ValueValidator<VALUE> validator = validatorClass.getConstructor(CustomGuardrailConfig.class).newInstance(config);

            logger.info("Using {} validator for guardrail '{}' with parameters {}", validator.getClass(), name, validator.getParameters());

            return validator;
        }
        catch (ConfigurationException ex)
        {
            throw new IllegalStateException(format("Invalid parameters for validator %s: %s", className, config));
        }
        catch (Exception ex)
        {
            throw new IllegalStateException(format("Unable to create instance of validator of class %s.", className), ex);
        }
    }
}
