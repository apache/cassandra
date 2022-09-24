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
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.db.guardrails.validators.NoOpValidator;
import org.apache.cassandra.service.ClientState;

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
    protected AtomicReference<ValueValidator<VALUE>> validator;
    protected AtomicReference<ValueGenerator<VALUE>> generator;
    protected final Supplier<CustomGuardrailConfig> configSupplier;
    private final boolean guardWhileSuperuser;

    /**
     * @param name                name of the custom guardrail
     * @param configSupplier      configuration supplier of the custom guardrail
     * @param guardWhileSuperuser when true, the guardrail will be executed even the caller is a superuser. If
     *                            false, this guardrail will be called only in case a caller is not a superuser.
     */
    public CustomGuardrail(String name,
                           String reason,
                           Supplier<CustomGuardrailConfig> configSupplier,
                           boolean guardWhileSuperuser)
    {
        super(name, reason);

        this.configSupplier = configSupplier;
        this.guardWhileSuperuser = guardWhileSuperuser;
    }

    private AtomicReference<ValueValidator<VALUE>> getValidator()
    {
        if (validator == null)
            validator = new AtomicReference<>(ValueValidator.getValidator(name, configSupplier.get()));

        return validator;
    }

    private AtomicReference<ValueGenerator<VALUE>> getGenerator()
    {
        if (generator == null)
            generator = new AtomicReference<>(ValueGenerator.getGenerator(name, configSupplier.get()));

        return generator;
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
     * @param state    client's state
     */
    public void guard(VALUE oldValue, VALUE newValue, ClientState state)
    {
        guard(Collections.singletonList(oldValue), newValue, state);
    }

    /**
     * @param oldValues the list of previous values
     * @param newValue  value to validate by the validator of this guardrail
     * @param state     client's state
     */
    public void guard(@Nullable List<VALUE> oldValues, VALUE newValue, ClientState state)
    {
        if (!enabled(state))
            return;

        List<VALUE> oldValuesList = oldValues == null ? Collections.emptyList() : oldValues;

        ValueValidator<VALUE> currentValidator = getValidator().get();

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
     * @param args  abritrary arguments a caller can specify to use upon persistence
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
        return getValidator().get().getParameters();
    }

    /**
     * Generates a value of given size.
     *
     * @param size size of value to be generated
     * @return generated value of given size
     */
    public VALUE generate(int size)
    {
        return getGenerator().get().generate(size);
    }

    /**
     * Generates a valid value.
     *
     * @return generated and valid value
     */
    public VALUE generate()
    {
        return getGenerator().get().generate();
    }

    /**
     * Reconfigures this custom guardrail. After the successful finish of this method, every
     * new call to this guardrail will use new configuration for its validator.
     * <p>
     * New configuration is merged into the old one. Values for the keys in the old configuration
     * are replaced by the values of the same key in the new configuration.
     *<p>
     * Reconfiguration is not allowed when key "reconfigure" for a specific guardrail in cassandra.yaml is false.
     * This property in cassandra.yaml is immutable. When {@link NoOpValidator} is used in cassandra.yml explicitly
     * or implicitly (when relevant configuration section is commented out), {@code reconfigure} key is irrelavant as
     * we can always reconfigure no-op validator, so it is replaced by a specific validator in runtime.
     *
     * @param newConfig if null or the configuration is an empty map, no reconfiguration happens.
     * @throws IllegalStateException when new validator can not replace the old one or when it is not possible
     *                               to instantiate new validator or generator.
     */
    void reconfigure(@Nullable Map<String, Object> newConfig)
    {
        if (newConfig == null || newConfig.isEmpty())
            return;

        if (!canReconfigure())
            throw new IllegalStateException(format("The new validator can not replace the old validator %s. " +
                                                   "Reconfiguration of validator %s is not allowed. If you want to " +
                                                   "reconfigure this validator, you have to change 'reconfigurable' " +
                                                   "parameter from 'false' to 'true' in the configuration section of " +
                                                   "this guardrail and restart the node. Configurations of current " +
                                                   "validator: %s.",
                                                   getValidator().get().getClass().getCanonicalName(),
                                                   name,
                                                   getValidator().get().getParameters()));

        Map<String, Object> mergedMap = new HashMap<>(getValidator().get().getParameters());
        mergedMap.putAll(newConfig);
        // put it back if we changed it in JMX (by accident or intentionally)
        // once reconfigurable - always reconfigurable
        // once not reconfigurable - never reconfigurable
        mergedMap.put(CustomGuardrailConfig.RECONFIGURABLE_KEY, validator.get().getParameters().isReconfigurable());

        CustomGuardrailConfig config = new CustomGuardrailConfig(mergedMap);

        ValueValidator<VALUE> newValidator = ValueValidator.getValidator(name, config);
        ValueGenerator<VALUE> newGenerator = ValueGenerator.getGenerator(name, config);

        getValidator().set(newValidator);
        getGenerator().set(newGenerator);
    }

    private boolean canReconfigure()
    {
        // we can always reconfigure no-op validator, regardless what "reconfigurable" is set to in cassandra.yml
        if (getValidator().get().getClass() == NoOpValidator.class)
            return true;

        return getValidator().get().getParameters().isReconfigurable();
    }
}
