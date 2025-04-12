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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.db.guardrails.ValueValidator.ValidationViolation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;

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
    private volatile Holder<VALUE> holder;
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

    public ValueValidator<VALUE> getValidator()
    {
        maybeInitialize();
        return holder.validator;
    }

    public ValueGenerator<VALUE> getGenerator()
    {
        maybeInitialize();
        return holder.generator;
    }

    @Override
    public boolean enabled(@Nullable ClientState state)
    {
        return guardWhileSuperuser ? super.enabled(null) : super.enabled(state);
    }

    /**
     * @param value value to validate by the validator of this guardrail
     * @param state client's state
     */
    public void guard(VALUE value, ClientState state)
    {
        if (!enabled(state))
            return;

        ValueValidator<VALUE> currentValidator = getValidator();
        boolean calledBySuperuser = isCalledBySuperuser(state);
        Optional<ValidationViolation> maybeViolation = currentValidator.shouldFail(value, calledBySuperuser);

        if (maybeViolation.isPresent())
            fail(maybeViolation.get().message,
                 maybeViolation.get().redactedMessage,
                 state);
        else
            currentValidator.shouldWarn(value, calledBySuperuser).ifPresent(result -> warn(result.message, result.redactedMessage));
    }

    private boolean isCalledBySuperuser(ClientState clientState)
    {
        return clientState != null && clientState.getUser() != null && clientState.getUser().isSuper();
    }

    /**
     * @return unmodifiable view of the configuration parameters of underlying value validator.
     */
    public CustomGuardrailConfig getConfig()
    {
        return getValidator().getParameters();
    }

    /**
     * Generates a value of given size.
     *
     * @param size size of value to be generated
     * @return generated value of given size
     */
    public VALUE generate(int size)
    {
        return getGenerator().generate(size, getValidator());
    }

    /**
     * Generates a valid value.
     *
     * @return generated and valid value
     */
    public VALUE generate()
    {
        return getGenerator().generate(getValidator());
    }

    /**
     * Reconfigures this custom guardrail. After the successful finish of this method, every
     * new call to this guardrail will use new configuration for its validator.
     * <p>
     * New configuration is merged into the old one. Values for the keys in the old configuration
     * are replaced by the values of the same key in the new configuration.
     * <p>
     *
     * @param newConfig if null or the configuration is an empty map, no reconfiguration happens.
     * @throws ConfigurationException when new validator can not replace the old one or when it is not possible
     *                                to instantiate new validator or generator.
     */
    void reconfigure(@Nullable Map<String, Object> newConfig)
    {
        Map<String, Object> mergedMap = new HashMap<>(getValidator().getParameters());

        if (newConfig != null)
            mergedMap.putAll(newConfig);

        CustomGuardrailConfig config = new CustomGuardrailConfig(mergedMap);

        ValueValidator<VALUE> newValidator = ValueValidator.getValidator(name, config);
        ValueGenerator<VALUE> newGenerator = ValueGenerator.getGenerator(name, config);

        holder = new Holder<>(newValidator, newGenerator);

        logger.info("Reconfigured validator {} for guardrail '{}' with parameters {}", holder.validator.getClass(), name, holder.validator.getParameters());
        logger.info("Reconfigured generator {} for guardrail '{}' with parameters {}", holder.generator.getClass(), name, holder.generator.getParameters());
    }

    private void maybeInitialize()
    {
        if (holder == null)
            holder = new Holder<>(ValueValidator.getValidator(name, configSupplier.get()),
                                  ValueGenerator.getGenerator(name, configSupplier.get()));
    }

    private static class Holder<VALUE>
    {
        final ValueValidator<VALUE> validator;
        final ValueGenerator<VALUE> generator;

        public Holder(ValueValidator<VALUE> validator, ValueGenerator<VALUE> generator)
        {
            this.validator = validator;
            this.generator = generator;
        }
    }
}
