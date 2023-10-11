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

import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS;

/**
 * Provider of {@link GuardrailsConfig}s for a {@link ClientState}.
 * <p>
 * The {@link Default} implementation always retuns the {@link GuardrailsConfig} parsed from {@code cassandra.yaml},
 * but different implementations can return different configurations based on the specified {@link ClientState}.
 * <p>
 * Custom implementations can be specified at runtime with the system property {@link #CUSTOM_IMPLEMENTATION_PROPERTY}.
 * These configurations can be used to read the guardrails configuration from some other source, and provide different
 * configurations depending on the {@link ClientState} or some other factors. However, this mechanism for pluggability
 * and the related {@link GuardrailsConfig} interface are not officially supported and may change in a minor release.
 */
public interface GuardrailsConfigProvider
{
    /**
     * @deprecated CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS.getKey() must be used instead. See CASSANDRA-17797
     */
    @Deprecated(since = "5.0")
    public static final String CUSTOM_IMPLEMENTATION_PROPERTY = CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS.getKey();

    GuardrailsConfigProvider instance = CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS.getString() == null ?
                                        new Default() : build(CUSTOM_GUARDRAILS_CONFIG_PROVIDER_CLASS.getString());

    /**
     * Returns the {@link GuardrailsConfig} to be used for the specified {@link ClientState}.
     *
     * @param state a client state, maybe {@code null} if the guardrails check for which we are getting the config is
     *              for a background process that is not associated to a user query.
     * @return the configuration to be used for {@code state}
     */
    GuardrailsConfig getOrCreate(@Nullable ClientState state);

    /**
     * Creates an instance of the custom guardrails config provider of the given class.
     *
     * @param customImpl the fully qualified classname of the guardrails config provider to be instantiated
     * @return a new instance of the specified custom guardrails config provider of the given class.
     */
    static GuardrailsConfigProvider build(String customImpl)
    {
        return FBUtilities.construct(customImpl, "custom guardrails config provider");
    }

    /**
     * Default implementation of {@link GuardrailsConfigProvider} that always returns the {@link GuardrailsConfig}
     * parsed from {@code cassandra.yaml}, independently of the {@link ClientState}.
     */
    class Default implements GuardrailsConfigProvider
    {
        @Override
        public GuardrailsConfig getOrCreate(@Nullable ClientState state)
        {
            return DatabaseDescriptor.getGuardrailsConfig();
        }
    }
}
