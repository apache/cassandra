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

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Validator which does nothing when it validates a value. It never fails nor warns, it
 * has the empty configuration which is valid.
 */
public class NoOpValidator<T> extends ValueValidator<T>
{
    private static final CustomGuardrailConfig config = new CustomGuardrailConfig();

    public NoOpValidator(CustomGuardrailConfig unused)
    {
        super(NoOpValidator.config);
    }

    @Override
    public Optional<ValidationViolation> shouldWarn(T value, boolean calledBySuperuser)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ValidationViolation> shouldFail(T value, boolean calledBySuperUser)
    {
        return Optional.empty();
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
    }
}
