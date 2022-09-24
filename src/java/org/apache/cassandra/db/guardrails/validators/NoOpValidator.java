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

package org.apache.cassandra.db.guardrails.validators;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.ValueValidator;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Validator which does nothing when it validates a value. It never fails nor warns, it
 * has the empty configuration which is valid.
 */
public class NoOpValidator<T> extends ValueValidator<T>
{
    public NoOpValidator()
    {
        this(new CustomGuardrailConfig());
    }

    public NoOpValidator(CustomGuardrailConfig config)
    {
        super(config);
    }

    @Override
    public Optional<String> shouldWarn(@Nonnull List<T> oldValues, T newValue)
    {
        return Optional.empty();
    }

    @Override
    public Optional<String> shouldFail(@Nonnull List<T> oldValues, T newValue)
    {
        return Optional.empty();
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
    }
}
