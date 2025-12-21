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

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * For test purposes, it will warn everything which has a number in it,
 * and it will fail if it is not alphanumeric.
 */
public class TestRoleNameValidator extends ValueValidator<String>
{
    public TestRoleNameValidator()
    {
        this(new CustomGuardrailConfig());
    }

    public TestRoleNameValidator(CustomGuardrailConfig config)
    {
        super(config);
    }

    @Override
    public Optional<ValidationViolation> shouldWarn(String string, boolean calledBySuperuser)
    {
        boolean containsNumber = StringUtils.containsAny(string, '1', '2', '3', '4', '5', '6', '7', '8', '9', '0');
        if (containsNumber)
            return Optional.of(new ValidationViolation("Role name contains a number."));

        return Optional.empty();
    }

    @Override
    public Optional<ValidationViolation> shouldFail(String string, boolean calledBySuperUser)
    {
        if (!StringUtils.isAlphanumeric(string))
            return Optional.of(new ValidationViolation("Role name is not alphanumeric."));

        return Optional.empty();
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {

    }

    @Nonnull
    @Override
    public CustomGuardrailConfig getParameters()
    {
        return config;
    }
}
