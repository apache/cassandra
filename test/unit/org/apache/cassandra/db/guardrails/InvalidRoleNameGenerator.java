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

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * This will generate a role name which will fail when validated with {@link TestRoleNameValidator}.
 */
public class InvalidRoleNameGenerator extends ValueGenerator<String>
{
    public InvalidRoleNameGenerator()
    {
        this(new CustomGuardrailConfig());
    }

    public InvalidRoleNameGenerator(CustomGuardrailConfig config)
    {
        super(config);
    }

    @Override
    public String generate(ValueValidator<String> validator, Map<String, Object> options)
    {
        return "non_alpha_numeric";
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
