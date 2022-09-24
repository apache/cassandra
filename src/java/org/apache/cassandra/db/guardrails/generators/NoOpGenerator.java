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

package org.apache.cassandra.db.guardrails.generators;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.ValueGenerator;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Generator which does not generate any value, it has the empty configuration which is valid.
 * Generators are meant to generate such values which would pass when tested against respective validators.
 */
public class NoOpGenerator<VALUE> extends ValueGenerator<VALUE>
{
    public NoOpGenerator()
    {
        this(new CustomGuardrailConfig());
    }

    public NoOpGenerator(CustomGuardrailConfig config)
    {
        super(config);
    }

    @Override
    public VALUE generate(int size)
    {
        return null;
    }

    @Override
    public VALUE generate()
    {
        return null;
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
    }
}
