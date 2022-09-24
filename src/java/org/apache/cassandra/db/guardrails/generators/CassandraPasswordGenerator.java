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

import java.util.List;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.ValueGenerator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.passay.CharacterRule;
import org.passay.PasswordGenerator;

import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.DEFAULT_DIGITS_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.DEFAULT_MIN_LENGTH_WARN;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.DEFAULT_MIN_LOWER_CASE_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.DEFAULT_MIN_UPPER_CASE_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.DEFAULT_SPECIAL_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.MIN_DIGITS_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.MIN_LENGTH_WARN_KEY;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.MIN_LOWER_CASE_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.MIN_SPECIAL_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.CassandraPasswordValidatorConfiguration.MIN_UPPER_CASE_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.validators.CassandraPasswordValidator.getCharacterRules;

public class CassandraPasswordGenerator extends ValueGenerator<String>
{
    private final PasswordGenerator passwordGenerator;
    private final List<CharacterRule> characterRules;
    private final int minLength;

    public CassandraPasswordGenerator(CustomGuardrailConfig config)
    {
        super(config);
        minLength = config.resolveInteger(MIN_LENGTH_WARN_KEY, DEFAULT_MIN_LENGTH_WARN);
        characterRules = getCharacterRules(config.resolveInteger(MIN_UPPER_CASE_CHARS_WARN_KEY, DEFAULT_MIN_UPPER_CASE_CHARS_WARN),
                                           config.resolveInteger(MIN_LOWER_CASE_CHARS_WARN_KEY, DEFAULT_MIN_LOWER_CASE_CHARS_WARN),
                                           config.resolveInteger(MIN_DIGITS_CHARS_WARN_KEY, DEFAULT_DIGITS_CHARS_WARN),
                                           config.resolveInteger(MIN_SPECIAL_CHARS_WARN_KEY, DEFAULT_SPECIAL_CHARS_WARN));

        passwordGenerator = new PasswordGenerator();
    }

    @Override
    public String generate(int size)
    {
        return passwordGenerator.generatePassword(size, characterRules);
    }

    @Override
    public String generate()
    {
        return generate(minLength);
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        // validation of parameters was done in custom guardrail already as we validated its validator counterpart
        // and this implementation uses same configuration properties
    }
}
