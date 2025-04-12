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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.PasswordGenerator;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_PASSWORD_GENERATOR_ATTEMTPS;
import static org.passay.EnglishCharacterData.Digit;

public class CassandraPasswordGenerator extends ValueGenerator<String>
{
    private final PasswordGenerator passwordGenerator;
    private final List<CharacterRule> characterRules;

    protected final CassandraPasswordConfiguration configuration;
    private final int maxPasswordGenerationAttempts;

    public CassandraPasswordGenerator(CustomGuardrailConfig config)
    {
        super(config);
        configuration = new CassandraPasswordConfiguration(config);
        characterRules = getCharacterGenerationRules(configuration.upperCaseWarn,
                                                     configuration.lowerCaseWarn,
                                                     configuration.digitsWarn,
                                                     configuration.specialsWarn);

        int maxAttempts = CASSANDRA_PASSWORD_GENERATOR_ATTEMTPS.getInt();
        this.maxPasswordGenerationAttempts = Math.max(maxAttempts, 1);

        passwordGenerator = new PasswordGenerator();
    }

    @Override
    public String generate(int size, ValueValidator<String> validator)
    {
        if (size > configuration.maxLength)
            throw new ConfigurationException("Unable to generate a password of length " + size);

        boolean dictionaryAware = validator instanceof PasswordDictionaryAware;

        for (int i = 0; i < maxPasswordGenerationAttempts; i++)
        {
            String generatedPassword = passwordGenerator.generatePassword(size, characterRules);
            if (validator.shouldWarn(generatedPassword, false).isEmpty())
            {
                if (!dictionaryAware || ((PasswordDictionaryAware<?>) validator).foundInDictionary(generatedPassword).isValid())
                    return generatedPassword;
            }
        }

        throw new ConfigurationException("It was not possible to generate a valid password " +
                                         "in " + maxPasswordGenerationAttempts + " attempts. " +
                                         "Check your configuration and try again.");
    }

    @Override
    public String generate(ValueValidator<String> validator)
    {
        return generate(configuration.lengthWarn, validator);
    }

    @Nonnull
    @Override
    public CustomGuardrailConfig getParameters()
    {
        return configuration.asCustomGuardrailConfig();
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        configuration.validateParameters();
    }

    protected List<CharacterRule> getCharacterGenerationRules(int upper, int lower, int digits, int special)
    {
        return Arrays.asList(new CharacterRule(EnglishCharacterData.UpperCase, upper),
                             new CharacterRule(EnglishCharacterData.LowerCase, lower),
                             new CharacterRule(Digit, digits),
                             new CharacterRule(CassandraPasswordValidator.specialCharacters, special));
    }
}
