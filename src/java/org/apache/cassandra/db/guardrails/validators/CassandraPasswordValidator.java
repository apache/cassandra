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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.ValueValidator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.mindrot.jbcrypt.BCrypt;
import org.passay.CharacterCharacteristicsRule;
import org.passay.CharacterData;
import org.passay.CharacterRule;
import org.passay.IllegalSequenceRule;
import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.PasswordValidator;
import org.passay.Rule;
import org.passay.RuleResult;
import org.passay.WhitespaceRule;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static org.passay.EnglishCharacterData.Digit;
import static org.passay.EnglishCharacterData.LowerCase;
import static org.passay.EnglishCharacterData.UpperCase;
import static org.passay.EnglishSequenceData.Alphabetical;
import static org.passay.EnglishSequenceData.Numerical;
import static org.passay.EnglishSequenceData.USQwerty;

public class CassandraPasswordValidator extends ValueValidator<String>
{
    // default implementation also contains non-ascii characters, we do not want them
    protected static final CharacterData specialCharacters = new CharacterData()
    {
        @Override
        public String getErrorCode()
        {
            return "INSUFFICIENT_SPECIAL";
        }

        @Override
        public String getCharacters()
        {
            return "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
        }
    };

    public static class CassandraPasswordValidatorConfiguration
    {
        // default values
        public static final int DEFAULT_MIN_CHARACTERISTICS_WARN = 3;
        public static final int DEFAULT_MIN_CHARACTERISTICS_FAIL = 2;

        public static final int DEFAULT_MIN_LENGTH_WARN = 12;
        public static final int DEFAULT_MIN_LENGTH_FAIL = 8;

        public static final int DEFAULT_MIN_UPPER_CASE_CHARS_WARN = 2;
        public static final int DEFAULT_MIN_UPPER_CASE_CHARS_FAIL = 1;

        public static final int DEFAULT_MIN_LOWER_CASE_CHARS_WARN = 2;
        public static final int DEFAULT_MIN_LOWER_CASE_CHARS_FAIL = 1;

        public static final int DEFAULT_DIGITS_CHARS_WARN = 2;
        public static final int DEFAULT_DIGITS_CHARS_FAIL = 1;

        public static final int DEFAULT_SPECIAL_CHARS_WARN = 2;
        public static final int DEFAULT_SPECIAL_CHARS_FAIL = 1;

        public static final int DEFAULT_ILLEGAL_SEQUENCE_LENGTH = 5;

        // configuration keys
        public static final String MIN_CHARACTERISTICS_WARN_KEY = "min_characteristics_warn";
        public static final String MIN_CHARACTERISTICS_FAIL_KEY = "min_characteristics_fail";

        public static final String MIN_LENGTH_WARN_KEY = "min_length_warn";
        public static final String MIN_LENGTH_FAIL_KEY = "min_length_fail";

        public static final String MIN_UPPER_CASE_CHARS_WARN_KEY = "min_upper_case_chars_warn";
        public static final String MIN_UPPER_CASE_CHARS_FAIL_KEY = "min_upper_case_chars_fail";

        public static final String MIN_LOWER_CASE_CHARS_WARN_KEY = "min_lower_case_chars_warn";
        public static final String MIN_LOWER_CASE_CHARS_FAIL_KEY = "min_lower_case_chars_fail";

        public static final String MIN_DIGITS_CHARS_WARN_KEY = "min_digits_chars_warn";
        public static final String MIN_DIGITS_CHARS_FAIL_KEY = "min_digits_chars_fail";

        public static final String MIN_SPECIAL_CHARS_WARN_KEY = "min_special_chars_warn";
        public static final String MIN_SPECIAL_CHARS_FAIL_KEY = "min_special_chars_fail";

        public static final String ILLEGAL_SEQUENCE_LENGTH_KEY = "illegal_sequence_length";

        // characteristics
        protected final int minCharacteristicsWarn;
        protected final int minCharacteristicsFail;

        // length
        protected final int minLengthWarn;
        protected final int minLengthFail;

        // upper case
        protected final int minUpperCaseCharsWarn;
        protected final int minUpperCaseCharsFail;

        // lower case
        protected final int minLowerCaseCharsWarn;
        protected final int minLowerCaseCharsFail;

        // digits
        protected final int minDigitsCharsWarn;
        protected final int minDigitsCharsFail;

        // special chars
        protected final int minSpecialCharsWarn;
        protected final int minSpecialCharsFail;

        protected final int illegalSequenceLength;

        protected final boolean historicalPasswordValidation;

        public CustomGuardrailConfig asCustomGuardrailConfig()
        {
            CustomGuardrailConfig config = new CustomGuardrailConfig();
            config.put(MIN_CHARACTERISTICS_WARN_KEY, minCharacteristicsWarn);
            config.put(MIN_CHARACTERISTICS_FAIL_KEY, minCharacteristicsFail);
            config.put(MIN_LENGTH_WARN_KEY, minLengthWarn);
            config.put(MIN_LENGTH_FAIL_KEY, minLengthFail);
            config.put(MIN_UPPER_CASE_CHARS_WARN_KEY, minUpperCaseCharsWarn);
            config.put(MIN_UPPER_CASE_CHARS_FAIL_KEY, minUpperCaseCharsFail);
            config.put(MIN_LOWER_CASE_CHARS_WARN_KEY, minLowerCaseCharsWarn);
            config.put(MIN_LOWER_CASE_CHARS_FAIL_KEY, minLowerCaseCharsFail);
            config.put(MIN_DIGITS_CHARS_WARN_KEY, minDigitsCharsWarn);
            config.put(MIN_DIGITS_CHARS_FAIL_KEY, minDigitsCharsFail);
            config.put(MIN_SPECIAL_CHARS_WARN_KEY, minSpecialCharsWarn);
            config.put(MIN_SPECIAL_CHARS_FAIL_KEY, minSpecialCharsFail);
            config.put(ILLEGAL_SEQUENCE_LENGTH_KEY, illegalSequenceLength);
            config.put(VALIDATE_AGAINST_HISTORICAL_VALUES_KEY, historicalPasswordValidation);

            return config;
        }

        private CassandraPasswordValidatorConfiguration(CustomGuardrailConfig config)
        {
            minCharacteristicsWarn = config.resolveInteger(MIN_CHARACTERISTICS_WARN_KEY, DEFAULT_MIN_CHARACTERISTICS_WARN);
            minCharacteristicsFail = config.resolveInteger(MIN_CHARACTERISTICS_FAIL_KEY, DEFAULT_MIN_CHARACTERISTICS_FAIL);

            minLengthWarn = config.resolveInteger(MIN_LENGTH_WARN_KEY, DEFAULT_MIN_LENGTH_WARN);
            minLengthFail = config.resolveInteger(MIN_LENGTH_FAIL_KEY, DEFAULT_MIN_LENGTH_FAIL);

            minUpperCaseCharsWarn = config.resolveInteger(MIN_UPPER_CASE_CHARS_WARN_KEY, DEFAULT_MIN_UPPER_CASE_CHARS_WARN);
            minUpperCaseCharsFail = config.resolveInteger(MIN_UPPER_CASE_CHARS_FAIL_KEY, DEFAULT_MIN_UPPER_CASE_CHARS_FAIL);

            minLowerCaseCharsWarn = config.resolveInteger(MIN_LOWER_CASE_CHARS_WARN_KEY, DEFAULT_MIN_LOWER_CASE_CHARS_WARN);
            minLowerCaseCharsFail = config.resolveInteger(MIN_LOWER_CASE_CHARS_FAIL_KEY, DEFAULT_MIN_LOWER_CASE_CHARS_FAIL);

            minDigitsCharsWarn = config.resolveInteger(MIN_DIGITS_CHARS_WARN_KEY, DEFAULT_DIGITS_CHARS_WARN);
            minDigitsCharsFail = config.resolveInteger(MIN_DIGITS_CHARS_FAIL_KEY, DEFAULT_DIGITS_CHARS_FAIL);

            minSpecialCharsWarn = config.resolveInteger(MIN_SPECIAL_CHARS_WARN_KEY, DEFAULT_SPECIAL_CHARS_WARN);
            minSpecialCharsFail = config.resolveInteger(MIN_SPECIAL_CHARS_FAIL_KEY, DEFAULT_SPECIAL_CHARS_FAIL);

            illegalSequenceLength = config.resolveInteger(ILLEGAL_SEQUENCE_LENGTH_KEY, DEFAULT_ILLEGAL_SEQUENCE_LENGTH);
            historicalPasswordValidation = config.resolveBoolean(VALIDATE_AGAINST_HISTORICAL_VALUES_KEY, false);

            validateParameters();
        }

        public void validateParameters() throws ConfigurationException
        {
            if (minLengthWarn <= minLengthFail)
                throw getValidationException(MIN_LENGTH_WARN_KEY, minLengthWarn, MIN_LENGTH_FAIL_KEY, minLengthFail);

            if (minSpecialCharsWarn <= minSpecialCharsFail)
                throw getValidationException(MIN_SPECIAL_CHARS_WARN_KEY, minSpecialCharsWarn, MIN_SPECIAL_CHARS_FAIL_KEY, minSpecialCharsFail);

            if (minDigitsCharsWarn <= minDigitsCharsFail)
                throw getValidationException(MIN_DIGITS_CHARS_WARN_KEY, minDigitsCharsWarn, MIN_DIGITS_CHARS_FAIL_KEY, minDigitsCharsFail);

            if (minUpperCaseCharsWarn <= minUpperCaseCharsFail)
                throw getValidationException(MIN_UPPER_CASE_CHARS_WARN_KEY, minUpperCaseCharsWarn, MIN_UPPER_CASE_CHARS_FAIL_KEY, minUpperCaseCharsFail);

            if (minLowerCaseCharsWarn <= minLowerCaseCharsFail)
                throw getValidationException(MIN_LOWER_CASE_CHARS_WARN_KEY, minLowerCaseCharsWarn, MIN_LOWER_CASE_CHARS_FAIL_KEY, minLowerCaseCharsFail);

            if (illegalSequenceLength < DEFAULT_ILLEGAL_SEQUENCE_LENGTH)
                throw new ConfigurationException(format("Illegal sequence length can not be lower than %s.", DEFAULT_ILLEGAL_SEQUENCE_LENGTH));

            if (minCharacteristicsWarn > 4)
                throw new ConfigurationException(format("%s can not be bigger than 4", MIN_CHARACTERISTICS_WARN_KEY));
            if (minCharacteristicsFail > 4)
                throw new ConfigurationException(format("%s can not be bigger than 4", MIN_CHARACTERISTICS_FAIL_KEY));
            if (minCharacteristicsFail == minCharacteristicsWarn)
                throw new ConfigurationException(format("%s can not be equal to %s. You set %s and %s respectively.",
                                                        MIN_CHARACTERISTICS_FAIL_KEY, MIN_CHARACTERISTICS_WARN_KEY,
                                                        minCharacteristicsFail, minCharacteristicsWarn));
            if (minCharacteristicsFail > minCharacteristicsWarn)
                throw new ConfigurationException(format("%s can not be bigger than %s. You have set %s and %s respectively.",
                                                        MIN_CHARACTERISTICS_FAIL_KEY, MIN_CHARACTERISTICS_WARN_KEY,
                                                        minCharacteristicsFail, minCharacteristicsWarn));

            int[] minimumLengthsWarn = new int[]{ minSpecialCharsWarn, minDigitsCharsWarn, minUpperCaseCharsWarn, minLowerCaseCharsWarn };
            Arrays.sort(minimumLengthsWarn);

            int minimumLenghtOfWarnCharacteristics = 0;
            for (int i = 0; i < minCharacteristicsWarn; i++)
                minimumLenghtOfWarnCharacteristics += minimumLengthsWarn[0];

            if (minimumLenghtOfWarnCharacteristics > minLengthWarn)
                throw new ConfigurationException(format("The shortest password to pass the warning validator for any %s characteristics out of 4 is %s but you have set the %s to %s.",
                                                        minCharacteristicsWarn, minimumLenghtOfWarnCharacteristics, MIN_LENGTH_WARN_KEY, minLengthWarn));

            int[] minimumLengthsFail = new int[]{ minSpecialCharsFail, minDigitsCharsFail, minUpperCaseCharsFail, minLowerCaseCharsFail };
            Arrays.sort(minimumLengthsFail);

            int minimumLenghtOfFailCharacteristics = 0;
            for (int i = 0; i < minCharacteristicsFail; i++)
                minimumLenghtOfFailCharacteristics += minimumLengthsFail[0];

            if (minimumLenghtOfFailCharacteristics > minLengthFail)
                throw new ConfigurationException(format("The shortest password to pass the failing validator for any %s characteristics out of 4 is %s but you have set the %s to %s.",
                                                        minCharacteristicsFail, minimumLenghtOfFailCharacteristics, MIN_LENGTH_FAIL_KEY, minLengthFail));
        }

        private ConfigurationException getValidationException(String key1, int value1, String key2, int value2)
        {
            return new ConfigurationException(format("%s of value %s is less or equal to %s of value %s",
                                                     key1, value1,
                                                     key2, value2));
        }


        public static CassandraPasswordValidatorConfiguration parse(CustomGuardrailConfig config)
        {
            return new CassandraPasswordValidatorConfiguration(config);
        }
    }

    protected final PasswordValidator warningValidator;
    protected final PasswordValidator failingValidator;

    private final CassandraPasswordValidatorConfiguration configuration;

    public CassandraPasswordValidator(CustomGuardrailConfig config)
    {
        super(config);

        configuration = CassandraPasswordValidatorConfiguration.parse(config);

        warningValidator = new PasswordValidator(getRules(configuration.minLengthWarn,
                                                          configuration.minCharacteristicsWarn,
                                                          configuration.illegalSequenceLength,
                                                          getCharacterRules(configuration.minUpperCaseCharsWarn,
                                                                            configuration.minLowerCaseCharsWarn,
                                                                            configuration.minDigitsCharsWarn,
                                                                            configuration.minSpecialCharsWarn)));
        failingValidator = new PasswordValidator(getRules(configuration.minLengthFail,
                                                          configuration.minCharacteristicsFail,
                                                          configuration.illegalSequenceLength,
                                                          getCharacterRules(configuration.minUpperCaseCharsFail,
                                                                            configuration.minLowerCaseCharsFail,
                                                                            configuration.minDigitsCharsFail,
                                                                            configuration.minSpecialCharsFail)));
    }

    @Override
    public Optional<String> shouldWarn(@Nonnull List<String> oldValues, String newValue)
    {
        RuleResult result = warningValidator.validate(new PasswordData(newValue));
        return result.isValid() ? empty() : Optional.of(getValidationMessage(warningValidator, true, result));
    }

    @Override
    public Optional<String> shouldFail(@Nonnull List<String> oldValues, String newValue)
    {
        if (configuration.historicalPasswordValidation && !oldValues.isEmpty())
            if (oldValues.stream().anyMatch(oldHash -> newValue.equals(oldHash) || BCrypt.checkpw(newValue, oldHash)))
            {
                String message = getValidationMessage(false, Set.of(format("Password matches one of %1$s previous passwords.", oldValues.size())));
                return Optional.of(message);
            }

        RuleResult result = failingValidator.validate(new PasswordData(newValue));
        return result.isValid() ? empty() : Optional.of(getValidationMessage(failingValidator, false, result));
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        configuration.validateParameters();
    }

    public static List<CharacterRule> getCharacterRules(int upper, int lower, int digits, int special)
    {
        return Arrays.asList(new CharacterRule(UpperCase, upper),
                             new CharacterRule(LowerCase, lower),
                             new CharacterRule(Digit, digits),
                             new CharacterRule(specialCharacters, special));
    }

    private List<Rule> getRules(int length,
                                int characteristics,
                                int illegalSequenceLength,
                                List<CharacterRule> characterRules)
    {
        List<Rule> rules = new ArrayList<>();

        rules.add(new LengthRule(length, Integer.MAX_VALUE));

        CharacterCharacteristicsRule characteristicsRule = new CharacterCharacteristicsRule();
        characteristicsRule.setNumberOfCharacteristics(characteristics);
        characteristicsRule.getRules().addAll(characterRules);
        rules.add(characteristicsRule);

        rules.add(new WhitespaceRule());

        rules.add(new IllegalSequenceRule(Alphabetical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(Numerical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(USQwerty, illegalSequenceLength, false));

        return rules;
    }

    private String getValidationMessage(boolean forWarn, Collection<String> messages)
    {
        String type = forWarn ? "warning" : "error";
        StringBuilder sb = new StringBuilder();
        sb.append("Password was")
          .append(forWarn ? " set, however it might not be strong enough according to the configured password strength policy. "
                          : " not set as it violated configured password strength policy. ")
          .append("To fix this ")
          .append(type)
          .append(", the following has to be resolved: ");

        for (String message : messages)
            sb.append(message).append(' ');

        return sb.toString();
    }

    private String getValidationMessage(PasswordValidator validator, boolean forWarn, RuleResult result)
    {
        return getValidationMessage(forWarn, validator.getMessages(result));
    }
}
