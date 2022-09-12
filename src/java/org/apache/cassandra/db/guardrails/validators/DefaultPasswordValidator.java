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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.PasswordGuardrail;
import org.apache.cassandra.db.guardrails.ValueValidator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.mindrot.jbcrypt.BCrypt;
import org.passay.CharacterCharacteristicsRule;
import org.passay.CharacterRule;
import org.passay.HistoryRule;
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
import static org.passay.EnglishCharacterData.Special;
import static org.passay.EnglishCharacterData.UpperCase;
import static org.passay.EnglishSequenceData.Alphabetical;
import static org.passay.EnglishSequenceData.Numerical;
import static org.passay.EnglishSequenceData.USQwerty;

public class DefaultPasswordValidator extends ValueValidator<String>
{
    // default values

    private static final int DEFAULT_NUMBER_OF_CHARACTERISTICS = 3;

    private static final int DEFAULT_MIN_LENGTH_WARN = 8;
    private static final int DEFAULT_MIN_LENGTH_FAIL = 5;

    private static final int DEFAULT_MIN_UPPER_CASE_CHARS_WARN = 2;
    private static final int DEFAULT_MIN_UPPER_CASE_CHARS_FAIL = 1;

    private static final int DEFAULT_MIN_LOWER_CASE_CHARS_WARN = 2;
    private static final int DEFAULT_MIN_LOWER_CASE_CHARS_FAIL = 1;

    private static final int DEFAULT_DIGITS_CHARS_WARN = 2;
    private static final int DEFAULT_DIGITS_CHARS_FAIL = 1;

    private static final int DEFAULT_SPECIAL_CHARS_WARN = 2;
    private static final int DEFAULT_SPECIAL_CHARS_FAIL = 1;

    private static final int DEFAULT_ILLEGAL_SEQUENCE_LENGTH = 5;

    // configuration keys

    private static final String CHARACTERISTICS_KEY = "characteristics";

    private static final String MIN_LENGTH_WARN_KEY = "min_length_warn";
    private static final String MIN_LENGTH_FAIL_KEY = "min_length_fail";

    private static final String MIN_UPPER_CASE_CHARS_WARN_KEY = "min_upper_case_chars_warn";
    private static final String MIN_UPPER_CASE_CHARS_FAIL_KEY = "min_upper_case_chars_fail";

    private static final String MIN_LOWER_CASE_CHARS_WARN_KEY = "min_lower_case_chars_warn";
    private static final String MIN_LOWER_CASE_CHARS_FAIL_KEY = "min_lower_case_chars_fail";

    private static final String MIN_DIGITS_CHARS_WARN_KEY = "min_digits_chars_warn";
    private static final String MIN_DIGITS_CHARS_FAIL_KEY = "min_digits_chars_fail";

    private static final String MIN_SPECIAL_CHARS_WARN_KEY = "min_special_chars_warn";
    private static final String MIN_SPECIAL_CHARS_FAIL_KEY = "min_special_chars_fail";

    private static final String ILLEGAL_SEQUENCE_LENGTH_KEY = "illegal_sequence_length";

    private static final Set<String> VALID_PARAMETERS = ImmutableSet.of("class_name",
                                                                        CHARACTERISTICS_KEY,
                                                                        MIN_LENGTH_WARN_KEY,
                                                                        MIN_LENGTH_FAIL_KEY,
                                                                        MIN_DIGITS_CHARS_FAIL_KEY,
                                                                        MIN_UPPER_CASE_CHARS_WARN_KEY,
                                                                        MIN_UPPER_CASE_CHARS_FAIL_KEY,
                                                                        MIN_LOWER_CASE_CHARS_WARN_KEY,
                                                                        MIN_LOWER_CASE_CHARS_FAIL_KEY,
                                                                        MIN_DIGITS_CHARS_WARN_KEY,
                                                                        MIN_DIGITS_CHARS_FAIL_KEY,
                                                                        MIN_SPECIAL_CHARS_WARN_KEY,
                                                                        MIN_SPECIAL_CHARS_FAIL_KEY,
                                                                        ILLEGAL_SEQUENCE_LENGTH_KEY);

    private final int numberOfCharacteristics;

    // length
    private final int minLengthWarn;
    private final int minLengthFail;

    // upper case
    private final int minUpperCaseCharsWarn;
    private final int minUpperCaseCharsFail;

    // lower case
    private final int minLowerCaseCharsWarn;
    private final int minLowerCaseCharsFail;

    // digits
    private final int minDigitsCharsWarn;
    private final int minDigitsCharsFail;

    // special chars
    private final int minSpecialCharsWarn;
    private final int minSpecialCharsFail;

    private final int illegalSequenceLength;

    private final PasswordValidator warningValidator;
    private final PasswordValidator failingValidator;

    private final boolean historicalPasswordValidation;

    public DefaultPasswordValidator(CustomGuardrailConfig config)
    {
        super(config);

        Set<String> configKeys = new HashSet<>(config.keySet());
        configKeys.removeAll(VALID_PARAMETERS);
        configKeys.removeAll(ValueValidator.VALID_PARAMETERS);
        if (!configKeys.isEmpty())
            throw new ConfigurationException(String.format("Configuration for %s validator contains unknown keys: %s",
                                                           DefaultPasswordValidator.class.getCanonicalName(),
                                                           configKeys));

        // in the future, we will be checking historically set passwords which were created with CassandraRoleManager
        if (!(DatabaseDescriptor.getRoleManager() instanceof CassandraRoleManager))
            throw new ConfigurationException(format("To use %s, role_manager has to be an intance of %s. ",
                                                    DefaultPasswordValidator.class.getCanonicalName(),
                                                    PasswordAuthenticator.class.getCanonicalName()));

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

        numberOfCharacteristics = config.resolveInteger(CHARACTERISTICS_KEY, DEFAULT_NUMBER_OF_CHARACTERISTICS);
        illegalSequenceLength = config.resolveInteger(ILLEGAL_SEQUENCE_LENGTH_KEY, DEFAULT_ILLEGAL_SEQUENCE_LENGTH);

        historicalPasswordValidation = config.resolveBoolean(VALIDATE_AGAINST_HISTORICAL_VALUES_KEY, false);

        validateParameters();

        warningValidator = getWarningValidator();
        failingValidator = getFailingValidator();
    }

    private PasswordValidator buildValidator(int length,
                                             int characteristics,
                                             int upper,
                                             int lower,
                                             int special,
                                             int digits,
                                             int illegalSequenceLength,
                                             boolean historicalPasswordValidation)
    {
        List<Rule> rules = new ArrayList<>();

        rules.add(new LengthRule(length, Integer.MAX_VALUE));

        CharacterCharacteristicsRule characteristicsRule = new CharacterCharacteristicsRule();
        characteristicsRule.setNumberOfCharacteristics(characteristics);
        characteristicsRule.getRules().add(new CharacterRule(UpperCase, upper));
        characteristicsRule.getRules().add(new CharacterRule(LowerCase, lower));
        characteristicsRule.getRules().add(new CharacterRule(Digit, digits));
        characteristicsRule.getRules().add(new CharacterRule(Special, special));
        rules.add(characteristicsRule);

        rules.add(new WhitespaceRule());

        rules.add(new IllegalSequenceRule(Alphabetical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(Numerical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(USQwerty, illegalSequenceLength, false));

        if (historicalPasswordValidation)
            rules.add(new CassandraPasswordHistoryRule());

        return new PasswordValidator(rules);
    }

    private PasswordValidator getWarningValidator()
    {
        return buildValidator(minLengthWarn,
                              numberOfCharacteristics,
                              minUpperCaseCharsWarn,
                              minLowerCaseCharsWarn,
                              minDigitsCharsWarn,
                              minSpecialCharsWarn,
                              illegalSequenceLength,
                              historicalPasswordValidation);
    }

    private PasswordValidator getFailingValidator()
    {
        return buildValidator(minLengthFail,
                              numberOfCharacteristics,
                              minUpperCaseCharsFail,
                              minLowerCaseCharsFail,
                              minDigitsCharsFail,
                              minSpecialCharsFail,
                              illegalSequenceLength,
                              historicalPasswordValidation);
    }

    @Override
    public Optional<String> shouldWarn(@Nonnull List<String> oldValues, String newValue)
    {
        PasswordData data = new PasswordData(newValue);
        data.setPasswordReferences(getPasswordReferences(oldValues));

        RuleResult result = warningValidator.validate(data);
        return result.isValid() ? empty() : Optional.of(getValidationMessage(warningValidator, true, result));
    }

    @Override
    public Optional<String> shouldFail(@Nonnull List<String> oldValues, String newValue)
    {
        PasswordData data = new PasswordData(newValue);
        data.setPasswordReferences(getPasswordReferences(oldValues));

        RuleResult result = failingValidator.validate(data);
        return result.isValid() ? empty() : Optional.of(getValidationMessage(failingValidator, false, result));
    }

    @Override
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
            throw new ConfigurationException(String.format("Illegal sequence length can not be lower than %s.", DEFAULT_ILLEGAL_SEQUENCE_LENGTH));

        if (numberOfCharacteristics < 1 || numberOfCharacteristics > 4)
            throw new ConfigurationException("Number of characteristics has to be between [1;4].");

        int[] minimumLengthsWarn = new int[]{ minSpecialCharsWarn, minDigitsCharsWarn, minUpperCaseCharsWarn, minLowerCaseCharsWarn };
        Arrays.sort(minimumLengthsWarn);

        int minimumLenghtOfWarnCharacteristics = 0;
        for (int i = 0; i < numberOfCharacteristics; i++)
            minimumLenghtOfWarnCharacteristics += minimumLengthsWarn[0];

        if (minimumLenghtOfWarnCharacteristics > minLengthWarn)
            throw new ConfigurationException(String.format("The shortest password to pass the warning validator for any %s characteristics is %s but you have set the %s to %s.",
                                                           numberOfCharacteristics, minimumLenghtOfWarnCharacteristics, MIN_LENGTH_WARN_KEY, minLengthWarn));

        int[] minimumLengthsFail = new int[]{ minSpecialCharsFail, minDigitsCharsFail, minUpperCaseCharsFail, minLowerCaseCharsFail };
        Arrays.sort(minimumLengthsFail);

        int minimumLenghtOfFailCharacteristics = 0;
        for (int i = 0; i < numberOfCharacteristics; i++)
            minimumLenghtOfFailCharacteristics += minimumLengthsFail[0];

        if (minimumLenghtOfFailCharacteristics > minLengthFail)
            throw new ConfigurationException(String.format("The shortest password to pass the failing validator for any %s characteristics is %s but you have set the %s to %s.",
                                                           numberOfCharacteristics, minimumLenghtOfFailCharacteristics, MIN_LENGTH_FAIL_KEY, minLengthFail));
    }

    private ConfigurationException getValidationException(String key1, int value1, String key2, int value2)
    {
        return new ConfigurationException(format("%s of value %s is less or equal %s of value %s",
                                                 key1, value1,
                                                 key2, value2));
    }

    private String getValidationMessage(PasswordValidator validator, boolean forWarn, RuleResult result)
    {
        String type = forWarn ? "warning" : "error";
        StringBuilder sb = new StringBuilder();
        sb.append("Password was")
          .append(forWarn ? " set, however it might not be strong enough according to the configured password strength policy. "
                          : " not set as it violated configured password strength policy. ")
          .append("To resolve this ")
          .append(type)
          .append(", the following has to be done: ");

        for (String message : validator.getMessages(result))
            sb.append(message).append(' ');

        return sb.toString();
    }

    private List<PasswordData.Reference> getPasswordReferences(@Nonnull List<String> oldPasswords)
    {
        return oldPasswords.stream().map(PasswordData.HistoricalReference::new).collect(Collectors.toList());
    }

    private static class CassandraPasswordHistoryRule extends HistoryRule
    {
        @Override
        protected boolean matches(String password, PasswordData.Reference reference)
        {
            return BCrypt.checkpw(password, reference.getPassword());
        }
    }
}
