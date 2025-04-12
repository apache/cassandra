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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.passay.IllegalSequenceRule;

import static java.lang.String.format;

public class CassandraPasswordConfiguration
{
    public static final int MAX_CHARACTERISTICS = 4;

    // default values
    public static final int DEFAULT_CHARACTERISTIC_WARN = 3;
    public static final int DEFAULT_CHARACTERISTIC_FAIL = 2;

    public static final int DEFAULT_MAX_LENGTH = 1000;
    public static final int DEFAULT_LENGTH_WARN = 12;
    public static final int DEFAULT_LENGTH_FAIL = 8;

    public static final int DEFAULT_UPPER_CASE_WARN = 2;
    public static final int DEFAULT_UPPER_CASE_FAIL = 1;

    public static final int DEFAULT_LOWER_CASE_WARN = 2;
    public static final int DEFAULT_LOWER_CASE_FAIL = 1;

    public static final int DEFAULT_DIGIT_WARN = 2;
    public static final int DEFAULT_DIGIT_FAIL = 1;

    public static final int DEFAULT_SPECIAL_WARN = 2;
    public static final int DEFAULT_SPECIAL_FAIL = 1;

    public static final int DEFAULT_ILLEGAL_SEQUENCE_LENGTH = 5;

    public static final String CHARACTERISTIC_WARN_KEY = "characteristic_warn";
    public static final String CHARACTERISTIC_FAIL_KEY = "characteristic_fail";

    public static final String MAX_LENGTH_KEY = "max_length";
    public static final String LENGTH_WARN_KEY = "length_warn";
    public static final String LENGTH_FAIL_KEY = "length_fail";

    public static final String UPPER_CASE_WARN_KEY = "upper_case_warn";
    public static final String UPPER_CASE_FAIL_KEY = "upper_case_fail";

    public static final String LOWER_CASE_WARN_KEY = "lower_case_warn";
    public static final String LOWER_CASE_FAIL_KEY = "lower_case_fail";

    public static final String DIGIT_WARN_KEY = "digit_warn";
    public static final String DIGIT_FAIL_KEY = "digit_fail";

    public static final String SPECIAL_WARN_KEY = "special_warn";
    public static final String SPECIAL_FAIL_KEY = "special_fail";

    public static final String ILLEGAL_SEQUENCE_LENGTH_KEY = "illegal_sequence_length";
    public static final String DICTIONARY_KEY = "dictionary";

    public static final String DETAILED_MESSAGES_KEY = "detailed_messages";

    protected final int characteristicsWarn;
    protected final int characteristicsFail;

    protected final int maxLength;
    protected final int lengthWarn;
    protected final int lengthFail;

    protected final int upperCaseWarn;
    protected final int upperCaseFail;

    protected final int lowerCaseWarn;
    protected final int lowerCaseFail;

    protected final int digitsWarn;
    protected final int digitsFail;

    protected final int specialsWarn;
    protected final int specialsFail;

    // various
    protected final int illegalSequenceLength;
    protected final String dictionary;

    public boolean detailedMessages;

    public CustomGuardrailConfig asCustomGuardrailConfig()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(CHARACTERISTIC_WARN_KEY, characteristicsWarn);
        config.put(CHARACTERISTIC_FAIL_KEY, characteristicsFail);
        config.put(MAX_LENGTH_KEY, maxLength);
        config.put(LENGTH_WARN_KEY, lengthWarn);
        config.put(LENGTH_FAIL_KEY, lengthFail);
        config.put(UPPER_CASE_WARN_KEY, upperCaseWarn);
        config.put(UPPER_CASE_FAIL_KEY, upperCaseFail);
        config.put(LOWER_CASE_WARN_KEY, lowerCaseWarn);
        config.put(LOWER_CASE_FAIL_KEY, lowerCaseFail);
        config.put(DIGIT_WARN_KEY, digitsWarn);
        config.put(DIGIT_FAIL_KEY, digitsFail);
        config.put(SPECIAL_WARN_KEY, specialsWarn);
        config.put(SPECIAL_FAIL_KEY, specialsFail);
        config.put(ILLEGAL_SEQUENCE_LENGTH_KEY, illegalSequenceLength);
        config.put(DETAILED_MESSAGES_KEY, detailedMessages);
        config.put(DICTIONARY_KEY, dictionary);

        return config;
    }

    public CassandraPasswordConfiguration(CustomGuardrailConfig config)
    {
        characteristicsWarn = config.resolveInteger(CHARACTERISTIC_WARN_KEY, DEFAULT_CHARACTERISTIC_WARN);
        characteristicsFail = config.resolveInteger(CHARACTERISTIC_FAIL_KEY, DEFAULT_CHARACTERISTIC_FAIL);

        maxLength = config.resolveInteger(MAX_LENGTH_KEY, DEFAULT_MAX_LENGTH);
        lengthWarn = config.resolveInteger(LENGTH_WARN_KEY, DEFAULT_LENGTH_WARN);
        lengthFail = config.resolveInteger(LENGTH_FAIL_KEY, DEFAULT_LENGTH_FAIL);

        upperCaseWarn = config.resolveInteger(UPPER_CASE_WARN_KEY, DEFAULT_UPPER_CASE_WARN);
        upperCaseFail = config.resolveInteger(UPPER_CASE_FAIL_KEY, DEFAULT_UPPER_CASE_FAIL);

        lowerCaseWarn = config.resolveInteger(LOWER_CASE_WARN_KEY, DEFAULT_LOWER_CASE_WARN);
        lowerCaseFail = config.resolveInteger(LOWER_CASE_FAIL_KEY, DEFAULT_LOWER_CASE_FAIL);

        digitsWarn = config.resolveInteger(DIGIT_WARN_KEY, DEFAULT_DIGIT_WARN);
        digitsFail = config.resolveInteger(DIGIT_FAIL_KEY, DEFAULT_DIGIT_FAIL);

        specialsWarn = config.resolveInteger(SPECIAL_WARN_KEY, DEFAULT_SPECIAL_WARN);
        specialsFail = config.resolveInteger(SPECIAL_FAIL_KEY, DEFAULT_SPECIAL_FAIL);

        illegalSequenceLength = config.resolveInteger(ILLEGAL_SEQUENCE_LENGTH_KEY, DEFAULT_ILLEGAL_SEQUENCE_LENGTH);
        dictionary = config.resolveString(DICTIONARY_KEY);
        detailedMessages = config.resolveBoolean(DETAILED_MESSAGES_KEY, true);

        validateParameters();
    }

    ConfigurationException mustBePositiveException(String parameter)
    {
        throw new ConfigurationException(parameter + " must be positive.");
    }

    public void validateParameters() throws ConfigurationException
    {
        if (maxLength < 0) throw mustBePositiveException(MAX_LENGTH_KEY);
        if (characteristicsWarn < 0) throw mustBePositiveException(CHARACTERISTIC_WARN_KEY);
        if (characteristicsFail < 0) throw mustBePositiveException(CHARACTERISTIC_FAIL_KEY);
        if (lowerCaseWarn < 0) throw mustBePositiveException(LOWER_CASE_WARN_KEY);
        if (lowerCaseFail < 0) throw mustBePositiveException(LOWER_CASE_FAIL_KEY);
        if (upperCaseWarn < 0) throw mustBePositiveException(UPPER_CASE_WARN_KEY);
        if (upperCaseFail < 0) throw mustBePositiveException(UPPER_CASE_FAIL_KEY);
        if (specialsWarn < 0) throw mustBePositiveException(SPECIAL_WARN_KEY);
        if (specialsFail < 0) throw mustBePositiveException(SPECIAL_FAIL_KEY);
        if (digitsWarn < 0) throw mustBePositiveException(DIGIT_WARN_KEY);
        if (digitsFail < 0) throw mustBePositiveException(DIGIT_FAIL_KEY);
        if (lengthWarn < 0) throw mustBePositiveException(LENGTH_WARN_KEY);
        if (lengthFail < 0) throw mustBePositiveException(LENGTH_FAIL_KEY);

        if (maxLength <= lengthWarn)
            throw getValidationException(MAX_LENGTH_KEY, maxLength, LENGTH_WARN_KEY, lengthWarn);

        if (lengthWarn <= lengthFail)
            throw getValidationException(LENGTH_WARN_KEY, lengthWarn, LENGTH_FAIL_KEY, lengthFail);

        if (specialsWarn <= specialsFail)
            throw getValidationException(SPECIAL_WARN_KEY,
                                         specialsWarn,
                                         SPECIAL_FAIL_KEY,
                                         specialsFail);

        if (digitsWarn <= digitsFail)
            throw getValidationException(DIGIT_WARN_KEY,
                                         digitsWarn,
                                         DIGIT_FAIL_KEY,
                                         digitsFail);

        if (upperCaseWarn <= upperCaseFail)
            throw getValidationException(UPPER_CASE_WARN_KEY,
                                         upperCaseWarn,
                                         UPPER_CASE_FAIL_KEY,
                                         upperCaseFail);

        if (lowerCaseWarn <= lowerCaseFail)
            throw getValidationException(LOWER_CASE_WARN_KEY,
                                         lowerCaseWarn,
                                         LOWER_CASE_FAIL_KEY,
                                         lowerCaseFail);

        if (illegalSequenceLength < IllegalSequenceRule.MINIMUM_SEQUENCE_LENGTH)
            throw new ConfigurationException(format("Illegal sequence length can not be lower than %s.",
                                                    IllegalSequenceRule.MINIMUM_SEQUENCE_LENGTH));

        if (characteristicsWarn > 4)
            throw new ConfigurationException(format("%s can not be bigger than %s",
                                                    CHARACTERISTIC_WARN_KEY,
                                                    MAX_CHARACTERISTICS));

        if (characteristicsFail > 4)
            throw new ConfigurationException(format("%s can not be bigger than %s",
                                                    CHARACTERISTIC_FAIL_KEY,
                                                    MAX_CHARACTERISTICS));

        if (characteristicsFail == characteristicsWarn)
            throw new ConfigurationException(format("%s can not be equal to %s. You set %s and %s respectively.",
                                                    CHARACTERISTIC_FAIL_KEY,
                                                    CHARACTERISTIC_WARN_KEY,
                                                    characteristicsFail,
                                                    characteristicsWarn));

        if (characteristicsFail > characteristicsWarn)
            throw new ConfigurationException(format("%s can not be bigger than %s. You have set %s and %s respectively.",
                                                    CHARACTERISTIC_FAIL_KEY,
                                                    CHARACTERISTIC_WARN_KEY,
                                                    characteristicsFail,
                                                    characteristicsWarn));

        int[] minimumLengthsWarn = new int[]{ specialsWarn, digitsWarn,
                                              upperCaseWarn, lowerCaseWarn };
        Arrays.sort(minimumLengthsWarn);

        int minimumLenghtOfWarnCharacteristics = 0;
        for (int i = 0; i < characteristicsWarn; i++)
            minimumLenghtOfWarnCharacteristics += minimumLengthsWarn[i];

        if (minimumLenghtOfWarnCharacteristics > lengthWarn)
            throw new ConfigurationException(format("The shortest password to pass the warning validator for any %s " +
                                                    "characteristics out of %s is %s but you have set the %s to %s.",
                                                    characteristicsWarn,
                                                    MAX_CHARACTERISTICS,
                                                    minimumLenghtOfWarnCharacteristics,
                                                    LENGTH_WARN_KEY,
                                                    lengthWarn));

        int[] minimumLengthsFail = new int[]{ specialsFail, digitsFail,
                                              upperCaseFail, lowerCaseFail };
        Arrays.sort(minimumLengthsFail);

        int minimumLenghtOfFailCharacteristics = 0;
        for (int i = 0; i < characteristicsFail; i++)
            minimumLenghtOfFailCharacteristics += minimumLengthsFail[i];

        if (minimumLenghtOfFailCharacteristics > lengthFail)
            throw new ConfigurationException(format("The shortest password to pass the failing validator for any %s " +
                                                    "characteristics out of %s is %s but you have set the %s to %s.",
                                                    characteristicsFail,
                                                    MAX_CHARACTERISTICS,
                                                    minimumLenghtOfFailCharacteristics,
                                                    LENGTH_FAIL_KEY,
                                                    lengthFail));

        if (dictionary != null)
        {
            File dictionaryFile = new File(dictionary);
            if (!dictionaryFile.exists())
                throw new ConfigurationException(format("Dictionary file %s does not exist.", dictionary));

            if (!dictionaryFile.isReadable())
                throw new ConfigurationException(format("Dictionary file %s is not readable.", dictionary));
        }
    }

    private ConfigurationException getValidationException(String key1, int value1, String key2, int value2)
    {
        return new ConfigurationException(format("%s of value %s is less or equal to %s of value %s",
                                                 key1, value1,
                                                 key2, value2));
    }
}
