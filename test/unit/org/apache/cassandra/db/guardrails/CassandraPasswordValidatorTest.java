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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.ValueValidator.ValidationViolation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.passay.IllegalSequenceRule;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.CHARACTERISTIC_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.CHARACTERISTIC_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_CHARACTERISTIC_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_CHARACTERISTIC_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_ILLEGAL_SEQUENCE_LENGTH;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_LENGTH_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_LENGTH_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_LOWER_CASE_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_LOWER_CASE_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_SPECIAL_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_SPECIAL_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_UPPER_CASE_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_UPPER_CASE_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DICTIONARY_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DIGIT_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DIGIT_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.ILLEGAL_SEQUENCE_LENGTH_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LOWER_CASE_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LOWER_CASE_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MAX_CHARACTERISTICS;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MAX_LENGTH_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.SPECIAL_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.SPECIAL_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.UPPER_CASE_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.UPPER_CASE_WARN_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CassandraPasswordValidatorTest
{
    @Test
    public void testEmptyConfigIsValid()
    {
        new CassandraPasswordValidator(new CustomGuardrailConfig());
    }

    @Test
    public void testDefaultConfiguration()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        CustomGuardrailConfig parameters = validator.getParameters();
        CassandraPasswordConfiguration conf = new CassandraPasswordConfiguration(parameters);

        assertEquals(DEFAULT_CHARACTERISTIC_WARN, conf.characteristicsWarn);
        assertEquals(DEFAULT_CHARACTERISTIC_FAIL, conf.characteristicsFail);

        assertEquals(DEFAULT_LENGTH_WARN, conf.lengthWarn);
        assertEquals(DEFAULT_LENGTH_FAIL, conf.lengthFail);

        assertEquals(DEFAULT_UPPER_CASE_WARN, conf.upperCaseWarn);
        assertEquals(DEFAULT_UPPER_CASE_FAIL, conf.upperCaseFail);

        assertEquals(DEFAULT_LOWER_CASE_WARN, conf.lowerCaseWarn);
        assertEquals(DEFAULT_LOWER_CASE_FAIL, conf.lowerCaseFail);

        assertEquals(DEFAULT_SPECIAL_WARN, conf.specialsWarn);
        assertEquals(DEFAULT_SPECIAL_FAIL, conf.specialsFail);

        assertEquals(DEFAULT_ILLEGAL_SEQUENCE_LENGTH, conf.illegalSequenceLength);
    }

    @Test
    public void testInvalidParameters()
    {
        for (int[] warnAndFail : new int[][]{ { 10, 10 }, { 5, 10 } })
        {
            int warn = warnAndFail[0];
            int fail = warnAndFail[1];
            validateWithConfig(() -> Map.of(LENGTH_WARN_KEY, warn, LENGTH_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      LENGTH_WARN_KEY, warn, LENGTH_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(SPECIAL_WARN_KEY, warn, SPECIAL_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      SPECIAL_WARN_KEY, warn, SPECIAL_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(DIGIT_WARN_KEY, warn, DIGIT_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      DIGIT_WARN_KEY, warn, DIGIT_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(LOWER_CASE_WARN_KEY, warn, LOWER_CASE_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      LOWER_CASE_WARN_KEY, warn, LOWER_CASE_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(UPPER_CASE_WARN_KEY, warn, UPPER_CASE_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      UPPER_CASE_WARN_KEY, warn, UPPER_CASE_FAIL_KEY, fail));
        }

        validateWithConfig(() -> Map.of(ILLEGAL_SEQUENCE_LENGTH_KEY, 2),
                           format("Illegal sequence length can not be lower than %s.",
                                  IllegalSequenceRule.MINIMUM_SEQUENCE_LENGTH));

        validateWithConfig(() -> Map.of(CHARACTERISTIC_WARN_KEY, 5),
                           format("%s can not be bigger than %s",
                                  CHARACTERISTIC_WARN_KEY, MAX_CHARACTERISTICS));

        validateWithConfig(() -> Map.of(CHARACTERISTIC_FAIL_KEY, 5),
                           format("%s can not be bigger than %s",
                                  CHARACTERISTIC_FAIL_KEY, MAX_CHARACTERISTICS));

        validateWithConfig(() -> Map.of(CHARACTERISTIC_WARN_KEY, 3, CHARACTERISTIC_FAIL_KEY, 3),
                           format("%s can not be equal to %s. You set %s and %s respectively.",
                                  CHARACTERISTIC_FAIL_KEY, CHARACTERISTIC_WARN_KEY, 3, 3));

        validateWithConfig(() -> Map.of(CHARACTERISTIC_WARN_KEY, 3, CHARACTERISTIC_FAIL_KEY, 4),
                           format("%s can not be bigger than %s. You have set %s and %s respectively.",
                                  CHARACTERISTIC_FAIL_KEY, CHARACTERISTIC_WARN_KEY, 4, 3));

        validateWithConfig(() -> Map.of(SPECIAL_WARN_KEY, 1,
                                        SPECIAL_FAIL_KEY, 0,
                                        DIGIT_WARN_KEY, 1,
                                        DIGIT_FAIL_KEY, 0,
                                        UPPER_CASE_WARN_KEY, 2,
                                        LOWER_CASE_WARN_KEY, 2,
                                        CHARACTERISTIC_WARN_KEY, 3,
                                        CHARACTERISTIC_FAIL_KEY, 2,
                                        LENGTH_WARN_KEY, 3,
                                        LENGTH_FAIL_KEY, 2),
                           format("The shortest password to pass the warning validator for any %s characteristics " +
                                  "out of %s is %s but you have set the %s to %s.",
                                  3, MAX_CHARACTERISTICS, 4, LENGTH_WARN_KEY, 3));

        validateWithConfig(() ->
                           new HashMap<>()
                           {{
                               put(SPECIAL_FAIL_KEY, 1);
                               put(DIGIT_WARN_KEY, 2);
                               put(DIGIT_FAIL_KEY, 1);
                               put(UPPER_CASE_WARN_KEY, 2);
                               put(UPPER_CASE_FAIL_KEY, 1);
                               put(LOWER_CASE_WARN_KEY, 2);
                               put(LOWER_CASE_FAIL_KEY, 1);
                               put(CHARACTERISTIC_WARN_KEY, 4);
                               put(CHARACTERISTIC_FAIL_KEY, 3);
                               put(LENGTH_WARN_KEY, 8);
                               put(LENGTH_FAIL_KEY, 2);
                           }},
                           format("The shortest password to pass the failing validator for any %s characteristics " +
                                  "out of %s is %s but you have set the %s to %s.",
                                  3, MAX_CHARACTERISTICS, 3, LENGTH_FAIL_KEY, 2));

        for (Map.Entry<String, Integer> entry : new HashMap<String, Integer>()
        {{
            put(MAX_LENGTH_KEY, -1);
            put(SPECIAL_FAIL_KEY, -1);
            put(DIGIT_WARN_KEY, -1);
            put(DIGIT_FAIL_KEY, -1);
            put(UPPER_CASE_WARN_KEY, -1);
            put(UPPER_CASE_FAIL_KEY, -1);
            put(LOWER_CASE_WARN_KEY, -1);
            put(LOWER_CASE_FAIL_KEY, -1);
            put(CHARACTERISTIC_WARN_KEY, -1);
            put(CHARACTERISTIC_FAIL_KEY, -1);
            put(LENGTH_WARN_KEY, -1);
            put(LENGTH_FAIL_KEY, -1);
        }}.entrySet())
        {
            validateWithConfig(() -> Map.of(entry.getKey(), entry.getValue()),
                               format("%s must be positive.", entry.getKey()));
        }
    }

    @Test
    public void testIllegalSequences()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldFail("A1$abcdefgh", true);
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message,
                   containsString("Password contains the illegal alphabetical sequence 'abcdefgh'."));

        validationResult = validator.shouldFail("A1$a123456", true);
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message,
                   containsString("Password contains the illegal numerical sequence '123456'."));

        validationResult = validator.shouldFail("A1$asdfghjkl", true);
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message,
                   containsString("Password contains the illegal QWERTY sequence 'asdfghjkl'."));

        // test e.g. illegal polish sequence
        validationResult = validator.shouldFail("A1$ęlłmnńoóp", true);
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message,
                   containsString("Password contains the illegal alphabetical sequence 'lłmnńoóp'."));
    }

    @Test
    public void testWhitespace()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldFail("A1$abcd efgh", true);
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message, containsString("Password contains a whitespace character."));
    }

    @Test
    public void testFailingValidationResult()
    {
        String verboseMessage = "Password was not set as it violated configured password strength policy. " +
                                "To fix this error, the following has to be resolved: " +
                                "Password must contain 1 or more uppercase characters. " +
                                "Password must contain 1 or more digit characters. " +
                                "Password must contain 1 or more special characters. " +
                                "Password matches 1 of 4 character rules, but 3 are required. " +
                                "You may also use 'GENERATED PASSWORD' upon role creation or alteration.";

        String simpleMessage = "Password was not set as it violated configured password strength policy. You may also use 'GENERATED PASSWORD' upon role creation or alteration.";

        for (Object[] entry : new Object[][]{
        // first boolean is if detailed messages should be provided
        // second boolean is if it is called by a super-user
        { simpleMessage, FALSE, FALSE },
        { verboseMessage, FALSE, TRUE },
        { verboseMessage, TRUE, FALSE },
        { verboseMessage, TRUE, TRUE } })
        {
            CustomGuardrailConfig config = new CustomGuardrailConfig();
            config.put(CassandraPasswordConfiguration.DETAILED_MESSAGES_KEY, entry[1]);
            CassandraPasswordValidator validator = new CassandraPasswordValidator(config);

            Optional<ValidationViolation> validationResult = validator.shouldFail("acefghuiiui", (boolean) entry[2]);
            assertTrue(validationResult.isPresent());
            assertEquals("[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, INSUFFICIENT_SPECIAL, INSUFFICIENT_UPPERCASE]",
                         validationResult.get().redactedMessage);
            assertEquals(entry[0], validationResult.get().message);

            // additionally, we satisfied special chars and digit characteristics,
            // by that we satisfied 3 out of 4 character rules, so we are not emitting a failure
            validationResult = validator.shouldFail("a5ef1hu.iu$", (boolean) entry[2]);
            assertFalse(validationResult.isPresent());
        }
    }

    @Test
    public void testWarningValidationResult()
    {
        String verboseMessage = "Password was set, however it might not be strong enough according to the configured password strength policy. " +
                                "To fix this warning, the following has to be resolved: " +
                                "Password must contain 2 or more uppercase characters. " +
                                "Password must contain 2 or more digit characters. " +
                                "Password matches 2 of 4 character rules, but 4 are required. " +
                                "You may also use 'GENERATED PASSWORD' upon role creation or alteration.";

        String simpleMessage = "Password was set, however it might not be strong enough according to the configured password strength policy.";

        for (Object[] entry : new Object[][]{
        // first boolean is if detailed messages should be provided
        // second boolean is if it is called by a super-user
        { simpleMessage, FALSE, FALSE },
        { verboseMessage, FALSE, TRUE },
        { verboseMessage, TRUE, FALSE },
        { verboseMessage, TRUE, TRUE } })
        {
            CustomGuardrailConfig config = new CustomGuardrailConfig();
            config.put(CassandraPasswordConfiguration.DETAILED_MESSAGES_KEY, entry[1]);
            CassandraPasswordValidator validator = new CassandraPasswordValidator(config);

            Optional<ValidationViolation> validationResult = validator.shouldWarn("t$Efg1#a..fr", (boolean) entry[2]);
            assertTrue(validationResult.isPresent());
            assertEquals("[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, INSUFFICIENT_UPPERCASE]",
                         validationResult.get().redactedMessage);
            assertEquals(entry[0], validationResult.get().message);

            // we added one more number and one more upper-case character,
            // so we started to satisfy 4 out of 4 characteristics which does not emit any warning
            assertFalse(validator.shouldWarn("A$Efg1#a..6r", (boolean) entry[2]).isPresent());
        }
    }

    @Test
    public void testDictionary()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig()
        {{
            put(DICTIONARY_KEY, new File("test/resources/passwordDictionary.txt").absolutePath());
        }};
        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);

        Optional<ValidationViolation> maybeViolation = validator.shouldFail("thisIsSOmePasswOrdInADictionary",
                                                                            false);
        assertTrue(maybeViolation.isPresent());
        assertEquals("[ILLEGAL_WORD]", maybeViolation.get().redactedMessage);

        validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        maybeViolation = validator.shouldFail("thisIsSOmePasswOrdInADictionary", false);
        assertTrue(maybeViolation.isPresent());
        assertEquals("[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, INSUFFICIENT_SPECIAL]",
                     maybeViolation.get().redactedMessage);
    }

    @Test
    public void testMissingDictionary()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig()
        {{
            put(DICTIONARY_KEY, new File("this/file/does/not/exist").absolutePath());
        }};

        assertThatThrownBy(() -> new CassandraPasswordValidator(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("does not exist");
    }

    private void validateWithConfig(Supplier<Map<String, Object>> configSupplier, String expectedMessage)
    {
        CustomGuardrailConfig customConfig = new CustomGuardrailConfig();
        customConfig.putAll(configSupplier.get());

        assertThatThrownBy(() -> new CassandraPasswordConfiguration(customConfig))
        .hasMessageContaining(expectedMessage)
        .isInstanceOf(ConfigurationException.class);
    }

    @Test
    public void testUnicodeCharacters()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        assertFalse(validator.shouldFail("13gE.^Tg$sSd%棚山", true).isPresent());
        assertFalse(validator.shouldFail("13gE.^Tg$sSd%棚", true).isPresent());
        assertFalse(validator.shouldFail("13gE.^Tg$sSd%", true).isPresent());
        assertFalse(validator.shouldFail("13g.^Tg$sSd", true).isPresent());

        assertFalse(validator.shouldWarn("13gE.^Tg$sSd%棚山", true).isPresent());
        assertFalse(validator.shouldWarn("13gE.^Tg$sSd%棚", true).isPresent());
        assertFalse(validator.shouldWarn("13gE.^Tg$sSd%", true).isPresent());

        assertFalse(validator.shouldWarn("ášK&Ič[ž90Ašž", true).isPresent());
        assertFalse(validator.shouldFail("ášK&Ič[ž90Ašž", true).isPresent());

        // unicode characters are contributing to length attribute, but
        // they are not contributing to any other rules, this password is 12 chars long,
        // which is required to not emit any warning,
        // it needs to contain at least:
        // 2 specials (contains 3)
        // 2 upper chars (contains 2)
        // 2 lower chars (contains 3)
        // 2 digits (contains 2)
        // together 10, plus 2 unicodes = 12, so it satisfies length characteristic as well
        assertFalse(validator.shouldWarn("山13g.^Tg$棚Sd", true).isPresent());

        // unsupported languages should be rejected
        assertTrue(validator.shouldWarn("卡桑德拉-卡桑德拉山山羊棚", true).isPresent());
        assertTrue(validator.shouldFail("卡桑德拉卡桑德拉山山羊棚", true).isPresent());
        assertTrue(validator.shouldWarn("لرئتينمسؤونعنلتنفس", true).isPresent());
        assertTrue(validator.shouldFail("لرئتينمسؤو-نعنلتنفس", true).isPresent());
    }

    @Test
    public void testMaxLength()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);
        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);
        String password = generator.generate(1000, validator);
        assertEquals(1000, password.length());

        assertThatThrownBy(() -> generator.generate(1001, validator))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Unable to generate a password of length " + 1001);
    }
}
