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

import org.junit.Test;

import org.assertj.core.api.Assertions;
import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.Rule;
import org.passay.RuleResultDetail;

import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_WARN_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraPasswordGeneratorTest
{
    @Test
    public void testPasswordGenerator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();

        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);
        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);

        PasswordData passwordData = new PasswordData(generator.generate(validator));
        for (Rule rule : validator.warnValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        for (Rule rule : validator.failValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        assertTrue(validator.shouldWarn(passwordData.getPassword(), true).isEmpty());
        assertTrue(validator.shouldFail(passwordData.getPassword(), true).isEmpty());
    }

    @Test
    public void testPasswordGenerationLength()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(LENGTH_WARN_KEY, 20);
        config.put(LENGTH_FAIL_KEY, 15);

        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);
        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);

        assertEquals(20, generator.generate(validator).length());
        assertEquals(30, generator.generate(30, validator).length());
    }

    @Test
    public void testPasswordGenerationOfLengthViolatingThreshold()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(LENGTH_WARN_KEY, 20);
        config.put(LENGTH_FAIL_KEY, 15);

        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);

        PasswordData passwordData = new PasswordData("13gE.^Tg$sSd%Tx34.w");

        for (Rule rule : validator.warnValidator.getRules())
        {
            if (rule instanceof LengthRule)
            {
                assertFalse(rule.validate(passwordData).isValid());
                List<RuleResultDetail> details = rule.validate(passwordData).getDetails();
                assertEquals(1, details.size());
                RuleResultDetail ruleResultDetail = details.get(0);
                assertEquals("TOO_SHORT", ruleResultDetail.getErrorCode());
            }
            else
            {
                assertTrue(rule.validate(passwordData).isValid());
            }
        }

        for (Rule rule : validator.failValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        assertFalse(validator.shouldWarn(passwordData.getPassword(), true).isEmpty());
        assertTrue(validator.shouldFail(passwordData.getPassword(), true).isEmpty());
    }

    @Test
    public void testGeneratedPasswordWithoutSpecialCharsIsInvalidWithDefaultValidator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);
        CassandraPasswordGenerator testGenerator = new TestPasswordGenerator(config);
        Assertions.assertThatThrownBy(() -> testGenerator.generate(validator))
        .hasMessageContaining("It was not possible to generate a valid password");
    }

    private static class TestPasswordGenerator extends CassandraPasswordGenerator
    {
        public TestPasswordGenerator(CustomGuardrailConfig config)
        {
            super(config);
        }

        @Override
        protected List<CharacterRule> getCharacterGenerationRules(int upper, int lower, int digits, int special)
        {
            // omit special chars rule on purpose
            return Arrays.asList(new CharacterRule(EnglishCharacterData.UpperCase, upper),
                                 new CharacterRule(EnglishCharacterData.LowerCase, lower),
                                 new CharacterRule(EnglishCharacterData.Digit, digits));
        }
    }
}
