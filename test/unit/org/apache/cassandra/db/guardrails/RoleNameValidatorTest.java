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

import org.junit.Test;

import org.apache.cassandra.db.guardrails.ValueValidator.ValidationViolation;

import static org.apache.cassandra.db.guardrails.ValueValidator.VALIDATOR_CLASS_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RoleNameValidatorTest
{
    @Test
    public void testRoleNameValidator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(VALIDATOR_CLASS_NAME_KEY, TestRoleNameValidator.class.getName());
        ValueValidator<Object> validator = ValueValidator.getValidator("role_name_policy", config);
        assertSame(TestRoleNameValidator.class.getName(), validator.getClass().getName());

        Optional<ValidationViolation> warningBecauseOfNumber = validator.shouldWarn("user123", false);
        assertTrue(warningBecauseOfNumber.isPresent());
        assertEquals("Role name contains a number.", warningBecauseOfNumber.get().message);

        Optional<ValidationViolation> errorBecauseOfSpecialChar = validator.shouldFail("user$", false);
        assertTrue(errorBecauseOfSpecialChar.isPresent());
        assertEquals("Role name is not alphanumeric.", errorBecauseOfSpecialChar.get().message);
    }
}
