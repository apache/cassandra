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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.mindrot.jbcrypt.BCrypt;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.LENGTH_WARN_KEY;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailPasswordTest extends GuardrailTester
{
    private void setGuardrail(Map<String, Object> config)
    {
        guardrails().reconfigurePasswordValidator(config);
    }

    @After
    public void afterTest() throws Throwable
    {
        // disable guardrail
        setGuardrail(Map.of());
    }

    private String entity = "ROLE";

    @Test
    public void testPasswordGuardrailForRole() throws Throwable
    {
        testPasswordGuardrailInternal();
    }

    @Test
    public void testPasswordGuardrailForUser() throws Throwable
    {
        entity = "USER";
        testPasswordGuardrailInternal();
    }

    /**
     * Test that if there is no password validator / generator set then it is not possible to generate any passwords
     */
    @Test
    public void testDisabledGuardrailPreventPasswordGeneration() throws Throwable
    {
        // this should generate a password automatically, but it will fail because there is no configured generator
        executeRoleStatement(true, "role6", null, true,
                             "You have to enable password_validator and it's generator_class_name property " +
                             "in cassandra.yaml to be able to generate passwords.",
                             null, true);
    }

    /**
     * If a user uses "GENERATED PASSWORD" and "HASHED PASSWORD", such combination is not possible
     */
    @Test
    public void testGeneratedAndHashedPasswordCannotBeUsedTogether() throws Throwable
    {
        // enable password guardrail
        setGuardrail(getConfig(true));

        String hashedPassword = BCrypt.hashpw("doesnotmatter", BCrypt.gensalt(10));

        assertThatThrownBy(() -> execute(userClientState, "CREATE ROLE role10 WITH GENERATED PASSWORD AND HASHED PASSWORD = '" + hashedPassword + '\''))
        .isInstanceOf(SyntaxException.class)
        .hasMessage("Options 'hashed password' and 'generated password' are mutually exclusive");

        assertThatThrownBy(() -> execute(userClientState, "CREATE ROLE role10 WITH GENERATED PASSWORD AND PASSWORD = 'doesnotmatter'"))
        .isInstanceOf(SyntaxException.class)
        .hasMessage("Options 'password' and 'generated password' are mutually exclusive");
    }

    @Test
    public void testAllSpecialCharactersArePossibleToUseInCQLQuery()
    {
        setGuardrail(getConfig(true));

        PasswordGuardrail guardrail = new PasswordGuardrail(() -> new CustomGuardrailConfig(getConfig(true)));
        String userGeneratedPassword = guardrail.generate(20);
        String allSpecialCharacters = CassandraPasswordValidator.specialCharacters.getCharacters();
        String passwordWithAllSpecialChars = userGeneratedPassword + allSpecialCharacters;

        String queryToExecute = "CREATE ROLE role123 WITH PASSWORD = '" + passwordWithAllSpecialChars + '\'';
        execute(userClientState, queryToExecute);
    }

    private String getEntityName(String name)
    {
        return toLowerCaseLocalized(name + entity);
    }

    private void testPasswordGuardrailInternal() throws Throwable
    {
        // test that by default there is no password guardrail by creating a user with some invalid password
        executeRoleStatement(true, getEntityName("role1"), "abcdefgh", false, null, null, true);

        // enable password guardrail
        setGuardrail(getConfig(true));

        // test that creation of a role with invalid password does not work anymore
        executeRoleStatement(true, getEntityName("role2"), "abcdefgh",
                             true,
                             "Password was not set as it violated configured password strength policy.",
                             "[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, TOO_SHORT, ILLEGAL_ALPHABETICAL_SEQUENCE, INSUFFICIENT_SPECIAL, INSUFFICIENT_UPPERCASE]}",
                             true);

        PasswordGuardrail guardrail = new PasswordGuardrail(() -> new CustomGuardrailConfig(getConfig(true)));
        String userGeneratedPassword = guardrail.generate(20);

        executeRoleStatement(true, getEntityName("role3"), userGeneratedPassword, false, null, null, true);

        // altering role3 with valid password should work
        Optional<ResultMessage> resultMessage = executeRoleStatement(false, getEntityName("role3"), userGeneratedPassword, false, null, null, true);
        // there is no password provided in the response as we set ours
        assertTrue(resultMessage.isEmpty());

        // alter role3 with generated password
        Optional<ResultMessage> resultMessage2 = executeRoleStatement(false, getEntityName("role3"), null, false, null, null, true);
        // we have not passed our own password, so it will be generated and returned to us
        assertTrue(resultMessage2.isPresent());
        String cassandraGeneratedPassword = extractGeneratedPassword(resultMessage2.get());

        // verify that they are valid
        assertTrue(guardrail.getValidator().shouldWarn(cassandraGeneratedPassword, superClientState.isSuper()).isEmpty());
        assertTrue(guardrail.getValidator().shouldFail(cassandraGeneratedPassword, superClientState.isSuper()).isEmpty());

        // reconfigure it so it requires 25/20 length

        CustomGuardrailConfig config = getConfig(true);
        config.put(LENGTH_FAIL_KEY, 20);
        config.put(LENGTH_WARN_KEY, 25);
        setGuardrail(config);

        // generate a new password which will be of size 21, that should emit warning now
        userGeneratedPassword = guardrail.generate(21);

        executeRoleStatement(true, getEntityName("role4"),
                             userGeneratedPassword,
                             false,
                             "Guardrail password violated: Password was set, however it might not be strong enough according to the configured password strength policy. " +
                             "To fix this warning, the following has to be resolved: Password must be 25 or more characters in length. " +
                             "You may also use 'GENERATED PASSWORD' upon role creation or alteration.",
                             "[TOO_SHORT]",
                             false);

        // disable password guardrail
        setGuardrail(getConfig(false));

        // without guardrail, we can create roles with whatever passwords again
        executeRoleStatement(true, getEntityName("role5"), "abcdefgh", false, null, null, true);
    }

    private String extractGeneratedPassword(ResultMessage resultMessage)
    {
        if (resultMessage.type != Message.Type.RESULT || resultMessage.kind != ResultMessage.Kind.ROWS)
            fail("Expected RESULT type and ROWS kind, got " + resultMessage.type + " and " + resultMessage.kind);

        ResultMessage.Rows rows = ((ResultMessage.Rows) resultMessage);
        assertNotNull(rows.result);
        assertFalse(rows.result.isEmpty());
        assertEquals(1, rows.result.rows.size());
        assertEquals(1, rows.result.metadata.names.size());
        assertEquals(UTF8Type.instance.asCQL3Type(), rows.result.metadata.names.get(0).type.asCQL3Type());
        assertEquals("generated_password", rows.result.metadata.names.get(0).name.toString());
        List<ByteBuffer> passwordByteBuffer = rows.result.rows.get(0);
        assertNotNull(passwordByteBuffer);
        assertEquals(1, passwordByteBuffer.size());
        return UTF8Type.instance.getSerializer().deserialize(passwordByteBuffer.get(0));
    }

    private Optional<ResultMessage> executeRoleStatement(boolean create,
                                                         String roleName,
                                                         String password,
                                                         boolean expectThrow,
                                                         String expectedFailureMessage,
                                                         String expectedRedactedMessage,
                                                         boolean assertFails) throws Throwable
    {
        String query;
        if (password == null)
        {
            if (create)
                query = format("CREATE " + entity + " %s WITH GENERATED PASSWORD", roleName);
            else
                query = format("ALTER " + entity + " %s WITH GENERATED PASSWORD", roleName);
        }
        else
        {
            if (create)
                query = format("CREATE " + entity + " %s WITH PASSWORD %s", roleName, entity.equals("ROLE") ? "= " : "") + '\'' + password + '\'';
            else
                query = format("ALTER " + entity + " %s WITH PASSWORD %s", roleName, entity.equals("ROLE") ? "= " : "") + '\'' + password + '\'';
        }

        final String queryToExecute = query;
        if (assertFails)
        {
            return assertFails(() -> execute(userClientState, queryToExecute),
                               expectThrow,
                               expectedFailureMessage == null ? List.of() : singletonList(expectedFailureMessage),
                               expectedRedactedMessage == null ? List.of() : singletonList(expectedRedactedMessage));
        }
        else
        {
            return Optional.ofNullable(assertWarnsWithResult(() -> execute(userClientState, queryToExecute),
                                                             expectedFailureMessage == null ? List.of() : singletonList(expectedFailureMessage),
                                                             expectedRedactedMessage == null ? List.of() : singletonList(expectedRedactedMessage)));
        }
    }

    private CustomGuardrailConfig getConfig(boolean validate)
    {
        if (validate)
        {
            CustomGuardrailConfig config = new CassandraPasswordConfiguration(new CustomGuardrailConfig()).asCustomGuardrailConfig();

            config.put(ValueValidator.CLASS_NAME_KEY, CassandraPasswordValidator.class.getName());
            config.put(ValueGenerator.GENERATOR_CLASS_NAME_KEY, CassandraPasswordGenerator.class.getName());

            config.put(LENGTH_FAIL_KEY, 15);
            config.put(LENGTH_WARN_KEY, 20);

            return config;
        }
        else
        {
            return new CustomGuardrailConfig();
        }
    }
}
