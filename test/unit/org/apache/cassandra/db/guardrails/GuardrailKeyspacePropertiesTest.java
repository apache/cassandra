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

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.db.guardrails.GuardrailTester;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.LocalizeString;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Tests the guardrail for keyspace properties, {@link Guardrails#keyspaceProperties}.
 */
public class GuardrailKeyspacePropertiesTest extends GuardrailTester
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s %s";
    private static final String ALTER_KEYSPACE = "ALTER KEYSPACE %s WITH %s";

    private static final String WARNED_PROPERTY_NAME = "keyspace_properties_warned";
    private static final String IGNORED_PROPERTY_NAME = "keyspace_properties_ignored";
    private static final String DISALLOWED_PROPERTY_NAME = "keyspace_properties_disallowed";

    private static final String DURABLE_WRITES = "durable_writes = false";
    private static final String REPLICATION = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}";
    
    private static final String WARN_MESSAGE = "Provided values [%s] are not recommended for Keyspace Properties";
    private static final String IGNORE_MESSAGE = "Ignoring provided values [%s] as they are not supported for Keyspace Properties";
    private static final String FAIL_MESSAGE = "Provided values [%s] are not allowed for Keyspace Properties";

    public GuardrailKeyspacePropertiesTest()
    {
        super(Guardrails.keyspaceProperties);
    }

    @Before
    public void before()
    {
        // Set up basic test configuration - only allow "replication" by default
        Set<String> allowed = new HashSet<>(List.of("replication", "durable_writes"));
        Set<String> allKeyspaceProperties = Arrays.stream(KeyspaceParams.Option.values())
                                                  .map(KeyspaceParams.Option::toString)
                                                  .collect(Collectors.toSet());
        guardrails().setKeyspacePropertiesDisallowed(allKeyspaceProperties
                                                     .stream()
                                                     .filter(p -> !allowed.contains(p))
                                                     .map(LocalizeString::toUpperCaseLocalized)
                                                     .collect(Collectors.toSet()));
        // Configure "durable_writes" to be warned about (tests will override as needed)
        guardrails().setKeyspacePropertiesWarned("durable_writes");
        guardrails().setKeyspacePropertiesIgnored(Collections.emptySet());
    }

    @Test
    public void testConfigValidation()
    {
        String message = "Invalid value for %s: null is not allowed";
        assertInvalidProperty(Guardrails::setKeyspacePropertiesWarned, (Set<String>) null, message, WARNED_PROPERTY_NAME);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesIgnored, (Set<String>) null, message, IGNORED_PROPERTY_NAME);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesDisallowed, (Set<String>) null, message, DISALLOWED_PROPERTY_NAME);

        assertValidProperty(Collections.emptySet());
        assertValidProperty(getValidKeyspaceProperties());

        assertValidPropertyCSV("");
        assertValidPropertyCSV(String.join(",", getValidKeyspaceProperties()));

        assertInvalidProperty(Collections.singleton("invalid"), Collections.singleton("invalid"));
        assertInvalidProperty(ImmutableSet.of("replication", "invalid1", "invalid2"), ImmutableSet.of("invalid1", "invalid2"));
        assertInvalidProperty(ImmutableSet.of("invalid1", "invalid2", "replication"), ImmutableSet.of("invalid1", "invalid2"));
        assertInvalidProperty(ImmutableSet.of("invalid1", "replication", "invalid2"), ImmutableSet.of("invalid1", "invalid2"));

        assertInvalidPropertyCSV("invalid", "[invalid]");
        assertInvalidPropertyCSV("replication,invalid1,invalid2", "[invalid1, invalid2]");
        assertInvalidPropertyCSV("invalid1,invalid2,replication", "[invalid1, invalid2]");
        assertInvalidPropertyCSV("invalid1,replication,invalid2", "[invalid1, invalid2]");
    }

    private void assertValidProperty(Set<String> properties)
    {
        assertValidProperty(Guardrails::setKeyspacePropertiesWarned, Guardrails::getKeyspacePropertiesWarned, properties);
        assertValidProperty(Guardrails::setKeyspacePropertiesIgnored, Guardrails::getKeyspacePropertiesIgnored, properties);
        assertValidProperty(Guardrails::setKeyspacePropertiesDisallowed, Guardrails::getKeyspacePropertiesDisallowed, properties);
    }

    private void assertValidPropertyCSV(String csv)
    {
        csv = sortCSV(csv);
        assertValidProperty(Guardrails::setKeyspacePropertiesWarnedCSV, g -> sortCSV(g.getKeyspacePropertiesWarnedCSV()), csv);
        assertValidProperty(Guardrails::setKeyspacePropertiesIgnoredCSV, g -> sortCSV(g.getKeyspacePropertiesIgnoredCSV()), csv);
        assertValidProperty(Guardrails::setKeyspacePropertiesDisallowedCSV, g -> sortCSV(g.getKeyspacePropertiesDisallowedCSV()), csv);
    }

    private void assertInvalidProperty(Set<String> properties, Set<String> invalidProperties)
    {
        String message = format("Invalid values for %s: '%s' do not parse as valid keyspace properties",
                                WARNED_PROPERTY_NAME, invalidProperties);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesWarned, properties, message, WARNED_PROPERTY_NAME);

        message = format("Invalid values for %s: '%s' do not parse as valid keyspace properties",
                         IGNORED_PROPERTY_NAME, invalidProperties);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesIgnored, properties, message, IGNORED_PROPERTY_NAME);

        message = format("Invalid values for %s: '%s' do not parse as valid keyspace properties",
                         DISALLOWED_PROPERTY_NAME, invalidProperties);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesDisallowed, properties, message, DISALLOWED_PROPERTY_NAME);
    }

    private void assertInvalidPropertyCSV(String properties, String rejected)
    {
        String message = "Invalid values for %s: '%s' do not parse as valid keyspace properties";
        assertInvalidProperty(Guardrails::setKeyspacePropertiesWarnedCSV, properties, message, WARNED_PROPERTY_NAME, rejected);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesIgnoredCSV, properties, message, IGNORED_PROPERTY_NAME, rejected);
        assertInvalidProperty(Guardrails::setKeyspacePropertiesDisallowedCSV, properties, message, DISALLOWED_PROPERTY_NAME, rejected);
    }

    @Test
    public void testCreateKeyspaceWithWarned() throws Throwable
    {
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION + " AND " + DURABLE_WRITES);
        assertWarns(statement, format(WARN_MESSAGE, "durable_writes"));
    }

    @Test
    public void testCreateKeyspaceWithIgnored() throws Throwable
    {
        guardrails().setKeyspacePropertiesIgnored("durable_writes");
        guardrails().setKeyspacePropertiesWarned(Collections.emptySet());
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION + " AND " + DURABLE_WRITES);
        assertWarns(statement, format(IGNORE_MESSAGE, "durable_writes"));
    }

    @Test
    public void testCreateKeyspaceWithDisallowed() throws Throwable
    {
        guardrails().setKeyspacePropertiesDisallowed("durable_writes");
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION + " AND " + DURABLE_WRITES);
        assertFails(statement, format(FAIL_MESSAGE, "durable_writes"));
    }

    @Test
    public void testAlterKeyspaceWithWarned() throws Throwable
    {
        String ks = createKeyspaceName();
        execute(format(CREATE_KEYSPACE, ks, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"));
        String statement = format(ALTER_KEYSPACE, ks, DURABLE_WRITES);
        assertWarns(statement, format(WARN_MESSAGE, "durable_writes"));
    }

    @Test
    public void testAlterKeyspaceWithDisallowed() throws Throwable
    {
        guardrails().setKeyspacePropertiesDisallowed("durable_writes");
        String ks = createKeyspaceName();
        execute(format(CREATE_KEYSPACE, ks, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"));
        String statement = format(ALTER_KEYSPACE, ks, DURABLE_WRITES);
        assertFails(statement, format(FAIL_MESSAGE, "durable_writes"));
    }

    @Test
    public void testReplicationPropertyNeverBlocked() throws Throwable
    {
        // Test that replication property is never blocked since it's required for keyspace creation
        guardrails().setKeyspacePropertiesDisallowed("replication");
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION);
        assertValid(statement);
    }

    @Test
    public void testDisallowedPropertyTakesPrecedenceOverWarned() throws Throwable
    {
        // Set a property as both warned and disallowed - disallowed should take precedence
        guardrails().setKeyspacePropertiesWarned("durable_writes");
        guardrails().setKeyspacePropertiesDisallowed("durable_writes");
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION + " AND " + DURABLE_WRITES);
        assertFails(statement, format(FAIL_MESSAGE, "durable_writes"));
    }

    @Test
    public void testValidKeyspaceCreation() throws Throwable
    {
        // Test normal keyspace creation case without any guardrail violations
        guardrails().setKeyspacePropertiesDisallowed(Collections.emptySet());
        guardrails().setKeyspacePropertiesWarned(Collections.emptySet());
        String ks = createKeyspaceName();
        String statement = format(CREATE_KEYSPACE, ks, REPLICATION);
        assertValid(statement);
    }

    private Set<String> getValidKeyspaceProperties()
    {
        return Arrays.stream(KeyspaceParams.Option.values())
                     .map(KeyspaceParams.Option::toString)
                     .collect(Collectors.toSet());
    }
}
