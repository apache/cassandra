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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.PREVIOUS_PASSWORDS;
import static org.apache.cassandra.auth.CassandraAuthorizer.authReadConsistencyLevel;
import static org.apache.cassandra.auth.CassandraAuthorizer.authWriteConsistencyLevel;
import static org.apache.cassandra.cql3.QueryProcessor.execute;
import static org.apache.cassandra.db.guardrails.ValueValidator.DEFAULT_MAX_HISTORICAL_VALUES;
import static org.apache.cassandra.db.guardrails.ValueValidator.MAX_HISTORICAL_VALUES_KEY;
import static org.apache.cassandra.db.guardrails.ValueValidator.VALIDATE_AGAINST_HISTORICAL_VALUES_KEY;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public class PasswordGuardrail extends CustomGuardrail<String>
{
    private static final String SALTED_HASH_COLUMN = "salted_hash";
    private static final String COUNT_COLUMN = "count";
    private static final String CREATED_COLUMN = "created";

    private final boolean validatingAgainstHistoricalPasswords;
    private final int maxHistoricalValues;

    /**
     * @param config configuration of the custom guardrail
     */
    public PasswordGuardrail(CustomGuardrailConfig config)
    {
        super("password", config, true);
        validatingAgainstHistoricalPasswords = config.resolveBoolean(VALIDATE_AGAINST_HISTORICAL_VALUES_KEY, false);
        int resolvedMaxHistoricalValues = config.resolveInteger(MAX_HISTORICAL_VALUES_KEY, DEFAULT_MAX_HISTORICAL_VALUES);
        maxHistoricalValues = resolvedMaxHistoricalValues < 2 ? DEFAULT_MAX_HISTORICAL_VALUES : resolvedMaxHistoricalValues;
    }

    @Override
    public boolean isValidatingAgainstHistoricalValues()
    {
        return validatingAgainstHistoricalPasswords;
    }

    @Override
    public void save(ClientState state, Object... args)
    {
        assert args != null && args.length == 2;
        assert args[0] instanceof String && args[1] instanceof String;

        String roleName = (String) args[0];
        String saltedhash = (String) args[1];

        // no saving for super-user
        if (roleName.equals("cassandra"))
            return;

        execute(format("INSERT INTO %s.%s (role, created, salted_hash) VALUES (?, ?, ?)",
                       AUTH_KEYSPACE_NAME, PREVIOUS_PASSWORDS),
                authWriteConsistencyLevel(),
                roleName,
                TimeUUID.Generator.nextTimeUUID(),
                saltedhash);

        if (maxHistoricalValues < getCount(roleName))
            removeOldest(roleName);
    }

    @Override
    public List<String> retrieveHistoricalValues(Object... args)
    {
        assert args != null && args.length == 1;
        assert args[0] instanceof String;

        String roleName = (String) args[0];

        // no check for super-user
        if (roleName.equals("cassandra"))
            return Collections.emptyList();

        UntypedResultSet rows = execute(format("SELECT salted_hash FROM %s.%s WHERE role = ?",
                                               AUTH_KEYSPACE_NAME, PREVIOUS_PASSWORDS),
                                        authReadConsistencyLevel(),
                                        roleName);

        List<String> saltedHashes = new ArrayList<>();

        if (rows != null)
            for (Row row : rows)
                if (row.has(SALTED_HASH_COLUMN))
                    saltedHashes.add(row.getString(SALTED_HASH_COLUMN));

        return saltedHashes;
    }

    private long getCount(String roleName)
    {
        UntypedResultSet rows = execute(format("SELECT count(role) as count FROM %s.%s WHERE role = ?",
                                               AUTH_KEYSPACE_NAME,
                                               PREVIOUS_PASSWORDS),
                                        authReadConsistencyLevel(),
                                        roleName);

        if (rows != null && !rows.isEmpty())
        {
            Row row = rows.one();
            return row.getLong(COUNT_COLUMN);
        }

        return 0;
    }

    private void removeOldest(String roleName)
    {
        UntypedResultSet rows = execute(format("SELECT created FROM %s.%s WHERE role = ? LIMIT 1",
                                               AUTH_KEYSPACE_NAME,
                                               PREVIOUS_PASSWORDS),
                                        authReadConsistencyLevel(),
                                        roleName);

        if (rows != null && !rows.isEmpty())
        {
            Row row = rows.one();
            TimeUUID created = row.getTimeUUID(CREATED_COLUMN);

            execute(format("DELETE FROM %s.%s WHERE role = ? AND created = ?",
                           AUTH_KEYSPACE_NAME,
                           PREVIOUS_PASSWORDS),
                    authWriteConsistencyLevel(),
                    roleName, created);
        }
    }
}
