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

package org.apache.cassandra.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.auth.AuthUtils.escapeCqlLiteral;

public abstract class AbstractDefaultRoleInitializer implements IDefaultRoleInitializer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractDefaultRoleInitializer.class);

    @Override
    public final void initializeDefaultRoleIfNeeded()
    {
        if (ClusterMetadata.current().tokenMap.tokens().isEmpty())
            throw new IllegalStateException(getClass().getSimpleName() + " skipped role setup: no known tokens in the ring");

        try {
            if (!hasExistingRoles())
                createDefaultRole();

        } catch (RequestExecutionException e)
        {
            logger.warn(getClass().getSimpleName() + " skipped default role setup: some nodes were not ready");
            throw e;
        }
    }

    @Override
    public final boolean hasExistingRoles()
    {
        // Try looking up the configured default role first, to avoid the range query if possible.
        String defaultRoleQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, escapeCqlLiteral(defaultRoleName()));
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        return !QueryProcessor.process(defaultRoleQuery, ConsistencyLevel.ONE).isEmpty()
               || !QueryProcessor.process(defaultRoleQuery, ConsistencyLevel.QUORUM).isEmpty()
               || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }
}
