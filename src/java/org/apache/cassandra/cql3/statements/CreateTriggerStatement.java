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
package org.apache.cassandra.cql3.statements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class CreateTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateTriggerStatement.class);

    private final String triggerName;
    private final String triggerClass;
    private final boolean ifNotExists;

    public CreateTriggerStatement(CFName name, String triggerName, String clazz, boolean ifNotExists)
    {
        super(name);
        this.triggerName = triggerName;
        this.triggerClass = clazz;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        state.ensureIsSuper("Only superusers are allowed to perform CREATE TRIGGER queries");
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        TableMetadata metadata = Schema.instance.validateTable(keyspace(), columnFamily());
        if (metadata.isView())
            throw new InvalidRequestException("Cannot CREATE TRIGGER against a materialized view");

        try
        {
            TriggerExecutor.instance.loadTriggerInstance(triggerClass);
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("Trigger class '%s' doesn't exist", triggerClass));
        }
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException, InvalidRequestException
    {
        TableMetadata current = Schema.instance.getTableMetadata(keyspace(), columnFamily());
        Triggers triggers = current.triggers;

        if (triggers.get(triggerName).isPresent())
        {
            if (ifNotExists)
                return null;
            else
                throw new InvalidRequestException(String.format("Trigger %s already exists", triggerName));
        }

        TableMetadata updated =
            current.unbuild()
                   .triggers(triggers.with(TriggerMetadata.create(triggerName, triggerClass)))
                   .build();

        logger.info("Adding trigger with name {} and class {}", triggerName, triggerClass);

        MigrationManager.announceTableUpdate(updated, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
