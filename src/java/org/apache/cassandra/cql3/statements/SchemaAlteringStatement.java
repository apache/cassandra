/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.db.migration.*;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

/**
 * Abstract class for statements that alter the schema.
 */
public abstract class SchemaAlteringStatement extends CFStatement
{
    private static final long timeLimitForSchemaAgreement = 10 * 1000;

    private final boolean isColumnFamilyLevel;

    protected SchemaAlteringStatement()
    {
        super(null);
        this.isColumnFamilyLevel = false;
    }

    protected SchemaAlteringStatement(CFName name)
    {
        super(name);
        this.isColumnFamilyLevel = true;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (isColumnFamilyLevel)
            super.prepareKeyspace(state);
    }

    public abstract Migration getMigration() throws InvalidRequestException, IOException, ConfigurationException;

    public void checkAccess(ClientState state) throws InvalidRequestException
    {
        if (isColumnFamilyLevel)
            state.hasColumnFamilySchemaAccess(Permission.WRITE);
        else
            state.hasKeyspaceSchemaAccess(Permission.WRITE);
    }

    @Override
    public void validate(ClientState state) throws InvalidRequestException, SchemaDisagreementException
    {
        validateSchemaAgreement();
    }

    public CqlResult execute(ClientState state, List<ByteBuffer> variables) throws InvalidRequestException, SchemaDisagreementException
    {
        try
        {
            applyMigrationOnStage(getMigration());
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.toString());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.toString());
            ex.initCause(e);
            throw ex;
        }
        return null;
    }

    // Copypasta from CassandraServer (where it is private).
    private static void validateSchemaAgreement() throws SchemaDisagreementException
    {
       if (describeSchemaVersions().size() > 1)
            throw new SchemaDisagreementException();
    }

    // Copypasta from o.a.c.thrift.CassandraDaemon
    private static void applyMigrationOnStage(final Migration m) throws SchemaDisagreementException, InvalidRequestException
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            // this means call() threw an exception. deal with it directly.
            if (e.getCause() != null)
            {
                InvalidRequestException ex = new InvalidRequestException(e.getCause().getMessage());
                ex.initCause(e.getCause());
                throw ex;
            }
            else
            {
                InvalidRequestException ex = new InvalidRequestException(e.getMessage());
                ex.initCause(e);
                throw ex;
            }
        }

        validateSchemaIsSettled();
    }

    private static Map<String, List<String>> describeSchemaVersions()
    {
        // unreachable hosts don't count towards disagreement
        return Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                               Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
    }

    private static void validateSchemaIsSettled() throws SchemaDisagreementException
    {
        long limit = System.currentTimeMillis() + timeLimitForSchemaAgreement;

        outer:
        while (limit - System.currentTimeMillis() >= 0)
        {
            String currentVersionId = Schema.instance.getVersion().toString();
            for (String version : describeSchemaVersions().keySet())
            {
                if (!version.equals(currentVersionId))
                    continue outer;
            }

            // schemas agree
            return;
        }

        throw new SchemaDisagreementException();
    }
}
