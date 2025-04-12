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

package org.apache.cassandra.transport;

import java.io.IOException;

import org.junit.After;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.FBUtilities;

public abstract class NativeProtocolLimitsTestBase extends CQLTester
{
    protected final ProtocolVersion version;

    protected long emulatedUsedCapacity;

    public NativeProtocolLimitsTestBase()
    {
        this(ProtocolVersion.V5);
    }

    public NativeProtocolLimitsTestBase(ProtocolVersion version)
    {
        this.version = version;
    }

    @After
    public void dropCreatedTable()
    {
        if (emulatedUsedCapacity > 0)
        {
            releaseEmulatedCapacity(emulatedUsedCapacity);
        }
        try
        {
            QueryProcessor.executeOnceInternal("DROP TABLE " + KEYSPACE + ".atable");
        }
        catch (Throwable t)
        {
            // ignore
        }
    }

    public QueryOptions queryOptions()
    {
        return QueryOptions.create(QueryOptions.DEFAULT.getConsistency(),
                                   QueryOptions.DEFAULT.getValues(),
                                   QueryOptions.DEFAULT.skipMetadata(),
                                   QueryOptions.DEFAULT.getPageSize(),
                                   QueryOptions.DEFAULT.getPagingState(),
                                   QueryOptions.DEFAULT.getSerialConsistency(),
                                   version,
                                   KEYSPACE);
    }

    public SimpleClient client()
    {
        return client(false);
    }

    @SuppressWarnings({"resource", "SameParameterValue"})
    public SimpleClient client(boolean throwOnOverload)
    {
        return client(throwOnOverload, FrameEncoder.Payload.MAX_SIZE);
    }

    @SuppressWarnings({"resource", "SameParameterValue"})
    public SimpleClient client(boolean throwOnOverload, int largeMessageThreshold)
    {
        try
        {
            return SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                               .protocolVersion(version)
                               .useBeta()
                               .largeMessageThreshold(largeMessageThreshold)
                               .build()
                               .connect(false, throwOnOverload);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error initializing client", e);
        }
    }

    public void runClientLogic(ClientLogic clientLogic)
    {
        runClientLogic(clientLogic, true);
    }

    public void runClientLogic(ClientLogic clientLogic, boolean createTable)
    {
        try (SimpleClient client = client())
        {
            if (createTable)
                createTable(client);
            clientLogic.run(client);
        }
    }

    public void createTable(SimpleClient client)
    {
        QueryMessage queryMessage = new QueryMessage("CREATE TABLE IF NOT EXISTS " +
                                                     KEYSPACE + ".atable (pk int PRIMARY KEY, v text)",
                                                     queryOptions());
        client.execute(queryMessage);
    }

    public void doTest(ClientLogic testLogic)
    {
        try (SimpleClient client = client())
        {
            testLogic.run(client);
        }
    }
    public interface ClientLogic
    {
        void run(SimpleClient simpleClient);
    }

    public QueryMessage queryMessage(long valueSize)
    {
        StringBuilder query = new StringBuilder("INSERT INTO " + KEYSPACE + ".atable (pk, v) VALUES (1, '");
        for (int i = 0; i < valueSize; i++)
            query.append('a');
        query.append("')");
        return new QueryMessage(query.toString(), queryOptions());
    }

    protected void emulateInFlightConcurrentMessage(long length)
    {
        ClientResourceLimits.Allocator allocator = ClientResourceLimits.getAllocatorForEndpoint(FBUtilities.getJustLocalAddress());
        ClientResourceLimits.ResourceProvider resourceProvider = new ClientResourceLimits.ResourceProvider.Default(allocator);
        resourceProvider.globalLimit().allocate(length);
        resourceProvider.endpointLimit().allocate(length);
        emulatedUsedCapacity += length;
    }

    protected void releaseEmulatedCapacity(long length)
    {
        ClientResourceLimits.Allocator allocator = ClientResourceLimits.getAllocatorForEndpoint(FBUtilities.getJustLocalAddress());
        ClientResourceLimits.ResourceProvider resourceProvider = new ClientResourceLimits.ResourceProvider.Default(allocator);
        resourceProvider.globalLimit().release(length);
        resourceProvider.endpointLimit().release(length);
    }
}
