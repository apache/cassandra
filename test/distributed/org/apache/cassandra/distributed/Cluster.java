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

package org.apache.cassandra.distributed;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.net.Message;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * A simple cluster supporting only the 'current' Cassandra version, offering easy access to the convenience methods
 * of IInvokableInstance on each node.
 */
public class Cluster extends AbstractCluster<IInvokableInstance>
{
    private Cluster(Builder builder)
    {
        super(builder);
    }

    protected IInvokableInstance newInstanceWrapper(Versions.Version version, IInstanceConfig config)
    {
        return new Wrapper(version, config);
    }

    public static Builder build()
    {
        return new Builder();
    }

    public static Builder build(int nodeCount)
    {
        return build().withNodes(nodeCount);
    }

    public static Cluster create(int nodeCount, Consumer<IInstanceConfig> configUpdater) throws IOException
    {
        return build(nodeCount).withConfig(configUpdater).start();
    }

    public static Cluster create(int nodeCount) throws Throwable
    {
        return build(nodeCount).start();
    }

    public static final class Builder extends AbstractBuilder<IInvokableInstance, Cluster, Builder>
    {
        public Builder()
        {
            super(Cluster::new);
            withVersion(CURRENT_VERSION);
        }
    }

    public void enableMessageLogging()
    {
        filters().allVerbs().inbound().messagesMatching((from, to, msg) -> {
            if (!get(1).isShutdown())
            {
                get(1).acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<IMessage>) (msgPassed) -> {
                    Message decoded = Instance.deserializeMessage(msgPassed);
                    if (!toLowerCaseLocalized(decoded.verb().toString()).contains("gossip"))
                        System.out.println(String.format("MSG %d -> %d: %s | %s", from, to, decoded, decoded.payload));
                }).accept(msg);
            }
            return false;
        }).drop().on();
    }

    @Override
    public void disableAutoCompaction(String keyspace)
    {
        stream().forEach(i -> {
            i.acceptsOnInstance(new DisableAutoCompaction()).accept(keyspace);
        });
    }

    // Without this class, lambda is trying to capture too much of the Cluster object, which leads to
    // an attempt to capture unshareable class instances.
    private static class DisableAutoCompaction implements IIsolatedExecutor.SerializableConsumer<String>
    {
        public void accept(String ks)
        {
            for (ColumnFamilyStore cs : Keyspace.open(ks).getColumnFamilyStores())
                cs.disableAutoCompaction();
        }
    }
}

