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

package org.apache.cassandra.distributed.test;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import com.google.common.collect.Iterators;
import org.junit.Assert;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;

public class ReprepareTestBase extends TestBaseImpl
{
    protected static ReprepareTestConfiguration cfg(boolean withUse, boolean skipBrokenBehaviours)
    {
        return new ReprepareTestConfiguration(withUse, skipBrokenBehaviours);
    }

    public void testReprepare(BiConsumer<ClassLoader, Integer> instanceInitializer, ReprepareTestConfiguration... configs) throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(instanceInitializer)
                                                            .start()))
        {
            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            for (ReprepareTestConfiguration config : configs)
            {
                // 1 has old behaviour
                for (int firstContact : new int[]{ 1, 2 })
                {
                    try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                    .addContactPoint("127.0.0.1")
                                                                                                    .addContactPoint("127.0.0.2")
                                                                                                    .withLoadBalancingPolicy(lbp)
                                                                                                    .build();
                         Session session = cluster.connect())
                    {
                        lbp.setPrimary(firstContact);
                        final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                        session.execute(select.bind());

                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact == 1 ? 2 : 1);

                        if (config.withUse)
                            session.execute(withKeyspace("USE %s"));

                        // Re-preparing on the node
                        if (!config.skipBrokenBehaviours && firstContact == 1)
                            session.execute(select.bind());

                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact);

                        // Re-preparing on the node with old behaviour will break no matter where the statement was initially prepared
                        if (!config.skipBrokenBehaviours)
                            session.execute(select.bind());

                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));
                    }
                }
            }
        }
    }

    public void testReprepareTwoKeyspaces(BiConsumer<ClassLoader, Integer> instanceInitializer) throws Throwable
    {
        try (ICluster<IInvokableInstance> c = init(builder().withNodes(2)
                                                            .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                                            .withInstanceInitializer(instanceInitializer)
                                                            .start()))
        {
            c.schemaChange(withKeyspace("CREATE KEYSPACE %s2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            ForceHostLoadBalancingPolicy lbp = new ForceHostLoadBalancingPolicy();

            for (int firstContact : new int[]{ 1, 2 })
                try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                                                                .addContactPoint("127.0.0.1")
                                                                                                .addContactPoint("127.0.0.2")
                                                                                                .withLoadBalancingPolicy(lbp)
                                                                                                .build();
                     Session session = cluster.connect())
                {
                    {
                        session.execute(withKeyspace("USE %s"));
                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact);
                        final PreparedStatement select = session.prepare(withKeyspace("SELECT * FROM %s.tbl"));
                        session.execute(select.bind());

                        c.stream().forEach((i) -> i.runOnInstance(QueryProcessor::clearPreparedStatementsCache));

                        lbp.setPrimary(firstContact == 1 ? 2 : 1);
                        session.execute(withKeyspace("USE %s2"));
                        try
                        {
                            session.execute(select.bind());
                        }
                        catch (DriverInternalError e)
                        {
                            Assert.assertTrue(e.getCause().getMessage().contains("can't execute it on"));
                            continue;
                        }
                        fail("Should have thrown");
                    }
                }
        }
    }

    protected static class ReprepareTestConfiguration
    {
        protected final boolean withUse;
        protected final boolean skipBrokenBehaviours;

        protected ReprepareTestConfiguration(boolean withUse, boolean skipBrokenBehaviours)
        {
            this.withUse = withUse;
            this.skipBrokenBehaviours = skipBrokenBehaviours;
        }
    }

    public static class PrepareBehaviour
    {
        protected static void setReleaseVersion(ClassLoader cl, String value)
        {
            new ByteBuddy().rebase(FBUtilities.class)
                           .method(named("getReleaseVersionString"))
                           .intercept(FixedValue.value(value))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        static void newBehaviour(ClassLoader cl, int nodeNumber)
        {
            setReleaseVersion(cl, QueryProcessor.NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40.toString());
        }

        static void oldBehaviour(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(QueryProcessor.class) // note that we need to `rebase` when we use @SuperCall
                               .method(named("prepare").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(PrepareBehaviour.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
                setReleaseVersion(cl, "4.0.0.0");
            }
            else
            {
                setReleaseVersion(cl, QueryProcessor.NEW_PREPARED_STATEMENT_BEHAVIOUR_SINCE_40.toString());
            }
        }

        public static ResultMessage.Prepared prepare(String queryString, ClientState clientState)
        {
            ResultMessage.Prepared existing = QueryProcessor.getStoredPreparedStatement(queryString, clientState.getRawKeyspace());
            if (existing != null)
                return existing;

            QueryHandler.Prepared prepared = QueryProcessor.parseAndPrepare(queryString, clientState, false);

            int boundTerms = prepared.statement.getBindVariables().size();
            if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
                throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));

            return QueryProcessor.storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared);
        }
    }

    protected static class ForceHostLoadBalancingPolicy implements LoadBalancingPolicy {

        protected final List<Host> hosts = new CopyOnWriteArrayList<Host>();
        protected int currentPrimary = 0;

        public void setPrimary(int idx) {
            this.currentPrimary = idx - 1; // arrays are 0-based
        }

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            this.hosts.addAll(hosts);
            this.hosts.sort(Comparator.comparingInt(h -> h.getAddress().getAddress()[3]));
        }

        @Override
        public HostDistance distance(Host host) {
            return HostDistance.LOCAL;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            if (hosts.isEmpty()) return Collections.emptyIterator();
            return Iterators.singletonIterator(hosts.get(currentPrimary));
        }

        @Override
        public void onAdd(Host host) {
            onUp(host);
        }

        @Override
        public void onUp(Host host) {
            hosts.add(host);
        }

        @Override
        public void onDown(Host host) {
            // no-op
        }

        @Override
        public void onRemove(Host host) {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }
}