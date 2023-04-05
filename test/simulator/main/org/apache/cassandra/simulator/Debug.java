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

package org.apache.cassandra.simulator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.TriFunction;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.function.Function.identity;
import static org.apache.cassandra.simulator.Action.Modifier.INFO;
import static org.apache.cassandra.simulator.Action.Modifier.WAKEUP;
import static org.apache.cassandra.simulator.ActionListener.runAfter;
import static org.apache.cassandra.simulator.ActionListener.runAfterAndTransitivelyAfter;
import static org.apache.cassandra.simulator.ActionListener.recursive;
import static org.apache.cassandra.simulator.Debug.EventType.*;
import static org.apache.cassandra.simulator.Debug.Info.LOG;
import static org.apache.cassandra.simulator.Debug.Level.*;
import static org.apache.cassandra.simulator.paxos.Ballots.paxosDebugInfo;

// TODO (feature): move logging to a depth parameter
// TODO (feature): log only deltas for schema/cluster data
public class Debug
{
    private static final Logger logger = LoggerFactory.getLogger(Debug.class);

    public enum EventType { PARTITION, CLUSTER }
    public enum Level
    {
        PLANNED,
        CONSEQUENCES,
        ALL;

        private static final Level[] LEVELS = values();
    }
    public enum Info
    {
        LOG(EventType.values()),
        PAXOS(PARTITION),
        OWNERSHIP(CLUSTER),
        GOSSIP(CLUSTER),
        RF(CLUSTER),
        RING(CLUSTER);

        public final EventType[] defaultEventTypes;

        Info(EventType ... defaultEventTypes)
        {
            this.defaultEventTypes = defaultEventTypes;
        }
    }

    public static class Levels
    {
        private final EnumMap<EventType, Level> levels;

        public Levels(EnumMap<EventType, Level> levels)
        {
            this.levels = levels;
        }

        public Levels(Level level, EventType ... types)
        {
            this.levels = new EnumMap<>(EventType.class);
            for (EventType type : types)
                this.levels.put(type, level);
        }

        public Levels(int partition, int cluster)
        {
            this.levels = new EnumMap<>(EventType.class);
            if (partition > 0) this.levels.put(PARTITION, Level.LEVELS[partition - 1]);
            if (cluster > 0) this.levels.put(CLUSTER, Level.LEVELS[cluster - 1]);
        }

        Level get(EventType type)
        {
            return levels.get(type);
        }

        boolean anyMatch(Predicate<Level> test)
        {
            return levels.values().stream().anyMatch(test);
        }
    }

    private final EnumMap<Info, Levels> levels;
    public final int[] primaryKeys;

    public Debug()
    {
        this(new EnumMap<>(Info.class), null);
    }

    public Debug(Map<Info, Levels> levels, int[] primaryKeys)
    {
        this.levels = new EnumMap<>(levels);
        this.primaryKeys = primaryKeys;
    }

    public ActionListener debug(EventType type, SimulatedTime time, Cluster cluster, String keyspace, Integer primaryKey)
    {
        List<ActionListener> listeners = new ArrayList<>();
        for (Map.Entry<Info, Levels> e : levels.entrySet())
        {
            Info info = e.getKey();
            Level level = e.getValue().get(type);
            if (level == null) continue;

            ActionListener listener;
            if (info == LOG)
            {
                Function<ActionListener, ActionListener> adapt = type == CLUSTER ? LogTermination::new : identity();
                switch (level)
                {
                    default: throw new AssertionError();
                    case PLANNED: listener = adapt.apply(new LogOne(time, false)); break;
                    case CONSEQUENCES: case ALL: listener = adapt.apply(recursive(new LogOne(time, true))); break;
                }
            }
            else if (keyspace != null)
            {
                Consumer<Action> debug;
                switch (info)
                {
                    default: throw new AssertionError();
                    case GOSSIP: debug = debugGossip(cluster); break;
                    case RF: debug = debugRf(cluster, keyspace); break;
                    case RING: debug = debugRing(cluster, keyspace); break;
                    case PAXOS: debug = forKeys(cluster, keyspace, primaryKey, Debug::debugPaxos); break;
                    case OWNERSHIP: debug = forKeys(cluster, keyspace, primaryKey, Debug::debugOwnership); break;
                }
                switch (level)
                {
                    default: throw new AssertionError();
                    case PLANNED: listener = type == CLUSTER ? runAfterAndTransitivelyAfter(debug) : runAfter(debug); break;
                    case CONSEQUENCES: listener = recursive(runAfter(ignoreWakeupAndLogEvents(debug))); break;
                    case ALL: listener = recursive(runAfter(ignoreLogEvents(debug))); break;
                }
            }
            else continue;

            listeners.add(listener);
        }

        if (listeners.isEmpty())
            return null;
        return new ActionListener.Combined(listeners);
    }

    public boolean isOn(Info info)
    {
        return isOn(info, PLANNED);
    }

    public boolean isOn(Info info, Level level)
    {
        Levels levels = this.levels.get(info);
        if (levels == null) return false;
        return levels.anyMatch(test -> level.compareTo(test) >= 0);
    }

    @SuppressWarnings("UnnecessaryToStringCall")
    private static class LogOne implements ActionListener
    {
        final SimulatedTime time;
        final boolean logConsequences;
        private LogOne(SimulatedTime time, boolean logConsequences)
        {
            this.time = time;
            this.logConsequences = logConsequences;
        }

        @Override
        public void before(Action action, Before before)
        {
            if (logger.isWarnEnabled()) // invoke toString() eagerly to ensure we have the task's descriptin
                logger.warn(String.format("%6ds %s %s", TimeUnit.NANOSECONDS.toSeconds(time.nanoTime()), before, action));
        }

        @Override
        public void consequences(ActionList consequences)
        {
            if (logConsequences && !consequences.isEmpty() && logger.isWarnEnabled())
                logger.warn(String.format("%6ds Next: %s", TimeUnit.NANOSECONDS.toSeconds(time.nanoTime()), consequences));
        }
    }

    private static class LogTermination extends ActionListener.Wrapped
    {
        public LogTermination(ActionListener wrap)
        {
            super(wrap);
        }

        @Override
        public void transitivelyAfter(Action finished)
        {
            logger.warn("Terminated {}", finished);
        }
    }

    private static Consumer<Action> ignoreWakeupAndLogEvents(Consumer<Action> consumer)
    {
        return action -> {
            if (!action.is(WAKEUP) && !action.is(INFO))
                consumer.accept(action);
        };
    }

    private static Consumer<Action> ignoreLogEvents(Consumer<Action> consumer)
    {
        return action -> {
            if (!action.is(INFO))
                consumer.accept(action);
        };
    }

    private Consumer<Action> debugGossip(Cluster cluster)
    {
        return ignore -> {
            cluster.forEach(i -> i.unsafeRunOnThisThread(() -> {
                for (InetAddressAndPort ep : Gossiper.instance.getLiveMembers())
                {
                    EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
                    logger.warn("Gossip {}: {} {}", ep, epState.isAlive(), epState.states().stream()
                                                                                   .map(e -> e.getKey().toString() + "=(" + e.getValue().value + ',' + e.getValue().version + ')')
                                                                                   .collect(Collectors.joining(", ", "[", "]")));
                }
            }));
        };
    }

    private Consumer<Action> forKeys(Cluster cluster, String keyspace, @Nullable Integer specificPrimaryKey, TriFunction<Cluster, String, Integer, Consumer<Action>> factory)
    {
        if (specificPrimaryKey != null) return factory.apply(cluster, keyspace, specificPrimaryKey);
        else return forEachKey(cluster, keyspace, primaryKeys, Debug::debugPaxos);
    }

    public static Consumer<Action> forEachKey(Cluster cluster, String keyspace, int[] primaryKeys, TriFunction<Cluster, String, Integer, Consumer<Action>> factory)
    {
        Consumer<Action>[] eachKey = new Consumer[primaryKeys.length];
        for (int i = 0 ; i < primaryKeys.length ; ++i)
            eachKey[i] = factory.apply(cluster, keyspace, primaryKeys[i]);

        return action -> {
            for (Consumer<Action> run : eachKey)
                run.accept(action);
        };
    }

    public static Consumer<Action> debugPaxos(Cluster cluster, String keyspace, int primaryKey)
    {
        return ignore -> {
            for (int node = 1 ; node <= cluster.size() ; ++node)
            {
                cluster.get(node).unsafeAcceptOnThisThread((num, pkint) -> {
                    try
                    {
                        TableMetadata metadata = Keyspace.open(keyspace).getColumnFamilyStore("tbl").metadata.get();
                        ByteBuffer pkbb = Int32Type.instance.decompose(pkint);
                        DecoratedKey key = new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(pkbb), pkbb);
                        logger.warn("node{}({}): {}", num, primaryKey, paxosDebugInfo(key, metadata, FBUtilities.nowInSeconds()));
                    }
                    catch (Throwable t)
                    {
                        logger.warn("node{}({})", num, primaryKey, t);
                    }
                }, node, primaryKey);
            }
        };
    }

    public static Consumer<Action> debugRf(Cluster cluster, String keyspace)
    {
        return ignore -> {
            cluster.forEach(i -> i.unsafeRunOnThisThread(() -> {
                logger.warn("{} {}",
                        Schema.instance.getKeyspaceMetadata(keyspace) == null ? "" : Schema.instance.getKeyspaceMetadata(keyspace).params.replication.toString(),
                        Schema.instance.getKeyspaceMetadata(keyspace) == null ? "" : Keyspace.open(keyspace).getReplicationStrategy().configOptions.toString());
            }));
        };
    }

    public static Consumer<Action> debugOwnership(Cluster cluster, String keyspace, int primaryKey)
    {
        return ignore -> {
            for (int node = 1 ; node <= cluster.size() ; ++node)
            {
                logger.warn("node{}({}): {}", node, primaryKey, cluster.get(node).unsafeApplyOnThisThread(v -> {
                    try
                    {
                        return ReplicaLayout.forTokenWriteLiveAndDown(Keyspace.open(keyspace), Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(v))).all().endpointList().toString();
                    }
                    catch (Throwable t)
                    {
                        return "Error";
                    }
                }, primaryKey));
            }
        };
    }

    public static Consumer<Action> debugRing(Cluster cluster, String keyspace)
    {
        return ignore -> cluster.forEach(i -> i.unsafeRunOnThisThread(() -> {
            if (Schema.instance.getKeyspaceMetadata(keyspace) != null)
                logger.warn("{}", StorageService.instance.getTokenMetadata().toString());
        }));
    }

}
