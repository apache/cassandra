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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.messages.AbstractRequest;
import net.openhft.chronicle.core.util.SerializablePredicate;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageSink;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordCache;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_GC_INSPECTOR;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.assertj.core.api.Assertions.fail;

// checkstyle: suppress below 'blockSystemPropertyUsage'
public class TestBaseImpl extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TestBaseImpl.class);

    public static final Object[][] EMPTY_ROWS = new Object[0][];
    public static final boolean[] BOOLEANS = new boolean[]{ false, true };

    private static final AtomicLong ZERO = new AtomicLong();
    protected static final Map<Verb, AtomicLong> messageCounts = new ConcurrentHashMap<>();

    protected static class MessageCountingSink implements IMessageSink
    {
        private final Cluster cluster;
        private final SerializablePredicate<Message<?>> filter;

        // This isn't perfect at excluding messages so make sure it excludes the ones you care about in your test
        public static final SerializablePredicate<Message<?>> EXCLUDE_SYNC_POINT_MESSAGES = message -> {
            if (message.payload instanceof AbstractRequest)
                return !((AbstractRequest<?>)message.payload).txnId.isSyncPoint();
            return true;
        };

        public MessageCountingSink(Cluster cluster)
        {
            this(cluster, message -> true);
        }

        public MessageCountingSink(Cluster cluster, SerializablePredicate<Message<?>> filter)
        {
            this.cluster = cluster;
            this.filter = filter;
        }

        @Override
        public void accept(InetSocketAddress to, IMessage iMessage)
        {
            Verb verb = Verb.fromId(iMessage.verb());
            logger.debug("verb {} to {}", verb, to);
            SerializablePredicate<Message<?>> filter = this.filter;
            // Do some gymnastics to evaluate the predicate because deserialization fails if done here
            // This races with shutdown so when attempting to count/deliver to shut down nodes ignore the exception
            // and count anyways since we can't do much better
            boolean accepted;
            try
            {
                accepted = cluster.get(to).unsafeCallOnThisThread(() -> {
                    try (DataInputBuffer in = new DataInputBuffer(iMessage.bytes()))
                    {
                        Message<?> message =  Message.serializer.deserialize(in, toCassandraInetAddressAndPort(iMessage.from()), iMessage.version());
                        return filter.test(message);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }
            catch (IllegalStateException e)
            {
                if (e.getMessage().startsWith("Can't use shutdown "))
                    return;
                throw e;
            }
            if (accepted)
            {
                messageCounts.computeIfAbsent(verb, ignored -> new AtomicLong()).incrementAndGet();
                cluster.get(to).receiveMessage(iMessage);
            }
        }
    }

    // Only works if MessageCountingSink is set on the cluster
    public static long messageCount(Verb verb)
    {
        return messageCounts.getOrDefault(verb, ZERO).get();
    }

    @After
    public void afterEach() {
        super.afterEach();
        messageCounts.clear();
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.SIMULATOR_STARTED.setString(Long.toString(MILLISECONDS.toSeconds(currentTimeMillis())));
        ICluster.setup();
        SKIP_GC_INSPECTOR.setBoolean(true);
        AccordCache.validateLoadOnEvict(true);
    }

    @Override
    public Cluster.Builder builder() {
        // This is definitely not the smartest solution, but given the complexity of the alternatives and low risk, we can just rely on the
        // fact that this code is going to work accross _all_ versions.
        return Cluster.build();
    }

    public static Object[][] rows(Object[]...rows)
    {
        Object[][] r = new Object[rows.length][];
        System.arraycopy(rows, 0, r, 0, rows.length);
        return r;
    }

    public static Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    public static Object set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    public static Object map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("Invalid number of arguments, got " + values.length);

        int size = values.length / 2;
        Map<Object, Object> m = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    public static ByteBuffer tuple(Object... values)
    {
        List<AbstractType<?>> types = new ArrayList<>(values.length);
        List<ByteBuffer> bbs = new ArrayList<>(values.length);
        for (Object value : values)
        {
            AbstractType type = typeFor(value);
            types.add(type);
            bbs.add(value == null ? null : type.decompose(value));
        }
        TupleType tupleType = new TupleType(types);
        return tupleType.pack(bbs, ByteBufferAccessor.instance);
    }

    public static String unloggedBatch(String... queries)
    {
        return batch(false, queries);
    }

    public static String batch(boolean logged, String... queries)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        sb.append(logged ? "" : "UNLOGGED ");
        sb.append("BATCH\n");
        for (String q : queries)
            sb.append(q).append(";\n");
        sb.append("APPLY BATCH;");
        return sb.toString();
    }

    protected void bootstrapAndJoinNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        IInvokableInstance newInstance = cluster.bootstrap(config);
        RESET_BOOTSTRAP_PROGRESS.setBoolean(false);
        withProperty(JOIN_RING, false,
                     () -> newInstance.startup(cluster));
        newInstance.nodetoolResult("join").asserts().success();
        newInstance.nodetoolResult("cms", "describe").asserts().success(); // just make sure we're joined, remove later
    }

    @SuppressWarnings("unchecked")
    private static ByteBuffer makeByteBuffer(Object value)
    {
        if (value == null)
            return null;

        if (value instanceof ByteBuffer)
            return (ByteBuffer) value;

        return typeFor(value).decompose(value);
    }

    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value == null)
            return BytesType.instance;

        if (value instanceof Byte)
            return ByteType.instance;

        if (value instanceof Short)
            return ShortType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Duration)
            return DurationType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof BigInteger)
            return IntegerType.instance;

        if (value instanceof BigDecimal)
            return DecimalType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof Boolean)
            return BooleanType.instance;

        if (value instanceof InetAddress)
            return InetAddressType.instance;

        if (value instanceof Date)
            return TimestampType.instance;

        if (value instanceof UUID)
            return UUIDType.instance;

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ')');
    }

    public static void fixDistributedSchemas(Cluster cluster)
    {
        // These keyspaces are under replicated by default, so must be updated when doing a multi-node cluster;
        // else bootstrap will fail with 'Unable to find sufficient sources for streaming range <range> in keyspace <name>'
        Map<String, Long> dcCounts = cluster.stream()
                                            .map(i -> i.config().localDatacenter())
                                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        String replica = "{'class': 'NetworkTopologyStrategy'";
        for (Map.Entry<String, Long> e : dcCounts.entrySet())
        {
            String dc = e.getKey();
            int rf = Math.min(e.getValue().intValue(), 3);
            replica += ", '" + dc + "': " + rf;
        }
        replica += "}";
        for (String ks : Arrays.asList("system_auth", "system_traces"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks + " WITH REPLICATION = " + replica);
        }

        // in real live repair is needed in this case, but in the test case it doesn't matter if the tables loose
        // anything, so ignoring repair to speed up the tests.
    }

    protected static void disableCompaction(Cluster cluster, String keyspace, String table)
    {
        for (int i = 1; i < cluster.size() + 1; i++)
            cluster.get(i).nodetool("disableautocompaction", keyspace, table);
    }

    public static String nodetool(IInstance instance, String... commandAndArgs)
    {
        NodeToolResult nodetoolResult = instance.nodetoolResult(commandAndArgs);
        if (!nodetoolResult.getStdout().isEmpty())
            System.out.println(nodetoolResult.getStdout());
        if (!nodetoolResult.getStderr().isEmpty())
            System.err.println(nodetoolResult.getStderr());
        if (nodetoolResult.getError() != null)
            fail("Failed nodetool " + Arrays.asList(commandAndArgs), nodetoolResult.getError());
        // TODO why does standard out end up in stderr in nodetool?
        return nodetoolResult.getStdout();
    }

    public static String nodetool(ICoordinator coordinator, String... commandAndArgs)
    {
        return nodetool(coordinator.instance(), commandAndArgs);
    }

    public static ListenableFuture<String> nodetoolAsync(IInstance instance, String... commandAndArgs)
    {
        return nodetoolAsync(instance.coordinator(), commandAndArgs);
    }

    public static ListenableFuture<String> nodetoolAsync(ICoordinator coordinator, String... commandAndArgs)
    {
        ListenableFutureTask<String> task = ListenableFutureTask.create(() -> nodetool(coordinator, commandAndArgs));
        Thread asyncThread = new Thread(task, "NodeTool: " + Arrays.asList(commandAndArgs));
        asyncThread.setDaemon(true);
        asyncThread.start();
        return task;
    }

    /**
     * @see org.apache.cassandra.cql3.CQLTester#wrapInTxn(String...)
     */
    protected static String wrapInTxn(String... stmts)
    {
        return wrapInTxn(Arrays.asList(stmts));
    }

    protected static String wrapInTxn(List<String> stmts)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN TRANSACTION\n");
        for (String stmt : stmts)
        {
            sb.append('\t').append(stmt);
            if (!stmt.endsWith(";"))
                sb.append(';');
            sb.append('\n');
        }
        sb.append("COMMIT TRANSACTION");
        return sb.toString();
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     *
     * @param cluster the tested cluster
     * @param keyspace the keyspace to flush
     * @param runnable the test to run
     */
    public static void beforeAndAfterFlush(Cluster cluster, String keyspace, CQLTester.CheckedFunction runnable) throws Throwable
    {
        try
        {
            runnable.apply();
        }
        catch (Throwable t)
        {
            throw new AssertionError("Test failed before flush:\n" + t, t);
        }

        for (int i = 1; i <= cluster.size(); i++)
        {
            cluster.get(i).flush(keyspace);

            try
            {
                runnable.apply();
            }
            catch (Throwable t)
            {
                throw new AssertionError("Test failed after flushing node " + i + ":\n" + t, t);
            }
        }
    }
}
