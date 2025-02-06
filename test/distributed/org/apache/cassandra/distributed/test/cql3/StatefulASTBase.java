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

package org.apache.cassandra.distributed.test.cql3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property;
import accord.utils.RandomSource;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.Bind;
import org.apache.cassandra.cql3.ast.CQLFormatter;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.StandardVisitors;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.cql3.ast.TableReference;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.cql3.ast.Visitor;
import org.apache.cassandra.cql3.ast.Visitor.CompositeVisitor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.model.ASTSingleTableModel;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.distributed.test.JavaDriverUtils.toDriverCL;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;

public class StatefulASTBase extends TestBaseImpl
{
    protected static final EnumSet<KnownIssue> IGNORED_ISSUES = KnownIssue.ignoreAll();
    /**
     * mutations and selects will use operators (eg. {@code 4 + 4}, the + operator), and this will be reflected in the history output.
     *
     * When an issue is found its common to filter out insertions to different partitions/rows but this can become a problem
     * as the issue is for {@code pk=8} but the insert is to {@code 4 + 4}!
     *
     * Setting this to {@code true} will cause all operators to be "applied" or "executored" and the CQL in the history
     * will be the output (eg. {@code 4 + 4 } is replaced with {@code 8}).
     */
    protected static boolean CQL_DEBUG_APPLY_OPERATOR = false;

    protected static final Gen<Gen<Boolean>> BIND_OR_LITERAL_DISTRO = Gens.bools().mixedDistribution();
    protected static final Gen<Gen<Boolean>> BETWEEN_EQ_DISTRO = Gens.bools().mixedDistribution();
    protected static final Gen<Gen<Conditional.Where.Inequality>> LESS_THAN_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                  .filter(i -> i == Conditional.Where.Inequality.LESS_THAN || i == Conditional.Where.Inequality.LESS_THAN_EQ)
                                                                                                                  .collect(Collectors.toList()));
    protected static final Gen<Gen<Conditional.Where.Inequality>> GREATER_THAN_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                     .filter(i -> i == Conditional.Where.Inequality.GREATER_THAN || i == Conditional.Where.Inequality.GREATER_THAN_EQ)
                                                                                                                     .collect(Collectors.toList()));
    protected static final Gen<Gen<Conditional.Where.Inequality>> RANGE_INEQUALITY_DISTRO = Gens.mixedDistribution(Stream.of(Conditional.Where.Inequality.values())
                                                                                                                         .filter(i -> i != Conditional.Where.Inequality.EQUAL && i != Conditional.Where.Inequality.NOT_EQUAL)
                                                                                                                         .collect(Collectors.toList()));
    protected static final Gen<Gen.IntGen> FETCH_SIZE_DISTRO = Gens.mixedDistribution(new int[] {1, 10, 100, 1000, 5000});

    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
        // queries maybe dumb which could lead to performance issues causing timeouts... don't timeout!
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);

        overridePrimitiveTypeSupport(AsciiType.instance, AbstractTypeGenerators.TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 10), stringComparator(AsciiType.instance)));
        overridePrimitiveTypeSupport(UTF8Type.instance, AbstractTypeGenerators.TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 10), stringComparator(UTF8Type.instance)));
        overridePrimitiveTypeSupport(BytesType.instance, AbstractTypeGenerators.TypeSupport.of(BytesType.instance, Generators.bytes(1, 10), FastByteOperations::compareUnsigned));
    }

    /**
     * There is an assumption that keyspace name doesn't impact this test, so to get simpler names use this counter...
     * if this assumption doesn't hold, then need to switch to random or rely on DROP KEYSPACE.
     */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    protected static String nextKeyspace()
    {
        return "ks" + COUNTER.incrementAndGet();
    }

    protected static Cluster createCluster(int nodeCount, Consumer<IInstanceConfig> config) throws IOException
    {
        Cluster cluster = Cluster.build(nodeCount)
                                 .withConfig(c -> {
                                     c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP)
                                      // When drop tables or truncate are performed, we attempt to take snapshots.  This can be costly and isn't needed by these tests
                                      .set("incremental_backups", false);
                                     config.accept(c);
                                 })
                                 .start();
        // we don't allow setting null in yaml... but these configs support null!
        cluster.forEach(i ->  i.runOnInstance(() -> {
            // When values are large SAI will drop them... soooo... disable that... this test does not care about perf but correctness
            DatabaseDescriptor.getRawConfig().sai_frozen_term_size_warn_threshold = null;
            DatabaseDescriptor.getRawConfig().sai_frozen_term_size_fail_threshold = null;
        }));
        return cluster;
    }

    protected <S extends BaseState> Property.StatefulSuccess<S, Void> onSuccess(Logger logger)
    {
        return (state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history));
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> flushTable(RandomSource rs, S state)
    {
        return new Property.SimpleCommand<>("nodetool flush " + state.metadata.keyspace + " " + state.metadata.name, s2 -> {
            s2.cluster.forEach(i -> i.nodetoolResult("flush", s2.metadata.keyspace, s2.metadata.name).asserts().success());
            s2.flush();
        });
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> compactTable(RandomSource rs, S state)
    {
        return new Property.SimpleCommand<>("nodetool compact " + state.metadata.keyspace + " " + state.metadata.name, s2 -> {
            state.cluster.forEach(i -> i.nodetoolResult("compact", s2.metadata.keyspace, s2.metadata.name).asserts().success());
            s2.compact();
        });
    }

    protected static <S extends CommonState> Property.Command<S, Void, ?> insert(RandomSource rs, S state)
    {
        int timestamp = ++state.operations;
        return state.command(rs, state.mutationGen().next(rs).withTimestamp(timestamp));
    }

    protected static <S extends BaseState> Property.Command<S, Void, ?> fullTableScan(RandomSource rs, S state)
    {
        Select select = Select.builder(state.metadata).build();
        return state.command(rs, select, "full table scan");
    }

    protected static abstract class BaseState implements AutoCloseable
    {
        protected final RandomSource rs;
        protected final Cluster cluster;
        protected final com.datastax.driver.core.Cluster client;
        protected final Session session;
        protected final Gen<Boolean> bindOrLiteralGen;
        protected final Gen<Boolean> betweenEqGen;
        protected final Gen<Conditional.Where.Inequality> lessThanGen;
        protected final Gen<Conditional.Where.Inequality> greaterThanGen;
        protected final Gen<Conditional.Where.Inequality> rangeInequalityGen;
        protected final Gen.IntGen fetchSizeGen;
        protected final TableMetadata metadata;
        protected final TableReference tableRef;
        protected final ASTSingleTableModel model;
        private final Visitor debug;
        private final int enoughMemtables;
        private final int enoughSSTables;
        protected int numMutations, mutationsSinceLastFlush;
        protected int numFlushes, flushesSinceLastCompaction;
        protected int numCompact;
        protected int operations;

        protected BaseState(RandomSource rs, Cluster cluster, TableMetadata metadata)
        {
            this.rs = rs;
            this.cluster = cluster;
            int javaDriverTimeout = Math.toIntExact(TimeUnit.MINUTES.toMillis(1));
            this.client = JavaDriverUtils.create(cluster, b -> b.withSocketOptions(new SocketOptions().setReadTimeoutMillis(javaDriverTimeout).setConnectTimeoutMillis(javaDriverTimeout)));
            this.session = client.connect();
            this.debug = CQL_DEBUG_APPLY_OPERATOR ? CompositeVisitor.of(StandardVisitors.APPLY_OPERATOR, StandardVisitors.DEBUG)
                                                  : StandardVisitors.DEBUG;

            this.bindOrLiteralGen = BIND_OR_LITERAL_DISTRO.next(rs);
            this.betweenEqGen = BETWEEN_EQ_DISTRO.next(rs);
            this.lessThanGen = LESS_THAN_DISTRO.next(rs);
            this.greaterThanGen = GREATER_THAN_DISTRO.next(rs);
            this.rangeInequalityGen = RANGE_INEQUALITY_DISTRO.next(rs);
            this.fetchSizeGen = FETCH_SIZE_DISTRO.next(rs);

            this.enoughMemtables = rs.pickInt(3, 10, 50);
            this.enoughSSTables = rs.pickInt(3, 10, 50);

            this.metadata = metadata;
            this.tableRef = TableReference.from(metadata);
            this.model = new ASTSingleTableModel(metadata);
            createTable(metadata);
        }

        protected boolean isMultiNode()
        {
            return cluster.size() > 1;
        }

        protected void createTable(TableMetadata metadata)
        {
            cluster.schemaChange(createKeyspaceCQL(metadata.keyspace));

            CassandraGenerators.visitUDTs(metadata, next -> cluster.schemaChange(next.toCqlString(false, false, true)));
            cluster.schemaChange(metadata.toCqlString(false, false, false));
        }

        private String createKeyspaceCQL(String ks)
        {
            return "CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(3, cluster.size()) + "};";
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Select select)
        {
            return command(rs, select, null);
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Select select, @Nullable String annotate)
        {
            var inst = selectInstance(rs);
            //TODO (coverage): don't limit this to all selects, only those doing range queries!
            int fetchSize = fetchSizeGen.nextInt(rs);
            String postfix = "on " + inst;
            if (fetchSize != Integer.MAX_VALUE)
                postfix += ", fetch size " + fetchSize;
            if (annotate == null) annotate = postfix;
            else                  annotate += ", " + postfix;
            return new Property.SimpleCommand<>(humanReadable(select, annotate), s -> {
                s.model.validate(s.executeQuery(inst, fetchSize, s.selectCl(), select), select);
            });
        }

        protected ConsistencyLevel selectCl()
        {
            return ConsistencyLevel.LOCAL_QUORUM;
        }

        protected ConsistencyLevel mutationCl()
        {
            return ConsistencyLevel.LOCAL_QUORUM;
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Mutation mutation)
        {
            return command(rs, mutation, null);
        }

        protected <S extends BaseState> Property.Command<S, Void, ?> command(RandomSource rs, Mutation mutation, @Nullable String annotate)
        {
            var inst = selectInstance(rs);
            String postfix = "on " + inst;
            if (annotate == null) annotate = postfix;
            else                  annotate += ", " + postfix;
            return new Property.SimpleCommand<>(humanReadable(mutation, annotate), s -> {
                s.executeQuery(inst, Integer.MAX_VALUE, s.mutationCl(), mutation);
                s.model.update(mutation);
                s.mutation();
            });
        }

        protected IInvokableInstance selectInstance(RandomSource rs)
        {
            return cluster.get(rs.nextInt(0, cluster.size()) + 1);
        }

        protected boolean hasEnoughMemtable()
        {
            return mutationsSinceLastFlush > enoughMemtables;
        }

        protected boolean hasEnoughSSTables()
        {
            return flushesSinceLastCompaction > enoughSSTables;
        }

        protected void mutation()
        {
            numMutations++;
            mutationsSinceLastFlush++;
        }

        protected void flush()
        {
            mutationsSinceLastFlush = 0;
            numFlushes++;
            flushesSinceLastCompaction++;
        }

        protected void compact()
        {
            flushesSinceLastCompaction = 0;
            numCompact++;
        }

        protected Value value(RandomSource rs, ByteBuffer bb, AbstractType<?> type)
        {
            return bindOrLiteralGen.next(rs) ? new Bind(bb, type) : new Literal(bb, type);
        }

        protected ByteBuffer[][] executeQuery(IInstance instance, int fetchSize, ConsistencyLevel cl, Statement stmt)
        {
            if (cl == ConsistencyLevel.NODE_LOCAL)
            {
                // This limitation is due to the fact the query column types are not known in the current QueryResult API.
                // In order to fix this we need to alter the API, and backport to each branch else we break upgrade.
                if (!(stmt instanceof Mutation))
                    throw new IllegalArgumentException("Unable to execute Statement of type " + stmt.getClass() + " when ConsistencyLevel.NODE_LOCAL is used");
                if (fetchSize != Integer.MAX_VALUE)
                    throw new IllegalArgumentException("Fetch size is not allowed for Mutations");
                instance.executeInternal(stmt.toCQL(), (Object[]) stmt.bindsEncoded());
                return new ByteBuffer[0][];
            }
            else
            {
                SimpleStatement ss = new SimpleStatement(stmt.toCQL(), (Object[]) stmt.bindsEncoded());
                if (fetchSize != Integer.MAX_VALUE)
                    ss.setFetchSize(fetchSize);
                ss.setConsistencyLevel(toDriverCL(cl));

                InetSocketAddress broadcastAddress = instance.config().broadcastAddress();
                var host = client.getMetadata().getAllHosts().stream()
                                 .filter(h -> h.getBroadcastSocketAddress().getAddress().equals(broadcastAddress.getAddress()))
                                 .filter(h -> h.getBroadcastSocketAddress().getPort() == broadcastAddress.getPort())
                                 .findAny()
                                 .get();
                ss.setHost(host);
                ResultSet result = session.execute(ss);
                return getRowsAsByteBuffer(result);
            }
        }

        @VisibleForTesting
        static ByteBuffer[][] getRowsAsByteBuffer(ResultSet result)
        {
            ColumnDefinitions columns = result.getColumnDefinitions();
            List<ByteBuffer[]> ret = new ArrayList<>();
            for (Row rowVal : result)
            {
                ByteBuffer[] row = new ByteBuffer[columns.size()];
                for (int i = 0; i < columns.size(); i++)
                    row[i] = rowVal.getBytesUnsafe(i);
                ret.add(row);
            }
            ByteBuffer[][] a = new ByteBuffer[ret.size()][];
            return ret.toArray(a);
        }

        private String humanReadable(Statement stmt, @Nullable String annotate)
        {
            // With UTF-8 some chars can cause printing issues leading to error messages that don't reproduce the original issue.
            // To avoid this problem, always escape the CQL so nothing gets lost
            String cql = StringUtils.escapeControlChars(stmt.visit(debug).toCQL(CQLFormatter.None.instance));
            if (annotate != null)
                cql += " -- " + annotate;
            return cql;
        }

        protected void toString(StringBuilder sb)
        {
            sb.append(createKeyspaceCQL(metadata.keyspace));
            CassandraGenerators.visitUDTs(metadata, udt -> sb.append('\n').append(udt.toCqlString(false, false, true)).append(';'));
            sb.append('\n').append(metadata.toCqlString(false, false, false));
        }

        @Override
        public void close() throws Exception
        {
            session.close();
            client.close();
            cluster.schemaChange("DROP TABLE " + metadata);
            cluster.schemaChange("DROP KEYSPACE " + metadata.keyspace);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            toString(sb);
            return sb.toString();
        }

        private static final class ValueWithType
        {
            final ByteBuffer value;
            final AbstractType type;

            private ValueWithType(ByteBuffer value, AbstractType<?> type)
            {
                this.value = value;
                this.type = type;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ValueWithType value1 = (ValueWithType) o;
                return value.equals(value1.value) && type.equals(value1.type);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(value, type);
            }

            @Override
            public String toString()
            {
                return type.toCQLString(value);
            }
        }
    }

    protected static abstract class CommonState extends BaseState
    {
        protected CommonState(RandomSource rs, Cluster cluster, TableMetadata metadata)
        {
            super(rs, cluster, metadata);
        }

        protected abstract Gen<Mutation> mutationGen();
    }
}
