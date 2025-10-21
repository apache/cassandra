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

import accord.utils.Gen;
import accord.utils.RandomSource;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.KnownIssue;
import org.apache.cassandra.cql3.ast.CasCondition;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.ASTGenerators;

import static org.apache.cassandra.utils.Generators.toGen;

public abstract class CasMultiNodeTableWalkBase extends MultiNodeTableWalkBase
{
    protected final Config.PaxosVariant paxos_variant;

    protected CasMultiNodeTableWalkBase(Config.PaxosVariant paxos_variant)
    {
        super(ReadRepairStrategy.NONE);
        this.paxos_variant = paxos_variant;
    }

    @Override
    protected void clusterConfig(IInstanceConfig c)
    {
        super.clusterConfig(c);
        c.set("paxos_variant", paxos_variant);
        c.set("cas_contention_timeout", "180s");
        //TODO (now): should these be included?  They are in the benchmark clusters
//        c.set("paxos_contention_min_wait", 0);
//        c.set("paxos_contention_max_wait", "100ms");
//        c.set("paxos_contention_min_delta", 0);
    }

    @Override
    protected SingleNodeTableWalkTest.State createState(RandomSource rs, Cluster cluster)
    {
        return new State(rs, cluster);
    }

    private static boolean isValueUDTSafe(Value value)
    {
        var bb = value.valueEncoded();
        return bb == null ? true : bb.hasRemaining();
    }

    protected class State extends MultiNodeState
    {
        private State(RandomSource rs, Cluster cluster)
        {
            super(rs, cluster);
        }

        @Override
        protected Gen<Mutation> toMutationGen(ASTGenerators.MutationGenBuilder mutationGenBuilder)
        {
            mutationGenBuilder.withCasGen(i -> true)
                              .disallowUpdateMultipleRows(); // paxos supports but the model doesn't yet
            // generator might not always generate a cas statement... should fix generator!
            Gen<Mutation> gen = toGen(mutationGenBuilder.build()).filter(Mutation::isCas);
            if (metadata.regularAndStaticColumns().stream().anyMatch(c -> c.type.isUDT())
                && IGNORED_ISSUES.contains(KnownIssue.CAS_CONDITION_ON_UDT_W_EMPTY_BYTES))
            {
                gen = gen.filter(m -> {
                    CasCondition condition;
                    switch (m.kind)
                    {
                        case INSERT:
                            return true;
                        case DELETE:
                            condition = ((Mutation.Delete) m).casCondition.get();
                            break;
                        case UPDATE:
                            condition = ((Mutation.Update) m).casCondition.get();
                            break;
                        default:
                            throw new UnsupportedOperationException(m.kind.name());
                    }
                    return !condition.streamRecursive(true).anyMatch(e -> {
                        if (!(e instanceof Conditional.Where)) return false;
                        var where = (Conditional.Where) e;
                        if (!where.lhs.type().isUDT()) return false;
                        if (where.lhs instanceof Value && !isValueUDTSafe((Value) where.lhs))
                            return true;
                        if (where.rhs instanceof Value && !isValueUDTSafe((Value) where.rhs))
                            return true;
                        return false;
                    });
                });
            }
            return gen;
        }

        @Override
        protected ConsistencyLevel selectCl()
        {
            return ConsistencyLevel.SERIAL;
        }

        @Override
        protected ConsistencyLevel mutationCl()
        {
            return ConsistencyLevel.SERIAL;
        }
    }
}
