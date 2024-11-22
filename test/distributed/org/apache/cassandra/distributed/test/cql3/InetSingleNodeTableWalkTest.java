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

import java.net.InetAddress;
import javax.annotation.Nullable;

import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.quicktheories.core.Gen;

public class InetSingleNodeTableWalkTest extends SingleNodeTableWalkTest
{
    static
    {
        IGNORED_ISSUES.remove(KnownIssue.SAI_INET_MIXED);
    }

    public InetSingleNodeTableWalkTest()
    {
        this(null);
    }

    protected InetSingleNodeTableWalkTest(@Nullable TransactionalMode transactionalMode)
    {
        super(transactionalMode);
    }

    @Override
    protected void preCheck(Property.StatefulBuilder builder)
    {
        // if a failing seed is detected, populate here
        // Example: builder.withSeed(42L);
//        builder.withSeed(3985593186746556237L);

        builder.withSeed(-7293505339069640960L); // ipv6 allow fitering missing partition
    }

    @Override
    protected AbstractTypeGenerators.TypeGenBuilder supportedTypes()
    {
        return AbstractTypeGenerators.withoutUnsafeEquality(AbstractTypeGenerators.builder()
                                                                                  .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE)
                                                                                  .withPrimitives(InetAddressType.instance));
    }

    private enum Mode { ipv4, ipv6, mixed }

    @Override
    protected InetState createState(RandomSource rs, Cluster cluster)
    {
//        Mode mode = rs.pick(Mode.values());
        Mode mode = rs.pick(Mode.ipv4, Mode.ipv6);
//        Mode mode = Mode.mixed;
        Gen<InetAddress> gen;
        switch (mode)
        {
            case ipv4:
                gen = Generators.INET_4_ADDRESS_UNRESOLVED_GEN;
                break;
            case ipv6:
                gen = Generators.INET_6_ADDRESS_UNRESOLVED_GEN;
                break;
            case mixed:
                gen = Generators.INET_ADDRESS_UNRESOLVED_GEN;
                break;
            default:
                throw new UnsupportedOperationException(mode.name());
        }
        var support = TypeSupport.of(InetAddressType.instance,
                                     gen,
                                     (a, b) -> FastByteOperations.compareUnsigned(a.getAddress(), b.getAddress())); // serialization strips the hostname, only keeps the address
        AbstractTypeGenerators.overridePrimitiveTypeSupport(InetAddressType.instance, support);
        return new InetState(rs, cluster, mode);
    }

    public class InetState extends State
    {
        private final Mode mode;

        public InetState(RandomSource rs, Cluster cluster, Mode mode)
        {
            super(rs, cluster);
            this.mode = mode;
        }

        @Override
        public String toString()
        {
            return "Mode: " + mode + "\n" + super.toString();
        }
    }
}
