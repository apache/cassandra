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
package org.apache.cassandra.service.reads.thresholds;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.service.reads.thresholds.WarningsSnapshot.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class WarningsSnapshotTest
{
    private static final InetAddressAndPort HOME = address(127, 0, 0, 1);
    private static final InetAddressAndPort VACATION_HOME = address(127, 0, 0, 2);

    @Test
    public void staticMergeEmtpy()
    {
        WarningsSnapshot result = merge(null, empty(), null, empty());
        assertThat(result).isNull();
    }

    @Test
    public void staticMergeNonEmtpy()
    {
        qt().forAll(nonEmpty(), nonEmpty()).check((a, b) -> {
            WarningsSnapshot result = merge(a, b, null, empty());
            return result != null && !result.isEmpty();
        });
    }

    @Test
    public void mergeEmtpy()
    {
        WarningsSnapshot result = empty().merge(empty());
        assertThat(result).isEqualTo(empty());
    }

    @Test
    public void mergeSelf()
    {
        qt().forAll(all()).check(self -> self.merge(self).equals(self));
    }

    @Test
    public void mergeSelfWithEmpty()
    {
        qt().forAll(all()).check(self -> self.merge(empty()).equals(self) && empty().merge(self).equals(self));
    }

    @Test
    public void mergeNonEmpty()
    {
        WarningsSnapshot expected = builder()
                                    .tombstonesAbort(ImmutableSet.of(HOME), 42)
                                    .localReadSizeWarning(ImmutableSet.of(VACATION_HOME), 12)
                                    .build();
        // validate builder to protect against empty = empty passing this test
        assertThat(expected.tombstones.aborts.instances).isEqualTo(ImmutableSet.of(HOME));
        assertThat(expected.tombstones.aborts.maxValue).isEqualTo(42);
        assertThat(expected.localReadSize.warnings.instances).isEqualTo(ImmutableSet.of(VACATION_HOME));
        assertThat(expected.localReadSize.warnings.maxValue).isEqualTo(12);

        WarningsSnapshot output = empty().merge(expected);
        assertThat(output).isEqualTo(expected).isEqualTo(expected.merge(empty()));
        assertThat(output.merge(expected)).isEqualTo(expected);
    }

    @Test
    public void mergeNonEmpty2()
    {
        WarningsSnapshot a = builder()
                             .tombstonesAbort(ImmutableSet.of(HOME), 42)
                             .build();
        WarningsSnapshot b = builder()
                             .localReadSizeWarning(ImmutableSet.of(VACATION_HOME), 12)
                             .build();
        WarningsSnapshot expected = builder()
                                    .tombstonesAbort(ImmutableSet.of(HOME), 42)
                                    .localReadSizeWarning(ImmutableSet.of(VACATION_HOME), 12)
                                    .build();

        // validate builder to protect against empty = empty passing this test
        assertThat(a.tombstones.aborts.instances).isEqualTo(expected.tombstones.aborts.instances).isEqualTo(ImmutableSet.of(HOME));
        assertThat(a.tombstones.aborts.maxValue).isEqualTo(expected.tombstones.aborts.maxValue).isEqualTo(42);
        assertThat(b.localReadSize.warnings.instances).isEqualTo(expected.localReadSize.warnings.instances).isEqualTo(ImmutableSet.of(VACATION_HOME));
        assertThat(b.localReadSize.warnings.maxValue).isEqualTo(expected.localReadSize.warnings.maxValue).isEqualTo(12);

        WarningsSnapshot output = a.merge(b);
        assertThat(output).isEqualTo(expected).isEqualTo(expected.merge(empty()));
        assertThat(output.merge(expected)).isEqualTo(expected);
    }

    @Test
    public void mergeConflict()
    {
        WarningsSnapshot a          = builder().tombstonesAbort(ImmutableSet.of(HOME), 42).build();
        WarningsSnapshot b          = builder().tombstonesAbort(ImmutableSet.of(VACATION_HOME), 12).build();
        WarningsSnapshot expected   = builder().tombstonesAbort(ImmutableSet.of(HOME, VACATION_HOME), 42).build();

        // validate builder to protect against empty = empty passing this test
        assertThat(a.tombstones.aborts.instances).isEqualTo(ImmutableSet.of(HOME));
        assertThat(a.tombstones.aborts.maxValue).isEqualTo(42);
        assertThat(b.tombstones.aborts.instances).isEqualTo(ImmutableSet.of(VACATION_HOME));
        assertThat(b.tombstones.aborts.maxValue).isEqualTo(12);
        assertThat(expected.tombstones.aborts.instances).isEqualTo(ImmutableSet.of(HOME, VACATION_HOME));
        assertThat(expected.tombstones.aborts.maxValue).isEqualTo(42);

        WarningsSnapshot output = a.merge(b);
        assertThat(output).isEqualTo(expected).isEqualTo(expected.merge(empty()));
        assertThat(output.merge(expected)).isEqualTo(expected);
    }

    private static InetAddressAndPort address(int a, int b, int c, int d)
    {
        try
        {
            InetAddress address = InetAddress.getByAddress(new byte[]{ (byte) a, (byte) b, (byte) c, (byte) d });
            return InetAddressAndPort.getByAddress(address);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static Gen<WarningsSnapshot> all()
    {
        Gen<Boolean> empty = SourceDSL.booleans().all();
        Gen<WarningsSnapshot> nonEmpty = nonEmpty();
        Gen<WarningsSnapshot> gen = rs ->
            empty.generate(rs) ? empty() : nonEmpty.generate(rs);
        return gen.describedAs(WarningsSnapshot::toString);
    }

    private static Gen<WarningsSnapshot> nonEmpty()
    {
        Gen<Counter> counter = counter();
        Gen<WarningsSnapshot> gen = rs -> {
            Builder builder = builder();
            builder.tombstonesWarning(counter.generate(rs));
            builder.tombstonesAbort(counter.generate(rs));
            builder.localReadSizeWarning(counter.generate(rs));
            builder.localReadSizeAbort(counter.generate(rs));
            builder.rowIndexSizeWarning(counter.generate(rs));
            builder.rowIndexSizeAbort(counter.generate(rs));
            return builder.build();
        };
        return gen.assuming(WarningsSnapshot::isDefined).describedAs(WarningsSnapshot::toString);
    }

    private static Gen<Counter> counter()
    {
        Gen<Boolean> empty = SourceDSL.booleans().all();
        Constraint maxValue = Constraint.between(1, Long.MAX_VALUE);
        Gen<ImmutableSet<InetAddressAndPort>> instances = SourceDSL.arbitrary()
                                                                   .pick(ImmutableSet.of(HOME), ImmutableSet.of(VACATION_HOME), ImmutableSet.of(HOME, VACATION_HOME));
        Gen<Counter> gen = rs ->
                           empty.generate(rs) ? Counter.empty()
                                              : new Counter(instances.generate(rs), rs.next(maxValue));
        return gen.describedAs(Counter::toString);
    }
}