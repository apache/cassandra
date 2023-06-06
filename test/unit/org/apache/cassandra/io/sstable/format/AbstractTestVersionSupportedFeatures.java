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

package org.apache.cassandra.io.sstable.format;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;

public abstract class AbstractTestVersionSupportedFeatures
{
    protected static final List<String> ALL_VERSIONS = IntStream.rangeClosed('a', 'z')
                                                                .mapToObj(i -> String.valueOf((char) i))
                                                                .flatMap(first -> IntStream.rangeClosed('a', 'z').mapToObj(second -> first + (char) second))
                                                                .collect(Collectors.toList());

    protected abstract Version getVersion(String v);

    protected abstract Stream<String> getPendingRepairSupportedVersions();

    protected abstract Stream<String> getPartitionLevelDeletionPresenceMarkerSupportedVersions();

    protected abstract Stream<String> getLegacyMinMaxSupportedVersions();

    protected abstract Stream<String> getImprovedMinMaxSupportedVersions();

    protected abstract Stream<String> getKeyRangeSupportedVersions();

    protected abstract Stream<String> getOriginatingHostIdSupportedVersions();

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCompatibility()
    {
        checkPredicateAgainstVersions(Version::hasPendingRepair, getPendingRepairSupportedVersions());
        checkPredicateAgainstVersions(Version::hasImprovedMinMax, getImprovedMinMaxSupportedVersions());
        checkPredicateAgainstVersions(Version::hasLegacyMinMax, getLegacyMinMaxSupportedVersions());
        checkPredicateAgainstVersions(Version::hasPartitionLevelDeletionsPresenceMarker, getPartitionLevelDeletionPresenceMarkerSupportedVersions());
        checkPredicateAgainstVersions(Version::hasKeyRange, getKeyRangeSupportedVersions());
        checkPredicateAgainstVersions(Version::hasOriginatingHostId, getOriginatingHostIdSupportedVersions());
    }

    public static Stream<String> range(String fromIncl, String toIncl)
    {
        int fromIdx = ALL_VERSIONS.indexOf(fromIncl);
        int toIdx = ALL_VERSIONS.indexOf(toIncl);
        assert fromIdx >= 0 && toIdx >= 0;
        return ALL_VERSIONS.subList(fromIdx, toIdx + 1).stream();
    }

    /**
     * Check the version predicate against the provided versions.
     *
     * @param predicate     predicate to check against version
     * @param versionBounds a stream of versions for which the predicate should return true
     */
    private void checkPredicateAgainstVersions(Predicate<Version> predicate, Stream<String> versionBounds)
    {
        List<String> expected = versionBounds.collect(Collectors.toList());
        List<String> actual = ALL_VERSIONS.stream().filter(v -> predicate.test(getVersion(v))).collect(Collectors.toList());
        Assertions.assertThat(actual).isEqualTo(expected);
    }
}
