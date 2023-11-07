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

package org.apache.cassandra.io.sstable.format.big;

import java.util.stream.Stream;

import org.apache.cassandra.io.sstable.format.AbstractTestVersionSupportedFeatures;
import org.apache.cassandra.io.sstable.format.Version;

public class VersionSupportedFeaturesTest extends AbstractTestVersionSupportedFeatures
{
    @Override
    protected Version getVersion(String v)
    {
        return BigFormat.getInstance().getVersion(v);
    }

    @Override
    protected Stream<String> getPendingRepairSupportedVersions()
    {
        return range("na", "zz");
    }

    @Override
    protected Stream<String> getPartitionLevelDeletionPresenceMarkerSupportedVersions()
    {
        return range("oa", "zz");
    }

    @Override
    protected Stream<String> getLegacyMinMaxSupportedVersions()
    {
        return range("ma", "nz");
    }

    @Override
    protected Stream<String> getImprovedMinMaxSupportedVersions()
    {
        return range("oa", "zz");
    }

    @Override
    protected Stream<String> getKeyRangeSupportedVersions()
    {
        return range("oa", "zz");
    }

    @Override
    protected Stream<String> getOriginatingHostIdSupportedVersions()
    {
        return Stream.concat(range("me", "mz"), range("nb", "zz"));
    }
}
