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

package org.apache.cassandra.utils.asserts;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.repair.LocalSyncTask;
import org.apache.cassandra.repair.SyncTask;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;

public class SyncTaskAssert extends AbstractObjectAssert<SyncTaskAssert, SyncTask> implements SizeableObjectAssert<SyncTaskAssert>
{
    private SyncTaskAssert(SyncTask syncTask)
    {
        super(syncTask, SyncTaskAssert.class);
    }

    public static SyncTaskAssert assertThat(SyncTask task)
    {
        return new SyncTaskAssert(task);
    }

    @Override
    public Object actual()
    {
        return actual;
    }

    public SyncTaskAssert hasLocal(boolean expected)
    {
        Assertions.assertThat(actual.isLocal()).isEqualTo(expected);
        return this;
    }

    public SyncTaskAssert isLocal()
    {
        Assertions.assertThat(actual.isLocal()).isTrue();
        return this;
    }

    public SyncTaskAssert isNotLocal()
    {
        Assertions.assertThat(actual.isLocal()).isFalse();
        return this;
    }

    public SyncTaskAssert isRequestRanges()
    {
        Preconditions.checkState(actual instanceof LocalSyncTask, "Tested value is not a LocalSyncTask");
        Assertions.assertThat(((LocalSyncTask) actual).requestRanges).isTrue();
        return this;
    }

    public SyncTaskAssert isNotRequestRanges()
    {
        Preconditions.checkState(actual instanceof LocalSyncTask, "Tested value is not a LocalSyncTask");
        Assertions.assertThat(((LocalSyncTask) actual).requestRanges).isFalse();
        return this;
    }

    public SyncTaskAssert hasTransferRanges(boolean expected)
    {
        Preconditions.checkState(actual instanceof LocalSyncTask, "Tested value is not a LocalSyncTask");
        Assertions.assertThat(((LocalSyncTask) actual).transferRanges).isEqualTo(expected);
        return this;
    }

    @SuppressWarnings("unchecked")
    public SyncTaskAssert hasRanges(Range... ranges)
    {
        Assertions.assertThat(actual.rangesToSync).containsOnly(ranges);
        return this;
    }
}
