/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageAttachedIndexBuilderTest extends SAITester
{
    private static volatile ColumnFamilyStore columnFamilyStore;
    private static volatile boolean descriptorCacheEvicted;

    @Test
    public void testRegisterComponentsFromBuildDescriptorAfterCacheEviction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, value int)");
        execute("INSERT INTO %s (id, value) VALUES ('k1', 1)");
        flush();

        columnFamilyStore = getCurrentColumnFamilyStore();
        descriptorCacheEvicted = false;

        Injection evictDescriptorCache = Injections.newCustom("evict_descriptor_cache_after_sai_build")
                                                    .add(InvokePointBuilder.newInvokePoint()
                                                                           .onClass(StorageAttachedIndexBuilder.class)
                                                                           .onMethod("completeSSTable")
                                                                           .after("INVOKE " + StorageAttachedIndexWriter.class.getName() + ".complete"))
                                                    .add(ActionBuilder.newActionBuilder()
                                                                      .actions()
                                                                      .doAction(Expression.expr(StorageAttachedIndexBuilderTest.class.getName())
                                                                                          .method("evictDescriptorCache")
                                                                                          .args()))
                                                    .build();

        try
        {
            Injections.inject(evictDescriptorCache);

            createIndex(String.format(CREATE_INDEX_TEMPLATE, "value"));
            Awaitility.await("Byteman called to clear SSTableContextManager")
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(descriptorCacheEvicted).isTrue());

            StorageAttachedIndexGroup group = Objects.requireNonNull(StorageAttachedIndexGroup.getIndexGroup(columnFamilyStore));
            SSTableReader sstable = Iterables.getOnlyElement(columnFamilyStore.getLiveSSTables());

            // there is one descriptor populated by index build and it has all built index components
            assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(1);
            assertCompleteComponents(group, sstable);

            // now clear the cached descriptor and reload from TOC
            group.sstableContextManager().clear();
            assertThat(group.sstableContextManager().getDescriptorCount()).isEqualTo(0);

            assertCompleteComponents(group, sstable);
        }
        finally
        {
            columnFamilyStore = null;
        }
    }

    // called by byteman
    @SuppressWarnings("unused")
    public static void evictDescriptorCache()
    {
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(columnFamilyStore);
        if (group == null)
            throw new IllegalStateException("Storage-attached index group is not registered");

        group.sstableContextManager().clear();
        descriptorCacheEvicted = true;
    }

    private void assertCompleteComponents(StorageAttachedIndexGroup group, SSTableReader sstable)
    {
        IndexDescriptor descriptor = group.descriptorFor(sstable);
        assertThat(descriptor.perSSTableComponents().isComplete()).isTrue();
        for (StorageAttachedIndex index : group.getIndexes())
            assertThat(descriptor.perIndexComponents(index.getIndexContext()).isComplete()).isTrue();
    }
}
