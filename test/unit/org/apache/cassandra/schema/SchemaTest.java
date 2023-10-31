/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SchemaTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        Schema.instance.loadFromDisk();
    }

    @Test
    public void testTransKsMigration() throws IOException
    {
        assertEquals(0, Schema.instance.distributedKeyspaces().size());

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
        try
        {
            // add a few.
            saveKeyspaces();
            Schema.instance.reloadSchemaAndAnnounceVersion();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.transform(keyspaces -> keyspaces.without(Arrays.asList("ks0", "ks1")));

            assertNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNull(Schema.instance.getKeyspaceMetadata("ks1"));

            saveKeyspaces();
            Schema.instance.reloadSchemaAndAnnounceVersion();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    public void testKeyspaceCreationWhenNotInitialized() {
        Keyspace.unsetInitialized();
        try
        {
            SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create("test", KeyspaceParams.simple(1)), true);
            assertNotNull(Schema.instance.getKeyspaceMetadata("test"));
            assertNull(Schema.instance.getKeyspaceInstance("test"));

            SchemaTestUtil.dropKeyspaceIfExist("test", true);
            assertNull(Schema.instance.getKeyspaceMetadata("test"));
            assertNull(Schema.instance.getKeyspaceInstance("test"));
        }
        finally
        {
            Keyspace.setInitialized();
        }

        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create("test", KeyspaceParams.simple(1)), true);
        assertNotNull(Schema.instance.getKeyspaceMetadata("test"));
        assertNotNull(Schema.instance.getKeyspaceInstance("test"));

        SchemaTestUtil.dropKeyspaceIfExist("test", true);
        assertNull(Schema.instance.getKeyspaceMetadata("test"));
        assertNull(Schema.instance.getKeyspaceInstance("test"));
    }

    /**
     * This test checks that the schema version is updated only after the entire schema change is processed.
     * In particular, we expect that {@link Schema#getVersion()} returns stale schema version as it was before
     * the change, but it does not block. On the other hand, {@link Schema#getDistributedSchemaBlocking()} should block
     * until the schema change is processed and return the new schema version.
     */
    @Test
    public void testGettingSchemaVersion()
    {
        Duration timeout = Duration.ofMillis(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS));

        // the listener with a barrier will let us control the schema change process
        CyclicBarrier barrier = new CyclicBarrier(2);
        SchemaChangeListener listener = new SchemaChangeListener()
        {
            @Override
            public void onCreateKeyspace(KeyspaceMetadata keyspace)
            {
                try
                {
                    barrier.await();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                catch (BrokenBarrierException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };

        String suffix = String.valueOf(System.currentTimeMillis());
        try
        {
            Schema.instance.registerListener(listener);

            // initial schema version - we will expect it not to change until the entire schema change is processed
            UUID v0 = Schema.instance.getDistributedSchemaBlocking().getVersion();

            // a multi-step schema transformation
            SchemaTransformation transformation = schema -> {
                schema = schema.withAddedOrReplaced(KeyspaceMetadata.create("test1" + suffix, KeyspaceParams.simple(1)));
                schema = schema.withAddedOrReplaced(KeyspaceMetadata.create("test2" + suffix, KeyspaceParams.simple(1)));
                schema = schema.withAddedOrReplaced(KeyspaceMetadata.create("test3" + suffix, KeyspaceParams.simple(1)));
                return schema;
            };

            // schema change is executed async because it is expected to wait for the barrier
            ForkJoinTask<SchemaTransformation.SchemaTransformationResult> resultFuture = ForkJoinPool.commonPool().submit(() -> Schema.instance.transform(transformation, true));

            // let the first statement execute
            barrier.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Awaitility.await().atMost(timeout).until(() -> Schema.instance.distributedKeyspaces().containsKeyspace("test1" + suffix));
            assertThat(Schema.instance.getVersion()).isEqualTo(v0); // we should be able to get version unsafely, and it should return stale schema version
            assertThat(resultFuture.isDone()).isFalse(); // the schema change should not be done yet

            // let's query the schema version safely - in particular, this task should not finish until the schema change is done
            ForkJoinTask<UUID> futureSchemaVersion = ForkJoinPool.commonPool().submit(() -> Schema.instance.getDistributedSchemaBlocking().getVersion());

            // let the second statement execute
            barrier.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Awaitility.await().atMost(timeout).until(() -> Schema.instance.distributedKeyspaces().containsKeyspace("test2" + suffix));
            assertThat(Schema.instance.getVersion()).isEqualTo(v0); // we should be able to get version unsafely, and it should return stale schema version
            assertThat(resultFuture.isDone()).isFalse(); // the schema change should not be done yet
            assertThat(futureSchemaVersion.isDone()).isFalse(); // the schema version should not be available yet

            // let the third statement execute
            barrier.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Awaitility.await().atMost(timeout).until(() -> Schema.instance.distributedKeyspaces().containsKeyspace("test3" + suffix));

            SchemaTransformation.SchemaTransformationResult result = resultFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS); // the schema change should be done shortly
            assertThat(futureSchemaVersion.isDone()).isTrue(); // the schema version should be available now
            UUID v1 = futureSchemaVersion.get();
            assertThat(v1).isNotEqualTo(v0); // the schema version should be updated
            assertThat(result).isNotNull();
            assertThat(Schema.instance.getVersion()).isEqualTo(v1);
            assertThat(result.after.getVersion()).isEqualTo(v1);
            assertThat(result.before.getVersion()).isEqualTo(v0);
        }
        catch (BrokenBarrierException | TimeoutException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally
        {
            Schema.instance.unregisterListener(listener);
            barrier.reset();
            Schema.instance.transform(schema -> schema.without(Arrays.asList("test1" + suffix, "test2" + suffix, "test3" + suffix)));
        }
    }

    private void saveKeyspaces()
    {
        Collection<Mutation> mutations = Arrays.asList(SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build(),
                                                       SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build());
        SchemaKeyspace.applyChanges(mutations);
    }
}
