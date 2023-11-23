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

package org.apache.cassandra.schema;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.triggers.TriggersTest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaChangeDuringRangeMovementTest extends CQLTester
{
    // at the moment, the detail of the specific LockedRanges doesn't matter, transformations
    // which are rejected in the presence of locking are rejected whatever is actually locked
    private static final LockedRanges.AffectedRanges toLock =
        LockedRanges.AffectedRanges.singleton(ReplicationParams.simple(3),
                                              new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                          DatabaseDescriptor.getPartitioner().getRandomToken()));

    @Test
    public void testAlwaysPermittedChanges() throws Throwable
    {
        // Category of schema transformations should always be allowed if syntactically
        // and semantically valid. The presence of inflight range movements is not relevant.

        // create/drop function
        // create/drop aggregate
        withAndWithoutLockedRanges(() -> {
            String f = createFunction(KEYSPACE,
                                      "double, double",
                                      "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                      "RETURNS NULL ON NULL INPUT " +
                                      "RETURNS double " +
                                      "LANGUAGE java " +
                                      "AS 'return 0.0;';");

            String a = createAggregate(KEYSPACE,
                                       "double",
                                       "CREATE OR REPLACE AGGREGATE %s(double) " +
                                       "SFUNC " + shortFunctionName(f) + " " +
                                       "STYPE double " +
                                       "INITCOND 0");

            execute("DROP AGGREGATE " + a);
            execute("DROP FUNCTION " + f);
        });


        // create/alter/drop table
        // create/drop index
        withAndWithoutLockedRanges(() -> {
            createTable(KEYSPACE, "CREATE TABLE %s (id int primary key, v1 text, v2 text)");
            alterTable("ALTER TABLE %s ADD v3 int;");
            String i = createIndex(KEYSPACE, "CREATE INDEX ON %s(v1)");
            dropIndex("DROP INDEX %s." + i);
            dropTable("DROP TABLE %s");

        });

        // create/drop trigger
        withAndWithoutLockedRanges(() -> {
            String t = createTable(KEYSPACE, "CREATE TABLE %s (id int primary key, v1 text, v2 text)");
            execute(String.format("CREATE TRIGGER tr1 ON %s.%s USING '%s'",
                                  KEYSPACE, t, TriggersTest.TestTrigger.class.getName()));
            execute("DROP TRIGGER tr1 ON %s");
            dropTable("DROP TABLE %s");
        });

        // create/alter/drop type
        withAndWithoutLockedRanges(() -> {
            String t = createType(KEYSPACE, "CREATE TYPE %s (a int)");
            execute(String.format("ALTER TYPE %s.%s ADD b int", KEYSPACE, t));
            execute(String.format("DROP TYPE %s.%s", KEYSPACE, t));
        });

        // create/alter/drop view
        withAndWithoutLockedRanges(() -> {
            String t = createTable("CREATE TABLE %s (" +
                                   "a int," +
                                   "b int," +
                                   "PRIMARY KEY (a, b))");
            // don't use CQLTester::createView here as it waits for the view to be built,
            // but this won't happen as StorageService isn't initialised
            execute(String.format("CREATE MATERIALIZED VIEW %s.v " +
                                  "AS " +
                                  "SELECT * FROM %s.%s " +
                                  "WHERE a IS NOT NULL AND b IS NOT NULL " +
                                  "PRIMARY KEY (b, a)", KEYSPACE, KEYSPACE, t));
            execute(String.format("DROP MATERIALIZED VIEW %s.v", KEYSPACE));
            execute(String.format("DROP TABLE %s.%s", KEYSPACE, t));
        });
    }

    @Test
    public void testRestrictedChanges() throws Throwable
    {
        final String RF9_KS1 = "rf9_ks1";
        final String RF9_KS2 = "rf9_ks2";
        final String RF9_KS3 = "rf9_ks3";
        final String RF9_KS4 = "rf9_ks4";
        final String RF10_KS1 = "rf10_ks1";

        // keyspace creation should be rejected if the same replication
        // params are not already in use by other keyspaces
        execute(String.format("CREATE KEYSPACE %s " +
                              "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':9}", RF9_KS1));
        // now lock ranges
        ClusterMetadata metadata = ClusterMetadataService.instance().commit(new LockRanges());
        assertFalse(metadata.lockedRanges.locked.isEmpty());

        // creating a ks with an existing set of replication params is permitted
        execute(String.format("CREATE KEYSPACE %s WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':9}", RF9_KS2));
        // but one with distinct replication settings is rejected
        expectRejection("CREATE KEYSPACE %s WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':10}", RF10_KS1);

        // altering a keyspace is allowed, as long as the replication settings aren't modified
        execute(String.format("ALTER KEYSPACE %s WITH DURABLE_WRITES = FALSE", RF9_KS1));
        expectRejection("ALTER KEYSPACE %s WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}", RF9_KS1);

        // dropping a keyspace is permitted as long as it isn't the only one with its replication settings
        execute(String.format("DROP KEYSPACE %s", RF9_KS2));
        expectRejection("DROP KEYSPACE %s", RF9_KS1);

        // dropping multiple keyspaces in one transformation
        execute(String.format("CREATE KEYSPACE %s " +
                              "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':9}", RF9_KS2));
        execute(String.format("CREATE KEYSPACE %s " +
                              "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':9}", RF9_KS3));
        execute(String.format("CREATE KEYSPACE %s " +
                              "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':9}", RF9_KS4));

        SchemaTransformation dropAllowed = (metadata_) -> metadata_.schema.getKeyspaces().without(RF9_KS4).without(RF9_KS3);
        metadata = ClusterMetadataService.instance().commit(new AlterSchema(dropAllowed, Schema.instance));
        assertFalse(metadata.schema.getKeyspaces().containsKeyspace(RF9_KS4));
        assertFalse(metadata.schema.getKeyspaces().containsKeyspace(RF9_KS3));

        try
        {
            SchemaTransformation dropRejected = (metadata_) -> metadata_.schema.getKeyspaces().without(RF9_KS2).without(RF9_KS1);
            ClusterMetadataService.instance().commit(new AlterSchema(dropRejected, Schema.instance));
            fail("Expected exception");
        }
        catch (IllegalStateException e)
        {
            // IllegalStateException because we're going directly to CMS here with a programmatically constructed
            // SchemaTransformation, in most circumstances this would be done via CQL and InvalidRequestException thrown
            assertTrue(e.getMessage().contains("The requested schema changes cannot be executed as they conflict with " +
                                               "ongoing range movements."));
            assertTrue(e.getMessage().contains(RF9_KS1));
            assertTrue(e.getMessage().contains(RF9_KS2));
        }

        metadata = ClusterMetadata.current();
        assertTrue(metadata.schema.getKeyspaces().containsKeyspace(RF9_KS2));
        assertTrue(metadata.schema.getKeyspaces().containsKeyspace(RF9_KS1));
    }

    private void expectRejection(String query, String keyspace) throws Throwable
    {
        try
        {
            execute(String.format(query, keyspace));
            fail("Expected exception");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("The requested schema changes cannot be executed as they conflict with " +
                                               "ongoing range movements. The changes for keyspaces [" + keyspace +
                                               "] are blocked"));
        }
    }

    private interface TestActions
    {
        void perform() throws Throwable;
    }

    private void withAndWithoutLockedRanges(TestActions actions) throws Throwable
    {
        // first verify without any locked ranges
        ClusterMetadata metadata = ClusterMetadata.current();
        assertTrue(metadata.lockedRanges.locked.isEmpty());
        actions.perform();

        metadata = ClusterMetadataService.instance().commit(new LockRanges());
        assertFalse(metadata.lockedRanges.locked.isEmpty());
        actions.perform();

        metadata = ClusterMetadataService.instance().commit(new ClearLockedRanges());
        assertTrue(metadata.lockedRanges.locked.isEmpty());
    }


    // Custom transforms to lock/unlock an arbitrary set of ranges to
    // avoid having to actually initiate some range movement
    private static class LockRanges implements Transformation
    {
        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            LockedRanges newLocked = metadata.lockedRanges.lock(LockedRanges.keyFor(metadata.epoch), toLock);
            return Transformation.success(metadata.transformer().with(newLocked), toLock);
        }
    }

    private static class ClearLockedRanges implements Transformation
    {
        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            LockedRanges newLocked = LockedRanges.EMPTY;
            return Transformation.success(metadata.transformer().with(newLocked), LockedRanges.AffectedRanges.EMPTY);
        }
    }
}
