/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A migration represents a single metadata mutation (cf dropped, added, etc.).
 *
 * There are two parts to a migration (think of it as a schema update):
 * 1. data is written to the schema cf (SCHEMA_KEYSPACES_CF).
 * 2. updated models are applied to the cassandra instance.
 * 
 * Since all steps are not committed atomically, care should be taken to ensure that a node/cluster is reasonably
 * quiescent with regard to the Keyspace or ColumnFamily whose schema is being modified.
 */
public abstract class Migration
{
    protected static final Logger logger = LoggerFactory.getLogger(Migration.class);
    
    public static final String NAME_VALIDATOR_REGEX = "\\w+";
    public static final String MIGRATIONS_CF = "Migrations";
    public static final String SCHEMA_CF = "Schema";
    public static final ByteBuffer LAST_MIGRATION_KEY = ByteBufferUtil.bytes("Last Migration");

    protected final long timestamp;

    Migration(long modificationTimestamp)
    {
        timestamp = modificationTimestamp;
    }

    public final void apply() throws ConfigurationException, IOException
    {
        applyImpl();

        if (!StorageService.instance.isClientMode())
            MigrationHelper.flushSchemaCFs();

        Schema.instance.updateVersion();
    }

    /**
     * Class specific apply implementation where schema migration logic should be put
     *
     * @throws IOException on any I/O related error.
     * @throws ConfigurationException if there is object misconfiguration.
     */
    protected abstract void applyImpl() throws ConfigurationException, IOException;

    /** Send schema update (in form of row mutations) to alive nodes in the cluster. apply() must be called first. */
    public final void announce()
    {
        assert !StorageService.instance.isClientMode();
        MigrationManager.announce(SystemTable.serializeSchema());
        passiveAnnounce(); // keeps gossip in sync w/ what we just told everyone
    }

    /** Announce new schema version over Gossip */
    public final void passiveAnnounce()
    {
        MigrationManager.passiveAnnounce(Schema.instance.getVersion());
    }

    /**
     * Used only in case node has old style migration schema (newly updated)
     * @return the UUID identifying version of the last applied migration
     */
    @Deprecated
    public static UUID getLastMigrationId()
    {
        DecoratedKey<?> dkey = StorageService.getPartitioner().decorateKey(LAST_MIGRATION_KEY);
        Table defs = Table.open(Table.SYSTEM_TABLE);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(SCHEMA_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(dkey, new QueryPath(SCHEMA_CF), LAST_MIGRATION_KEY);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        if (cf == null || cf.getColumnNames().size() == 0)
            return null;
        else
            return UUIDGen.getUUID(cf.getColumn(LAST_MIGRATION_KEY).value());
    }
    
    public static boolean isLegalName(String s)
    {
        return s.matches(Migration.NAME_VALIDATOR_REGEX);
    }
}
