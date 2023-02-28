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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;

/**
 * Immutable snapshot of the current schema along with its version.
 */
public class DistributedSchema implements MetadataValue<DistributedSchema>
{
    public static final Serializer serializer = new Serializer();

    public static final DistributedSchema empty()
    {
        return new DistributedSchema(Keyspaces.none(), Epoch.EMPTY);
    }

    public static DistributedSchema first()
    {
        return new DistributedSchema(Keyspaces.of(DistributedMetadataLogKeyspace.metadata(),
                                                  SystemDistributedKeyspace.metadata()), Epoch.FIRST);
    }

    private final Keyspaces keyspaces;
    private final Epoch epoch;
    private final UUID version;
    private final Map<String, Keyspace> keyspaceInstances = new HashMap<>();

    public DistributedSchema(Keyspaces keyspaces)
    {
        this(keyspaces, Epoch.EMPTY);
    }

    public DistributedSchema(Keyspaces keyspaces, Epoch epoch)
    {
        Objects.requireNonNull(keyspaces);
        this.keyspaces = keyspaces;
        this.epoch = epoch;
        this.version = new UUID(0, epoch.getEpoch());
        validate();
    }

    @Override
    public DistributedSchema withLastModified(Epoch epoch)
    {
        return new DistributedSchema(keyspaces, epoch);
    }

    @Override
    public Epoch lastModified()
    {
        return epoch;
    }

    public Keyspace getKeyspace(String keyspace)
    {
        return keyspaceInstances.get(keyspace);
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspace)
    {
        return keyspaces.get(keyspace).get();
    }

    public static DistributedSchema fromSystemTables(Keyspaces keyspaces)
    {
        if (!keyspaces.containsKeyspace(SchemaConstants.METADATA_KEYSPACE_NAME))
            keyspaces = keyspaces.with(DistributedMetadataLogKeyspace.metadata());
        return new DistributedSchema(keyspaces, Epoch.UPGRADE_GOSSIP);
    }

    public void initializeKeyspaceInstances(DistributedSchema prev)
    {
        initializeKeyspaceInstances(prev, true);
    }

    public void initializeKeyspaceInstances(DistributedSchema prev, boolean loadSSTables)
    {
        keyspaceInstances.putAll(prev.keyspaceInstances);

        Keyspaces.KeyspacesDiff ksDiff = Keyspaces.diff(prev.getKeyspaces(), getKeyspaces());

        SchemaChangeNotifier schemaChangeNotifier = Schema.instance.schemaChangeNotifier();
        schemaChangeNotifier.notifyPreChanges(new SchemaTransformation.SchemaTransformationResult(prev, this, ksDiff));

        ksDiff.dropped.forEach(metadata -> {
            schemaChangeNotifier.notifyKeyspaceDropped(metadata, loadSSTables);
            dropKeyspace(metadata, true);
        });

        ksDiff.created.forEach(metadata -> {
            schemaChangeNotifier.notifyKeyspaceCreated(metadata);
            keyspaceInstances.put(metadata.name, new Keyspace(Schema.instance, metadata, loadSSTables));
        });

        ksDiff.altered.forEach(delta -> {
            boolean initialized = Keyspace.isInitialized();

            Keyspace keyspace = initialized ? keyspaceInstances.get(delta.before.name) : null;
            if (initialized)
            {
                assert keyspace != null : String.format("Keyspace %s is not initialized. Initialized keyspaces: %s.", delta.before.name, keyspaceInstances.keySet());
                assert delta.before.name.equals(delta.after.name);

                // drop tables and views
                delta.views.dropped.forEach(v -> dropView(keyspace, v, true));
                delta.tables.dropped.forEach(t -> dropTable(keyspace, t, true));

                // add tables and views
                delta.tables.created.forEach(t -> createTable(keyspace, t));
                delta.views.created.forEach(v -> createView(keyspace, v));

                // update tables and views
                delta.tables.altered.forEach(diff -> alterTable(keyspace, diff.after));
                delta.views.altered.forEach(diff -> alterView(keyspace, diff.after));

                schemaChangeNotifier.notifyKeyspaceAltered(delta, loadSSTables);
                // deal with all added, and altered views
                keyspaceInstances.get(delta.after.name).viewManager.reload(true);
            }

            //schemaChangeNotifier.notifyKeyspaceAltered(delta);
            SchemaDiagnostics.keyspaceAltered(Schema.instance, delta);
        });

        // Avoid system table side effects during initialization
        if (epoch.isEqualOrAfter(Epoch.FIRST))
        {
            Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(ksDiff, FBUtilities.timestampMicros());
            SchemaKeyspace.applyChanges(mutations);
        }

        QueryProcessor.clearInternalStatementsCache();
        QueryProcessor.clearPreparedStatementsCache();
    }

    private void dropView(Keyspace keyspace, ViewMetadata metadata, boolean dropData)
    {
        keyspace.viewManager.dropView(metadata.name());
        dropTable(keyspace, metadata.metadata, dropData);
    }

    // TODO: handle drops after snapshots
    private void dropKeyspace(KeyspaceMetadata keyspaceMetadata, boolean dropData)
    {
        SchemaDiagnostics.keyspaceDropping(Schema.instance, keyspaceMetadata);

        boolean initialized = Keyspace.isInitialized();
        Keyspace keyspace = initialized ? Keyspace.open(keyspaceMetadata.name) : null;
        if (initialized)
        {
            if (keyspace == null)
                return;

            keyspaceMetadata.views.forEach(v -> dropView(keyspace, v, dropData));
            keyspaceMetadata.tables.forEach(t -> dropTable(keyspace, t, dropData));

            // remove the keyspace from the static instances
            Keyspace unloadedKeyspace = keyspaceInstances.remove(keyspaceMetadata.name);
            unloadedKeyspace.unload(true);
            SchemaDiagnostics.metadataRemoved(Schema.instance, keyspaceMetadata);
            assert unloadedKeyspace == keyspace;

            Keyspace.writeOrder.awaitNewBarrier();
        }
        else
        {
            keyspace.unload(true);
            SchemaDiagnostics.metadataRemoved(Schema.instance, keyspaceMetadata);
        }

        SchemaDiagnostics.keyspaceDropped(Schema.instance, keyspaceMetadata);
    }
    /**
     *
     * @param keyspace
     * @param metadata
     */
    private void dropTable(Keyspace keyspace, TableMetadata metadata, boolean dropData)
    {
        SchemaDiagnostics.tableDropping(Schema.instance, metadata);
        keyspace.dropCf(metadata.id, dropData);
        SchemaDiagnostics.tableDropped(Schema.instance, metadata);
    }

    private void createTable(Keyspace keyspace, TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(Schema.instance, table);
        keyspace.initCf(table, true);
        SchemaDiagnostics.tableCreated(Schema.instance, table);
    }

    private void createView(Keyspace keyspace, ViewMetadata view)
    {
        SchemaDiagnostics.tableCreating(Schema.instance, view.metadata);
        keyspace.initCf(view.metadata, true);
        SchemaDiagnostics.tableCreated(Schema.instance, view.metadata);
    }

    private void alterTable(Keyspace keyspace, TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(Schema.instance, updated);
        keyspace.getColumnFamilyStore(updated.name).reload(updated);
        SchemaDiagnostics.tableAltered(Schema.instance, updated);
    }

    private void alterView(Keyspace keyspace, ViewMetadata updated)
    {
        SchemaDiagnostics.tableAltering(Schema.instance, updated.metadata);
        keyspace.getColumnFamilyStore(updated.name()).reload(updated.metadata);
        SchemaDiagnostics.tableAltered(Schema.instance, updated.metadata);
    }

    public Keyspaces getKeyspaces()
    {
        return keyspaces;
    }

    public boolean isEmpty()
    {
        return epoch.is(Epoch.EMPTY);
    }

    public UUID getVersion()
    {
        return version;
    }

    /**
     * Converts the given schema version to a string. Returns {@code unknown}, if {@code version} is {@code null}
     * or {@code "(empty)"}, if {@code version} refers to an {@link SchemaConstants#emptyVersion empty) schema.
     */
    public static String schemaVersionToString(UUID version)
    {
        return version == null
               ? "unknown"
               : SchemaConstants.emptyVersion.equals(version)
                 ? "(empty)"
                 : version.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedSchema schema = (DistributedSchema) o;
        return keyspaces.equals(schema.keyspaces) && version.equals(schema.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaces, version);
    }

    private void validate()
    {
        keyspaces.forEach(ksm -> {
            ksm.tables.forEach(tm -> Preconditions.checkArgument(tm.keyspace.equals(ksm.name), "Table %s metadata points to keyspace %s while defined in keyspace %s", tm.name, tm.keyspace, ksm.name));
            ksm.views.forEach(vm -> Preconditions.checkArgument(vm.keyspace().equals(ksm.name), "View %s metadata points to keyspace %s while defined in keyspace %s", vm.name(), vm.keyspace(), ksm.name));
            ksm.types.forEach(ut -> Preconditions.checkArgument(ut.keyspace.equals(ksm.name), "Type %s points to keyspace %s while defined in keyspace %s", ut.name, ut.keyspace, ksm.name));
            ksm.userFunctions.forEach(f -> Preconditions.checkArgument(f.name().keyspace.equals(ksm.name), "Function %s points to keyspace %s while defined in keyspace %s", f.name().name, f.name().keyspace, ksm.name));
        });
    }

    public static class Serializer implements MetadataSerializer<DistributedSchema>
    {
        public void serialize(DistributedSchema t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.epoch, out, version);
            out.writeInt(t.keyspaces.size());
            for (KeyspaceMetadata ksm : t.keyspaces)
                KeyspaceMetadata.serializer.serialize(ksm, out, version);
        }

        public DistributedSchema deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch basedOnEpoch = Epoch.serializer.deserialize(in, version);
            int ksCount = in.readInt();
            List<KeyspaceMetadata> ksms = new ArrayList<>(ksCount);
            for (int i = 0; i < ksCount; i++)
                ksms.add(KeyspaceMetadata.serializer.deserialize(in, version));

            return new DistributedSchema(Keyspaces.of(ksms.toArray(new KeyspaceMetadata[ksCount])), basedOnEpoch);
        }

        public long serializedSize(DistributedSchema t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.epoch, version);
            size += sizeof(t.keyspaces.size());
            for (KeyspaceMetadata ksm : t.keyspaces)
                size += KeyspaceMetadata.serializer.serializedSize(ksm, version);
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "DistributedSchema{" +
               "keyspaces=" + keyspaces +
               ", epoch=" + epoch +
               ", version=" + version +
               ", keyspaceInstances=" + keyspaceInstances +
               '}';
    }
}
