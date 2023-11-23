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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.utils.Collectors3;

public final class SchemaEvent extends DiagnosticEvent
{
    private final SchemaEventType type;

    private final ImmutableCollection<String> keyspaces;
    private final ImmutableMap<String, String> indexTables;
    private final ImmutableCollection<String> tables;
    private final ImmutableCollection<String> nonSystemKeyspaces;
    private final ImmutableCollection<String> userKeyspaces;
    private final int numberOfTables;
    private final UUID version;

    @Nullable
    private final KeyspaceMetadata ksUpdate;
    @Nullable
    private final KeyspaceMetadata previous;
    @Nullable
    private final KeyspaceMetadata.KeyspaceDiff ksDiff;
    @Nullable
    private final TableMetadata tableUpdate;
    @Nullable
    private final Tables.TablesDiff tablesDiff;
    @Nullable
    private final Views.ViewsDiff viewsDiff;
    @Nullable
    private final MapDifference<String, TableMetadata> indexesDiff;

    public enum SchemaEventType
    {
        KS_METADATA_LOADED,
        KS_METADATA_RELOADED,
        KS_METADATA_REMOVED,
        VERSION_UPDATED,
        VERSION_ANOUNCED,
        KS_CREATING,
        KS_CREATED,
        KS_ALTERING,
        KS_ALTERED,
        KS_DROPPING,
        KS_DROPPED,
        TABLE_CREATING,
        TABLE_CREATED,
        TABLE_ALTERING,
        TABLE_ALTERED,
        TABLE_DROPPING,
        TABLE_DROPPED,
        SCHEMATA_LOADING,
        SCHEMATA_LOADED,
        SCHEMATA_CLEARED
    }

    SchemaEvent(SchemaEventType type, Schema schema, @Nullable KeyspaceMetadata ksUpdate,
                @Nullable KeyspaceMetadata previous, @Nullable KeyspaceMetadata.KeyspaceDiff ksDiff,
                @Nullable TableMetadata tableUpdate, @Nullable Tables.TablesDiff tablesDiff,
                @Nullable Views.ViewsDiff viewsDiff, @Nullable MapDifference<String, TableMetadata> indexesDiff)
    {
        this.type = type;
        this.ksUpdate = ksUpdate;
        this.previous = previous;
        this.ksDiff = ksDiff;
        this.tableUpdate = tableUpdate;
        this.tablesDiff = tablesDiff;
        this.viewsDiff = viewsDiff;
        this.indexesDiff = indexesDiff;

        this.keyspaces = schema.getKeyspaces().immutableCopy();
        this.nonSystemKeyspaces = schema.distributedKeyspaces().names();
        this.userKeyspaces = schema.getUserKeyspaces().immutableCopy();
        this.numberOfTables = schema.getNumberOfTables();
        this.version = schema.getVersion(); // TODO: rename this field to reflect that the schema version we know here is stale (before the entire transformation started)

        this.indexTables = schema.distributedKeyspaces().stream()
                                 .flatMap(ks -> ks.tables.indexTables().entrySet().stream())
                                 .collect(Collectors3.toImmutableMap(e -> String.format("%s,%s", e.getValue().keyspace, e.getKey()),
                                                                     e -> String.format("%s,%s,%s", e.getValue().id.toHexString(), e.getValue().keyspace, e.getValue().name)));

        this.tables = schema.distributedKeyspaces().stream()
                            .flatMap(ks -> StreamSupport.stream(ks.tablesAndViews().spliterator(), false))
                            .map(e -> String.format("%s,%s,%s", e.id.toHexString(), e.keyspace, e.name))
                            .collect(Collectors3.toImmutableList());
    }

    public SchemaEventType getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("keyspaces", this.keyspaces);
        ret.put("nonSystemKeyspaces", this.nonSystemKeyspaces);
        ret.put("userKeyspaces", this.userKeyspaces);
        ret.put("numberOfTables", this.numberOfTables);
        ret.put("version", this.version);
        ret.put("tables", this.tables);
        ret.put("indexTables", this.indexTables);
        if (ksUpdate != null) ret.put("ksMetadataUpdate", repr(ksUpdate));
        if (previous != null) ret.put("ksMetadataPrevious", repr(previous));
        if (ksDiff != null)
        {
            HashMap<String, Serializable> ks = new HashMap<>();
            ks.put("before", repr(ksDiff.before));
            ks.put("after", repr(ksDiff.after));
            ks.put("tables", repr(ksDiff.tables));
            ks.put("views", repr(ksDiff.views));
            ks.put("types", repr(ksDiff.types));
            ks.put("udas", repr(ksDiff.udas));
            ks.put("udfs", repr(ksDiff.udfs));
            ret.put("ksDiff", ks);
        }
        if (tableUpdate != null) ret.put("tableMetadataUpdate", repr(tableUpdate));
        if (tablesDiff != null) ret.put("tablesDiff", repr(tablesDiff));
        if (viewsDiff != null) ret.put("viewsDiff", repr(viewsDiff));
        if (indexesDiff != null) ret.put("indexesDiff", Lists.newArrayList(indexesDiff.entriesDiffering().keySet()));
        return ret;
    }

    private HashMap<String, Serializable> repr(Diff<?, ?> diff)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (diff.created != null) ret.put("created", diff.created.toString());
        if (diff.dropped != null) ret.put("dropped", diff.dropped.toString());
        if (diff.altered != null)
            ret.put("created", Lists.newArrayList(diff.altered.stream().map(Diff.Altered::toString).iterator()));
        return ret;
    }

    private HashMap<String, Serializable> repr(KeyspaceMetadata ksm)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("name", ksm.name);
        if (ksm.kind != null) ret.put("kind", ksm.kind.name());
        if (ksm.params != null) ret.put("params", ksm.params.toString());
        if (ksm.tables != null) ret.put("tables", ksm.tables.toString());
        if (ksm.views != null) ret.put("views", ksm.views.toString());
        if (ksm.userFunctions != null) ret.put("functions", ksm.userFunctions.toString());
        if (ksm.types != null) ret.put("types", ksm.types.toString());
        return ret;
    }

    private HashMap<String, Serializable> repr(TableMetadata table)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("id", table.id.toHexString());
        ret.put("name", table.name);
        ret.put("keyspace", table.keyspace);
        ret.put("partitioner", table.partitioner.toString());
        ret.put("kind", table.kind.name());
        ret.put("flags", Lists.newArrayList(table.flags.stream().map(Enum::name).iterator()));
        ret.put("params", repr(table.params));
        ret.put("indexes", Lists.newArrayList(table.indexes.stream().map(this::repr).iterator()));
        ret.put("triggers", Lists.newArrayList(repr(table.triggers)));
        ret.put("columns", Lists.newArrayList(table.columns.values().stream().map(this::repr).iterator()));
        ret.put("droppedColumns", Lists.newArrayList(table.droppedColumns.values().stream().map(this::repr).iterator()));
        ret.put("isCompactTable", table.isCompactTable());
        ret.put("isCompound", TableMetadata.Flag.isCompound(table.flags));
        ret.put("isCounter", table.isCounter());
        ret.put("isCQLTable", TableMetadata.Flag.isCQLTable(table.flags));
        ret.put("isDense", TableMetadata.Flag.isDense(table.flags));
        ret.put("isIndex", table.isIndex());
        ret.put("isStaticCompactTable", TableMetadata.Flag.isStaticCompactTable(table.flags));
        ret.put("isView", table.isView());
        ret.put("isVirtual", table.isVirtual());
        return ret;
    }

    private HashMap<String, Serializable> repr(TableParams params)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (params == null) return ret;
        ret.put("minIndexInterval", params.minIndexInterval);
        ret.put("maxIndexInterval", params.maxIndexInterval);
        ret.put("defaultTimeToLive", params.defaultTimeToLive);
        ret.put("gcGraceSeconds", params.gcGraceSeconds);
        ret.put("bloomFilterFpChance", params.bloomFilterFpChance);
        ret.put("cdc", params.cdc);
        ret.put("crcCheckChance", params.crcCheckChance);
        ret.put("memtableFlushPeriodInMs", params.memtableFlushPeriodInMs);
        ret.put("comment", params.comment);
        ret.put("caching", repr(params.caching));
        ret.put("compaction", repr(params.compaction));
        ret.put("compression", repr(params.compression));
        ret.put("memtable", repr(params.memtable));
        if (params.speculativeRetry != null) ret.put("speculativeRetry", params.speculativeRetry.kind().name());
        return ret;
    }

    private HashMap<String, Serializable> repr(CachingParams caching)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (caching == null) return ret;
        ret.putAll(caching.asMap());
        return ret;
    }

    private HashMap<String, Serializable> repr(CompactionParams comp)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (comp == null) return ret;
        ret.putAll(comp.asMap());
        return ret;
    }

    private HashMap<String, Serializable> repr(CompressionParams compr)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (compr == null) return ret;
        ret.putAll(compr.asMap());
        return ret;
    }

    private String repr(MemtableParams params)
    {
        return params.configurationKey();
    }

    private HashMap<String, Serializable> repr(IndexMetadata index)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (index == null) return ret;
        ret.put("name", index.name);
        ret.put("kind", index.kind.name());
        ret.put("id", index.id);
        ret.put("options", new HashMap<>(index.options));
        ret.put("isCustom", index.isCustom());
        ret.put("isKeys", index.isKeys());
        ret.put("isComposites", index.isComposites());
        return ret;
    }

    private List<Map<String, Serializable>> repr(Triggers triggers)
    {
        List<Map<String, Serializable>> ret = new ArrayList<>();
        if (triggers == null) return ret;
        Iterator<TriggerMetadata> iter = triggers.iterator();
        while (iter.hasNext()) ret.add(repr(iter.next()));
        return ret;
    }

    private HashMap<String, Serializable> repr(TriggerMetadata trigger)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (trigger == null) return ret;
        ret.put("name", trigger.name);
        ret.put("classOption", trigger.classOption);
        return ret;
    }

    private HashMap<String, Serializable> repr(ColumnMetadata col)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (col == null) return ret;
        ret.put("name", col.name.toString());
        ret.put("kind", col.kind.name());
        ret.put("type", col.type.toString());
        ret.put("ksName", col.ksName);
        ret.put("cfName", col.cfName);
        ret.put("position", col.position());
        ret.put("clusteringOrder", col.clusteringOrder().name());
        ret.put("isComplex", col.isComplex());
        ret.put("isStatic", col.isStatic());
        ret.put("isPrimaryKeyColumn", col.isPrimaryKeyColumn());
        ret.put("isSimple", col.isSimple());
        ret.put("isPartitionKey", col.isPartitionKey());
        ret.put("isClusteringColumn", col.isClusteringColumn());
        ret.put("isCounterColumn", col.isCounterColumn());
        ret.put("isRegular", col.isRegular());
        return ret;
    }

    private HashMap<String, Serializable> repr(DroppedColumn column)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (column == null) return ret;
        ret.put("droppedTime", column.droppedTime);
        ret.put("column", repr(column.column));
        return ret;
    }
}
