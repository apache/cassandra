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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Validates and fixes type issues in the serialization-header of sstables.
 */
public abstract class SSTableHeaderFix
{
    // C* 3.0 upgrade code

    private static final String SKIPAUTOMATICUDTFIX = "cassandra.skipautomaticudtfix";
    private static final boolean SKIP_AUTOMATIC_FIX_ON_UPGRADE = Boolean.getBoolean(SKIPAUTOMATICUDTFIX);

    public static void fixNonFrozenUDTIfUpgradeFrom30()
    {
        String previousVersionString = FBUtilities.getPreviousReleaseVersionString();
        if (previousVersionString == null)
            return;
        CassandraVersion previousVersion = new CassandraVersion(previousVersionString);
        if (previousVersion.major != 3 || previousVersion.minor > 0)
        {
            // Not an upgrade from 3.0 to 3.x, nothing to do here
            return;
        }

        if (SKIP_AUTOMATIC_FIX_ON_UPGRADE)
        {
            logger.warn("Detected upgrade from {} to {}, but -D{}=true, NOT fixing UDT type references in " +
                        "sstable metadata serialization-headers",
                        previousVersionString,
                        FBUtilities.getReleaseVersionString(),
                        SKIPAUTOMATICUDTFIX);
            return;
        }

        logger.info("Detected upgrade from {} to {}, fixing UDT type references in sstable metadata serialization-headers",
                    previousVersionString,
                    FBUtilities.getReleaseVersionString());

        SSTableHeaderFix instance = SSTableHeaderFix.builder()
                                                    .schemaCallback(() -> Schema.instance::getCFMetaData)
                                                    .build();
        instance.execute();
    }

    // "regular" SSTableHeaderFix code, also used by StandaloneScrubber.

    private static final Logger logger = LoggerFactory.getLogger(SSTableHeaderFix.class);

    protected final Consumer<String> info;
    protected final Consumer<String> warn;
    protected final Consumer<String> error;
    protected final boolean dryRun;
    protected final Function<Descriptor, CFMetaData> schemaCallback;

    private final List<Descriptor> descriptors;

    private final List<Pair<Descriptor, Map<MetadataType, MetadataComponent>>> updates = new ArrayList<>();
    private boolean hasErrors;

    SSTableHeaderFix(Builder builder)
    {
        this.info = builder.info;
        this.warn = builder.warn;
        this.error = builder.error;
        this.dryRun = builder.dryRun;
        this.schemaCallback = builder.schemaCallback.get();
        this.descriptors = new ArrayList<>(builder.descriptors);
        Objects.requireNonNull(this.info, "info is null");
        Objects.requireNonNull(this.warn, "warn is null");
        Objects.requireNonNull(this.error, "error is null");
        Objects.requireNonNull(this.schemaCallback, "schemaCallback is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder to configure and construct an instance of {@link SSTableHeaderFix}.
     * Default settings:
     * <ul>
     *     <li>log via the slf4j logger of {@link SSTableHeaderFix}</li>
     *     <li>no dry-run (i.e. validate and fix, if no serious errors are detected)</li>
     *     <li>no schema callback</li>
     * </ul>
     * If neither {@link #withDescriptor(Descriptor)} nor {@link #withPath(Path)} are used,
     * all "live" sstables in all data directories will be scanned.
     */
    public static class Builder
    {
        private final List<Path> paths = new ArrayList<>();
        private final List<Descriptor> descriptors = new ArrayList<>();
        private Consumer<String> info = (ln) -> logger.info("{}", ln);
        private Consumer<String> warn = (ln) -> logger.warn("{}", ln);
        private Consumer<String> error = (ln) -> logger.error("{}", ln);
        private boolean dryRun;
        private Supplier<Function<Descriptor, CFMetaData>> schemaCallback = () -> null;

        private Builder()
        {}

        /**
         * Only validate and prepare fix, but do not write updated (fixed) sstable serialization-headers.
         */
        public Builder dryRun()
        {
            dryRun = true;
            return this;
        }

        public Builder info(Consumer<String> output)
        {
            this.info = output;
            return this;
        }

        public Builder warn(Consumer<String> warn)
        {
            this.warn = warn;
            return this;
        }

        public Builder error(Consumer<String> error)
        {
            this.error = error;
            return this;
        }

        /**
         * Manually provide an individual sstable or directory containing sstables.
         *
         * Implementation note: procesing "live" sstables in their data directories as well as sstables
         * in snapshots and backups in the data directories works.
         *
         * But processing sstables that reside somewhere else (i.e. verifying sstables before import)
         * requires the use of {@link #withDescriptor(Descriptor)}.
         */
        public Builder withPath(Path path)
        {
            this.paths.add(path);
            return this;
        }

        public Builder withDescriptor(Descriptor descriptor)
        {
            this.descriptors.add(descriptor);
            return this;
        }

        /**
         * Schema callback to retrieve the schema of a table. Production code always delegates to the
         * live schema ({@code Schema.instance}). Unit tests use this method to feed a custom schema.
         */
        public Builder schemaCallback(Supplier<Function<Descriptor, CFMetaData>> schemaCallback)
        {
            this.schemaCallback = schemaCallback;
            return this;
        }

        public SSTableHeaderFix build()
        {
            if (paths.isEmpty() && descriptors.isEmpty())
                return new AutomaticHeaderFix(this);

            return new ManualHeaderFix(this);
        }

        public Builder logToList(List<String> output)
        {
            return info(ln -> output.add("INFO  " + ln))
                   .warn(ln -> output.add("WARN  " + ln))
                   .error(ln -> output.add("ERROR " + ln));
        }
    }

    public final void execute()
    {
        prepare();

        logger.debug("Processing {} sstables:{}",
                     descriptors.size(),
                     descriptors.stream().map(Descriptor::toString).collect(Collectors.joining("\n    ", "\n    ", "")));

        descriptors.forEach(this::processSSTable);

        if (updates.isEmpty())
            return;

        if (hasErrors)
        {
            info.accept("Stopping due to previous errors. Either fix the errors or specify the ignore-errors option.");
            return;
        }

        if (dryRun)
        {
            info.accept("Not fixing identified and fixable serialization-header issues.");
            return;
        }

        info.accept("Writing new metadata files");
        updates.forEach(descAndMeta -> writeNewMetadata(descAndMeta.left, descAndMeta.right));
        info.accept("Finished writing new metadata files");
    }

    /**
     * Whether {@link #execute()} encountered an error.
     */
    public boolean hasError()
    {
        return hasErrors;
    }

    /**
     * Whether {@link #execute()} found mismatches.
     */
    public boolean hasChanges()
    {
        return !updates.isEmpty();
    }

    abstract void prepare();

    private void error(String format, Object... args)
    {
        hasErrors = true;
        error.accept(String.format(format, args));
    }

    void processFileOrDirectory(Path path)
    {
        Stream.of(path)
              .flatMap(SSTableHeaderFix::maybeExpandDirectory)
              .filter(p -> {
                  File f = p.toFile();
                  return Component.fromFilename(f.getParentFile(), f.getName()).right.type == Component.Type.DATA;
              })
              .map(Path::toString)
              .map(Descriptor::fromFilename)
              .forEach(descriptors::add);
    }

    private static Stream<Path> maybeExpandDirectory(Path path)
    {
        if (Files.isRegularFile(path))
            return Stream.of(path);
        return LifecycleTransaction.getFiles(path, (file, fileType) -> fileType == Directories.FileType.FINAL, Directories.OnTxnErr.IGNORE)
                                   .stream()
                                   .map(File::toPath);
    }

    private void processSSTable(Descriptor desc)
    {
        if (desc.cfname.indexOf('.') != -1)
        {
            // secondary index not checked

            // partition-key is the indexed column type
            // clustering-key is org.apache.cassandra.db.marshal.PartitionerDefinedOrder
            // no static columns, no regular columns
            return;
        }

        CFMetaData tableMetadata = schemaCallback.apply(desc);
        if (tableMetadata == null)
        {
            error("Table %s.%s not found in the schema - NOT checking sstable %s", desc.ksname, desc.cfname, desc);
            return;
        }

        Set<Component> components = SSTable.discoverComponentsFor(desc);
        if (components.stream().noneMatch(c -> c.type == Component.Type.STATS))
        {
            error("sstable %s has no -Statistics.db component.", desc);
            return;
        }

        Map<MetadataType, MetadataComponent> metadata = readSSTableMetadata(desc);
        if (metadata == null)
            return;

        MetadataComponent component = metadata.get(MetadataType.HEADER);
        if (!(component instanceof SerializationHeader.Component))
        {
            error("sstable %s: Expected %s, but got %s from metadata.get(MetadataType.HEADER)",
                  desc,
                  SerializationHeader.Component.class.getName(),
                  component != null ? component.getClass().getName() : "'null'");
            return;
        }
        SerializationHeader.Component header = (SerializationHeader.Component) component;

        // check partition key type
        AbstractType<?> keyType = validatePartitionKey(desc, tableMetadata, header);

        // check clustering columns
        List<AbstractType<?>> clusteringTypes = validateClusteringColumns(desc, tableMetadata, header);

        // check static and regular columns
        Map<ByteBuffer, AbstractType<?>> staticColumns = validateColumns(desc, tableMetadata, header.getStaticColumns(), ColumnDefinition.Kind.STATIC);
        Map<ByteBuffer, AbstractType<?>> regularColumns = validateColumns(desc, tableMetadata, header.getRegularColumns(), ColumnDefinition.Kind.REGULAR);

        SerializationHeader.Component newHeader = SerializationHeader.Component.buildComponentForTools(keyType,
                                                                                                       clusteringTypes,
                                                                                                       staticColumns,
                                                                                                       regularColumns,
                                                                                                       header.getEncodingStats());

        // SerializationHeader.Component has no equals(), but a "good" toString()
        if (header.toString().equals(newHeader.toString()))
            return;

        Map<MetadataType, MetadataComponent> newMetadata = new LinkedHashMap<>(metadata);
        newMetadata.put(MetadataType.HEADER, newHeader);

        updates.add(Pair.create(desc, newMetadata));
    }

    private AbstractType<?> validatePartitionKey(Descriptor desc, CFMetaData tableMetadata, SerializationHeader.Component header)
    {
        boolean keyMismatch = false;
        AbstractType<?> headerKeyType = header.getKeyType();
        AbstractType<?> schemaKeyType = tableMetadata.getKeyValidator();
        boolean headerKeyComposite = headerKeyType instanceof CompositeType;
        boolean schemaKeyComposite = schemaKeyType instanceof CompositeType;
        if (headerKeyComposite != schemaKeyComposite)
        {
            // one is a composite partition key, the other is not - very suspicious
            keyMismatch = true;
        }
        else if (headerKeyComposite) // && schemaKeyComposite
        {
            // Note, the logic is similar as just calling 'fixType()' using the composite partition key,
            // but the log messages should use the composite partition key column names.
            List<AbstractType<?>> headerKeyComponents = ((CompositeType) headerKeyType).types;
            List<AbstractType<?>> schemaKeyComponents = ((CompositeType) schemaKeyType).types;
            if (headerKeyComponents.size() != schemaKeyComponents.size())
            {
                // different number of components in composite partition keys - very suspicious
                keyMismatch = true;
                // Just use the original type from the header. Since the number of partition key components
                // don't match, there's nothing to meaningfully validate against.
            }
            else
            {
                // fix components in composite partition key, if necessary
                List<AbstractType<?>> newComponents = new ArrayList<>(schemaKeyComponents.size());
                for (int i = 0; i < schemaKeyComponents.size(); i++)
                {
                    AbstractType<?> headerKeyComponent = headerKeyComponents.get(i);
                    AbstractType<?> schemaKeyComponent = schemaKeyComponents.get(i);
                    AbstractType<?> fixedType = fixType(desc,
                                                        tableMetadata.partitionKeyColumns().get(i).name.bytes,
                                                        headerKeyComponent,
                                                        schemaKeyComponent,
                                                        false);
                    if (fixedType == null)
                        keyMismatch = true;
                    else
                        headerKeyComponent = fixedType;
                    newComponents.add(fixType(desc,
                                              tableMetadata.partitionKeyColumns().get(i).name.bytes,
                                              headerKeyComponent,
                                              schemaKeyComponent,
                                              false));
                }
                headerKeyType = CompositeType.getInstance(newComponents);
            }
        }
        else
        {
            // fix non-composite partition key, if necessary
            AbstractType<?> fixedType = fixType(desc, tableMetadata.partitionKeyColumns().get(0).name.bytes, headerKeyType, schemaKeyType, false);
            if (fixedType == null)
                // non-composite partition key doesn't match and cannot be fixed
                keyMismatch = true;
            else
                headerKeyType = fixedType;
        }
        if (keyMismatch)
            error("sstable %s: Mismatch in partition key type between sstable serialization-header and schema (%s vs %s)",
                  desc,
                  headerKeyType.asCQL3Type(),
                  schemaKeyType.asCQL3Type());
        return headerKeyType;
    }

    private List<AbstractType<?>> validateClusteringColumns(Descriptor desc, CFMetaData tableMetadata, SerializationHeader.Component header)
    {
        List<AbstractType<?>> headerClusteringTypes = header.getClusteringTypes();
        List<AbstractType<?>> clusteringTypes = new ArrayList<>();
        boolean clusteringMismatch = false;
        List<ColumnDefinition> schemaClustering = tableMetadata.clusteringColumns();
        if (schemaClustering.size() != headerClusteringTypes.size())
        {
            clusteringMismatch = true;
            // Just use the original types. Since the number of clustering columns don't match, there's nothing to
            // meaningfully validate against.
            clusteringTypes.addAll(headerClusteringTypes);
        }
        else
        {
            for (int i = 0; i < headerClusteringTypes.size(); i++)
            {
                AbstractType<?> headerType = headerClusteringTypes.get(i);
                ColumnDefinition column = schemaClustering.get(i);
                AbstractType<?> schemaType = column.type;
                AbstractType<?> fixedType = fixType(desc, column.name.bytes, headerType, schemaType, false);
                if (fixedType == null)
                    clusteringMismatch = true;
                else
                    headerType = fixedType;
                clusteringTypes.add(headerType);
            }
        }
        if (clusteringMismatch)
            error("sstable %s: mismatch in clustering columns between sstable serialization-header and schema (%s vs %s)",
                  desc,
                  headerClusteringTypes.stream().map(AbstractType::asCQL3Type).map(CQL3Type::toString).collect(Collectors.joining(",")),
                  schemaClustering.stream().map(cd -> cd.type.asCQL3Type().toString()).collect(Collectors.joining(",")));
        return clusteringTypes;
    }

    private Map<ByteBuffer, AbstractType<?>> validateColumns(Descriptor desc, CFMetaData tableMetadata, Map<ByteBuffer, AbstractType<?>> columns, ColumnDefinition.Kind kind)
    {
        Map<ByteBuffer, AbstractType<?>> target = new LinkedHashMap<>();
        for (Map.Entry<ByteBuffer, AbstractType<?>> nameAndType : columns.entrySet())
        {
            ByteBuffer name = nameAndType.getKey();
            AbstractType<?> type = nameAndType.getValue();

            AbstractType<?> fixedType = validateColumn(desc, tableMetadata, kind, name, type);
            if (fixedType == null)
            {
                error("sstable %s: contains column '%s' of type '%s', which could not be validated",
                      desc,
                      type,
                      logColumnName(name));
                // don't use a "null" type instance
                fixedType = type;
            }

            target.put(name, fixedType);
        }
        return target;
    }

    private AbstractType<?> validateColumn(Descriptor desc, CFMetaData tableMetadata, ColumnDefinition.Kind kind, ByteBuffer name, AbstractType<?> type)
    {
        ColumnDefinition cd = tableMetadata.getColumnDefinition(name);
        if (cd == null)
        {
            // In case the column was dropped, there is not much that we can actually validate.
            // The column could have been recreated using the same or a different kind or the same or
            // a different type. Lottery...

            cd = tableMetadata.getDroppedColumnDefinition(name, kind == ColumnDefinition.Kind.STATIC);
            if (cd == null)
            {
                for (IndexMetadata indexMetadata : tableMetadata.getIndexes())
                {
                    String target = indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME);
                    if (target != null && ByteBufferUtil.bytes(target).equals(name))
                    {
                        warn.accept(String.format("sstable %s: contains column '%s', which is not a column in the table '%s.%s', but a target for that table's index '%s'",
                                                  desc,
                                                  logColumnName(name),
                                                  tableMetadata.ksName,
                                                  tableMetadata.cfName,
                                                  indexMetadata.name));
                        return type;
                    }
                }

                warn.accept(String.format("sstable %s: contains column '%s', which is not present in the schema",
                                          desc,
                                          logColumnName(name)));
            }
            else
            {
                // This is a best-effort approach to handle the case of a UDT column created *AND* dropped in
                // C* 3.0.
                if (type instanceof UserType && cd.type instanceof TupleType)
                {
                    // At this point, we know that the type belongs to a dropped column, recorded with the
                    // dropped column type "TupleType" and using "UserType" in the sstable. So it is very
                    // likely, that this belongs to a dropped UDT. Fix that information to tuple-type.
                    return fixType(desc, name, type, cd.type, true);
                }
            }

            return type;
        }

        // At this point, the column name is known to be a "non-dropped" column in the table.
        if (cd.kind != kind)
            error("sstable %s: contains column '%s' as a %s column, but is of kind %s in the schema",
                  desc,
                  logColumnName(name),
                  kind.name().toLowerCase(),
                  cd.kind.name().toLowerCase());
        else
            type = fixType(desc, name, type, cd.type, false);
        return type;
    }

    private AbstractType<?> fixType(Descriptor desc, ByteBuffer name, AbstractType<?> typeInHeader, AbstractType<?> typeInSchema, boolean droppedColumnMode)
    {
        AbstractType<?> fixedType = fixTypeInner(typeInHeader, typeInSchema, droppedColumnMode);
        if (fixedType != null)
        {
            if (fixedType != typeInHeader)
                info.accept(String.format("sstable %s: Column '%s' needs to be updated from type '%s' to '%s'",
                                          desc,
                                          logColumnName(name),
                                          typeInHeader.asCQL3Type(),
                                          fixedType.asCQL3Type()));
            return fixedType;
        }

        error("sstable %s: contains column '%s' as type '%s', but schema mentions '%s'",
              desc,
              logColumnName(name),
              typeInHeader.asCQL3Type(),
              typeInSchema.asCQL3Type());

        return typeInHeader;
    }

    private AbstractType<?> fixTypeInner(AbstractType<?> typeInHeader, AbstractType<?> typeInSchema, boolean droppedColumnMode)
    {
        if (typeEquals(typeInHeader, typeInSchema))
            return typeInHeader;

        if (typeInHeader instanceof CollectionType)
            return fixTypeInnerCollection(typeInHeader, typeInSchema, droppedColumnMode);

        if (typeInHeader instanceof AbstractCompositeType)
            return fixTypeInnerAbstractComposite(typeInHeader, typeInSchema, droppedColumnMode);

        if (typeInHeader instanceof TupleType)
            return fixTypeInnerAbstractTuple(typeInHeader, typeInSchema, droppedColumnMode);

        // all types, beside CollectionType + AbstractCompositeType + TupleType, should be ok (no nested types) - just check for compatibility
        if (typeInHeader.isCompatibleWith(typeInSchema))
            return typeInHeader;

        return null;
    }

    private AbstractType<?> fixTypeInnerAbstractTuple(AbstractType<?> typeInHeader, AbstractType<?> typeInSchema, boolean droppedColumnMode)
    {
        // This first 'if' handles the case when a UDT has been dropped, as a dropped UDT is recorded as a tuple
        // in dropped_columns. If a UDT is to be replaced with a tuple, then also do that for the inner UDTs.
        if (droppedColumnMode && typeInHeader.getClass() == UserType.class && typeInSchema instanceof TupleType)
            return fixTypeInnerUserTypeDropped((UserType) typeInHeader, (TupleType) typeInSchema);

        if (typeInHeader.getClass() != typeInSchema.getClass())
            return null;

        if (typeInHeader.getClass() == UserType.class)
            return fixTypeInnerUserType((UserType) typeInHeader, (UserType) typeInSchema);

        if (typeInHeader.getClass() == TupleType.class)
            return fixTypeInnerTuple((TupleType) typeInHeader, (TupleType) typeInSchema, droppedColumnMode);

        throw new IllegalArgumentException("Unknown tuple type class " + typeInHeader.getClass().getName());
    }

    private AbstractType<?> fixTypeInnerCollection(AbstractType<?> typeInHeader, AbstractType<?> typeInSchema, boolean droppedColumnMode)
    {
        if (typeInHeader.getClass() != typeInSchema.getClass())
            return null;

        if (typeInHeader.getClass() == ListType.class)
            return fixTypeInnerList((ListType<?>) typeInHeader, (ListType<?>) typeInSchema, droppedColumnMode);

        if (typeInHeader.getClass() == SetType.class)
            return fixTypeInnerSet((SetType<?>) typeInHeader, (SetType<?>) typeInSchema, droppedColumnMode);

        if (typeInHeader.getClass() == MapType.class)
            return fixTypeInnerMap((MapType<?, ?>) typeInHeader, (MapType<?, ?>) typeInSchema, droppedColumnMode);

        throw new IllegalArgumentException("Unknown collection type class " + typeInHeader.getClass().getName());
    }

    private AbstractType<?> fixTypeInnerAbstractComposite(AbstractType<?> typeInHeader, AbstractType<?> typeInSchema, boolean droppedColumnMode)
    {
        if (typeInHeader.getClass() != typeInSchema.getClass())
            return null;

        if (typeInHeader.getClass() == CompositeType.class)
            return fixTypeInnerComposite((CompositeType) typeInHeader, (CompositeType) typeInSchema, droppedColumnMode);

        if (typeInHeader.getClass() == DynamicCompositeType.class)
        {
            // Not sure if we should care about UDTs in DynamicCompositeType at all...
            if (!typeInHeader.isCompatibleWith(typeInSchema))
                return null;

            return typeInHeader;
        }

        throw new IllegalArgumentException("Unknown composite type class " + typeInHeader.getClass().getName());
    }

    private AbstractType<?> fixTypeInnerUserType(UserType cHeader, UserType cSchema)
    {
        if (!cHeader.keyspace.equals(cSchema.keyspace) || !cHeader.name.equals(cSchema.name))
            // different UDT - bummer...
            return null;

        if (cHeader.isMultiCell() != cSchema.isMultiCell())
        {
            if (cHeader.isMultiCell() && !cSchema.isMultiCell())
            {
                // C* 3.0 writes broken SerializationHeader.Component instances - i.e. broken UDT type
                // definitions into the sstable -Stats.db file, because 3.0 does not enclose frozen UDTs
                // (and all UDTs in 3.0 were frozen) with an '' bracket. Since CASSANDRA-7423 (support
                // for non-frozen UDTs, committed to C* 3.6), that frozen-bracket is quite important.
                // Non-frozen (= multi-cell) UDTs are serialized in a fundamentally different way than
                // frozen UDTs in sstables - most importantly, the order of serialized columns depends on
                // the type: fixed-width types first, then variable length types (like frozen types),
                // multi-cell types last. If C* >= 3.6 reads an sstable with a UDT that's written by
                // C* < 3.6, a variety of CorruptSSTableExceptions get logged and clients will encounter
                // read errors.
                // At this point, we know that the type belongs to a "live" (non-dropped) column, so it
                // is safe to correct the information from the header.
                return cSchema;
            }

            // In all other cases, there's not much we can do.
            return null;
        }

        return cHeader;
    }

    private AbstractType<?> fixTypeInnerUserTypeDropped(UserType cHeader, TupleType cSchema)
    {
        // Do not mess around with the UserType in the serialization header, if the column has been dropped.
        // Only fix the multi-cell status when the header contains it as a multicell (non-frozen) UserType,
        // but the schema says "frozen".
        if (cHeader.isMultiCell() && !cSchema.isMultiCell())
        {
            return new UserType(cHeader.keyspace, cHeader.name, cHeader.fieldNames(), cHeader.fieldTypes(), cSchema.isMultiCell());
        }

        return cHeader;
    }

    private AbstractType<?> fixTypeInnerTuple(TupleType cHeader, TupleType cSchema, boolean droppedColumnMode)
    {
        if (cHeader.size() != cSchema.size())
            // different number of components - bummer...
            return null;
        List<AbstractType<?>> cHeaderFixed = new ArrayList<>(cHeader.size());
        boolean anyChanged = false;
        for (int i = 0; i < cHeader.size(); i++)
        {
            AbstractType<?> cHeaderComp = cHeader.type(i);
            AbstractType<?> cHeaderCompFixed = fixTypeInner(cHeaderComp, cSchema.type(i), droppedColumnMode);
            if (cHeaderCompFixed == null)
                // incompatible, bummer...
                return null;
            cHeaderFixed.add(cHeaderCompFixed);
            anyChanged |= cHeaderComp != cHeaderCompFixed;
        }
        if (anyChanged || cSchema.isMultiCell() != cHeader.isMultiCell())
            // TODO this should create a non-frozen tuple type for the sake of handling a dropped, non-frozen UDT
            return new TupleType(cHeaderFixed);
        return cHeader;
    }

    private AbstractType<?> fixTypeInnerComposite(CompositeType cHeader, CompositeType cSchema, boolean droppedColumnMode)
    {
        if (cHeader.types.size() != cSchema.types.size())
            // different number of components - bummer...
            return null;
        List<AbstractType<?>> cHeaderFixed = new ArrayList<>(cHeader.types.size());
        boolean anyChanged = false;
        for (int i = 0; i < cHeader.types.size(); i++)
        {
            AbstractType<?> cHeaderComp = cHeader.types.get(i);
            AbstractType<?> cHeaderCompFixed = fixTypeInner(cHeaderComp, cSchema.types.get(i), droppedColumnMode);
            if (cHeaderCompFixed == null)
                // incompatible, bummer...
                return null;
            cHeaderFixed.add(cHeaderCompFixed);
            anyChanged |= cHeaderComp != cHeaderCompFixed;
        }
        if (anyChanged)
            return CompositeType.getInstance(cHeaderFixed);
        return cHeader;
    }

    private AbstractType<?> fixTypeInnerList(ListType<?> cHeader, ListType<?> cSchema, boolean droppedColumnMode)
    {
        AbstractType<?> cHeaderElem = cHeader.getElementsType();
        AbstractType<?> cHeaderElemFixed = fixTypeInner(cHeaderElem, cSchema.getElementsType(), droppedColumnMode);
        if (cHeaderElemFixed == null)
            // bummer...
            return null;
        if (cHeaderElem != cHeaderElemFixed)
            // element type changed
            return ListType.getInstance(cHeaderElemFixed, cHeader.isMultiCell());
        return cHeader;
    }

    private AbstractType<?> fixTypeInnerSet(SetType<?> cHeader, SetType<?> cSchema, boolean droppedColumnMode)
    {
        AbstractType<?> cHeaderElem = cHeader.getElementsType();
        AbstractType<?> cHeaderElemFixed = fixTypeInner(cHeaderElem, cSchema.getElementsType(), droppedColumnMode);
        if (cHeaderElemFixed == null)
            // bummer...
            return null;
        if (cHeaderElem != cHeaderElemFixed)
            // element type changed
            return SetType.getInstance(cHeaderElemFixed, cHeader.isMultiCell());
        return cHeader;
    }

    private AbstractType<?> fixTypeInnerMap(MapType<?, ?> cHeader, MapType<?, ?> cSchema, boolean droppedColumnMode)
    {
        AbstractType<?> cHeaderKey = cHeader.getKeysType();
        AbstractType<?> cHeaderVal = cHeader.getValuesType();
        AbstractType<?> cHeaderKeyFixed = fixTypeInner(cHeaderKey, cSchema.getKeysType(), droppedColumnMode);
        AbstractType<?> cHeaderValFixed = fixTypeInner(cHeaderVal, cSchema.getValuesType(), droppedColumnMode);
        if (cHeaderKeyFixed == null || cHeaderValFixed == null)
            // bummer...
            return null;
        if (cHeaderKey != cHeaderKeyFixed || cHeaderVal != cHeaderValFixed)
            // element type changed
            return MapType.getInstance(cHeaderKeyFixed, cHeaderValFixed, cHeader.isMultiCell());
        return cHeader;
    }

    private boolean typeEquals(AbstractType<?> typeInHeader, AbstractType<?> typeInSchema)
    {
        // Quite annoying, but the implementations of equals() on some implementation of AbstractType seems to be
        // wrong, but toString() seems to work in such cases.
        return typeInHeader.equals(typeInSchema) || typeInHeader.toString().equals(typeInSchema.toString());
    }

    private static String logColumnName(ByteBuffer columnName)
    {
        try
        {
            return ByteBufferUtil.string(columnName);
        }
        catch (CharacterCodingException e)
        {
            return "?? " + e;
        }
    }

    private Map<MetadataType, MetadataComponent> readSSTableMetadata(Descriptor desc)
    {
        Map<MetadataType, MetadataComponent> metadata;
        try
        {
            metadata = desc.getMetadataSerializer().deserialize(desc, EnumSet.allOf(MetadataType.class));
        }
        catch (IOException e)
        {
            error("Failed to deserialize metadata for sstable %s: %s", desc, e.toString());
            return null;
        }
        return metadata;
    }

    private void writeNewMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> newMetadata)
    {
        String file = desc.filenameFor(Component.STATS);
        info.accept(String.format("  Writing new metadata file %s", file));
        try
        {
            desc.getMetadataSerializer().rewriteSSTableMetadata(desc, newMetadata);
        }
        catch (IOException e)
        {
            error("Failed to write metadata component for %s: %s", file, e.toString());
            throw new RuntimeException(e);
        }
    }

    /**
     * Fix individually provided sstables or directories containing sstables.
     */
    static class ManualHeaderFix extends SSTableHeaderFix
    {
        private final List<Path> paths;

        ManualHeaderFix(Builder builder)
        {
            super(builder);
            this.paths = builder.paths;
        }

        public void prepare()
        {
            paths.forEach(this::processFileOrDirectory);
        }
    }

    /**
     * Fix all sstables in the configured data-directories.
     */
    static class AutomaticHeaderFix extends SSTableHeaderFix
    {
        AutomaticHeaderFix(Builder builder)
        {
            super(builder);
        }

        public void prepare()
        {
            info.accept("Scanning all data directories...");
            for (Directories.DataDirectory dataDirectory : Directories.dataDirectories)
                scanDataDirectory(dataDirectory);
            info.accept("Finished scanning all data directories...");
        }

        private void scanDataDirectory(Directories.DataDirectory dataDirectory)
        {
            info.accept(String.format("Scanning data directory %s", dataDirectory.location));
            File[] ksDirs = dataDirectory.location.listFiles();
            if (ksDirs == null)
                return;
            for (File ksDir : ksDirs)
            {
                if (!ksDir.isDirectory() || !ksDir.canRead())
                    continue;

                String name = ksDir.getName();

                // silently ignore all system keyspaces
                if (SchemaConstants.isLocalSystemKeyspace(name) || SchemaConstants.isReplicatedSystemKeyspace(name))
                    continue;

                File[] tabDirs = ksDir.listFiles();
                if (tabDirs == null)
                    continue;
                for (File tabDir : tabDirs)
                {
                    if (!tabDir.isDirectory() || !tabDir.canRead())
                        continue;

                    processFileOrDirectory(tabDir.toPath());
                }
            }
        }
    }
}
