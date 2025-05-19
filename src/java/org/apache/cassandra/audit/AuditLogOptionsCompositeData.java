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

package org.apache.cassandra.audit;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.audit.AuditLogOptionsCompositeData.AuditLogOption.option;

public class AuditLogOptionsCompositeData
{
    private static final String MAP_KEY = "key";
    private static final String MAP_VALUE = "value";
    private static final CompositeType MAP_ENTRY_TYPE = createMapEntryType();
    private static final TabularType PARAMETERS_TYPE = createParametersType(MAP_ENTRY_TYPE);

    public static class AuditLogOption
    {
        // TODO these constants will be used in upcoming audit log vtable too, see CASSANDRA-16725
        public static final String AUDIT_LOGS_DIR = "audit_logs_dir";
        public static final String ARCHIVE_COMMAND = "archive_command";
        public static final String ROLL_CYCLE = "roll_cycle";
        public static final String BLOCK = "block";
        public static final String MAX_QUEUE_WEIGHT = "max_queue_weight";
        public static final String MAX_LOG_SIZE = "max_log_size";
        public static final String MAX_ARCHIVE_RETRIES = "max_archive_retries";
        public static final String ENABLED = "enabled";
        public static final String INCLUDED_KEYSPACES = "included_keyspaces";
        public static final String EXCLUDED_KEYSPACES = "excluded_keyspaces";
        public static final String INCLUDED_CATEGORIES = "included_categories";
        public static final String EXCLUDED_CATEGORIES = "excluded_categories";
        public static final String INCLUDED_USERS = "included_users";
        public static final String EXCLUDED_USERS = "excluded_users";
        public static final String LOGGER = "logger";
        public static final String PARAMETERS = "parameters";

        private final String name;
        private final String description;
        private final OpenType<?> type;
        private final Function<AuditLogOptions, Object> toCompositeMapping;
        private final BiConsumer<AuditLogOptions, Object> fromCompositeMapping;

        public AuditLogOption(final String name,
                              final String description,
                              final OpenType<?> type,
                              final Function<AuditLogOptions, Object> toCompositeMapping,
                              final BiConsumer<AuditLogOptions, Object> fromCompositeMapping)
        {
            this.name = name;
            this.description = description;
            this.type = type;
            this.toCompositeMapping = toCompositeMapping;
            this.fromCompositeMapping = fromCompositeMapping;
        }

        public static AuditLogOption option(final String name,
                                            final String description,
                                            final OpenType<?> type,
                                            final Function<AuditLogOptions, Object> toCompositeMapping,
                                            final BiConsumer<AuditLogOptions, Object> fromCompositeMapping)
        {
            return new AuditLogOption(name, description, type, toCompositeMapping, fromCompositeMapping);
        }
    }

    private static final AuditLogOption[] options = new AuditLogOption[]{
    option(AuditLogOption.AUDIT_LOGS_DIR,
           "directory where audit data are stored",
           SimpleType.STRING,
           o -> o.audit_logs_dir,
           (opts, obj) -> opts.audit_logs_dir = (String) obj),

    option(AuditLogOption.ARCHIVE_COMMAND,
           "archive command for audit data",
           SimpleType.STRING,
           o -> o.archive_command,
           (opts, obj) -> opts.archive_command = (String) obj),

    option(AuditLogOption.ROLL_CYCLE,
           "how often to roll BinLog segments so they can potentially be reclaimed",
           SimpleType.STRING,
           o -> o.roll_cycle,
           (opts, obj) -> opts.roll_cycle = (String) obj),

    option(AuditLogOption.BLOCK,
           "indicates if the BinLog should block if it falls behind or should drop bin log records",
           SimpleType.BOOLEAN,
           o -> o.block,
           (opts, obj) -> opts.block = (Boolean) obj),

    option(AuditLogOption.MAX_QUEUE_WEIGHT,
           "maximum weight of in-memory queue for records waiting to be written to the binlog file before blocking or dropping the log records",
           SimpleType.INTEGER,
           o -> o.max_queue_weight,
           (opts, obj) -> opts.max_queue_weight = (Integer) obj),

    option(AuditLogOption.MAX_LOG_SIZE,
           "maximum size of the rolled files to retain on disk before deleting the oldest file",
           SimpleType.LONG,
           o -> o.max_log_size,
           (opts, obj) -> opts.max_log_size = (Long) obj),

    option(AuditLogOption.MAX_ARCHIVE_RETRIES,
           "number of times to retry an archive command",
           SimpleType.INTEGER,
           o -> o.max_archive_retries,
           (opts, obj) -> opts.max_archive_retries = (Integer) obj),

    option(AuditLogOption.ENABLED,
           "boolean telling if we are enabled or not",
           SimpleType.BOOLEAN,
           o -> o.enabled,
           (opts, obj) -> opts.enabled = (Boolean) obj),

    option(AuditLogOption.INCLUDED_KEYSPACES,
           "included keyspaces",
           SimpleType.STRING,
           o -> o.included_keyspaces,
           (opts, obj) -> opts.included_keyspaces = (String) obj),

    option(AuditLogOption.EXCLUDED_KEYSPACES,
           "excluded keyspaces",
           SimpleType.STRING,
           o -> o.excluded_keyspaces,
           (opts, obj) -> opts.excluded_keyspaces = (String) obj),

    option(AuditLogOption.INCLUDED_CATEGORIES,
           "included categories",
           SimpleType.STRING,
           o -> o.included_categories,
           (opts, obj) -> opts.included_categories = (String) obj),

    option(AuditLogOption.EXCLUDED_CATEGORIES,
           "excluded categories",
           SimpleType.STRING,
           o -> o.excluded_categories,
           (opts, obj) -> opts.excluded_categories = (String) obj),

    option(AuditLogOption.INCLUDED_USERS,
           "included users",
           SimpleType.STRING,
           o -> o.included_users,
           (opts, obj) -> opts.included_users = (String) obj),

    option(AuditLogOption.EXCLUDED_USERS,
           "excluded users",
           SimpleType.STRING,
           o -> o.excluded_users,
           (opts, obj) -> opts.excluded_users = (String) obj),

    option(AuditLogOption.LOGGER,
           "audit logger implementation class name",
           SimpleType.STRING,
           o -> o.logger.class_name,
           (opts, obj) -> upsertLogger(opts, (String) obj, null)),

    option(AuditLogOption.PARAMETERS,
           "parameters to be passed to the audit logger implementation class",
           PARAMETERS_TYPE,
           o -> toTabular(o.logger.parameters),
           (opts, obj) -> upsertLogger(opts, null, fromTabular(obj)))
    };


    static CompositeType createMapEntryType() {
        return createMapEntryType(new String[]{MAP_KEY, MAP_VALUE},
                                  new String[]{"Map key", "Map value"},
                                  new OpenType[]{SimpleType.STRING, SimpleType.STRING});
    }

    static CompositeType createMapEntryType(String[] itemNames,
                                            String[] itemDescriptions,
                                            OpenType<?>[] itemTypes) {
        try
        {
            return new CompositeType(
            "MapEntry",
            "Entry in a map",
            itemNames,
            itemDescriptions,
            itemTypes);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException("Failed to initialize PARAMETERS_TYPE", e);
        }
    }

    static TabularType createParametersType(CompositeType entryType) {
        try {
            return new TabularType(
            "Parameters",
            "Map<String, String> parameters",
            entryType,
            new String[]{MAP_KEY});
        } catch (OpenDataException e) {
            throw new RuntimeException("Failed to initialize PARAMETERS_TYPE", e);
        }
    }

    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPE = new CompositeType(AuditLogOptions.class.getName(),
                                               "AuditLogOptions",
                                               Arrays.stream(options).map(o -> o.name).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.description).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.type).toArray(OpenType[]::new));
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static TabularDataSupport toTabular(Map<String, String> map)
    {
        TabularDataSupport table = new TabularDataSupport(PARAMETERS_TYPE);
        if (map == null || map.isEmpty())
            return table;

        map.forEach((k, v) -> {
            try
            {
                Object[] item = new Object[]{ k, v };
                table.put(new CompositeDataSupport(
                MAP_ENTRY_TYPE,
                new String[]{ MAP_KEY, MAP_VALUE },
                item));
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException("Failed to build TabularData", e);
            }
        });
        return table;
    }

    @SuppressWarnings("unchecked")
    private static Map<String,String> fromTabular(Object td)
    {
        Map<String,String> map = new HashMap<>();
        if (td == null)
            return map;

        TabularData table = (TabularData) td;
        table.values().forEach(val -> {
            CompositeData cd = (CompositeData) val;
            map.put((String) cd.get(MAP_KEY), (String) cd.get(MAP_VALUE));
        });
        return map;
    }

    private static void upsertLogger(AuditLogOptions     opts,
                                     String              newName,
                                     Map<String, String> newParams)
    {
        final String className =
        newName != null ? newName.trim()
                        : opts.logger != null
                          ? opts.logger.class_name
                          : BinAuditLogger.class.getSimpleName();

        final Map<String, String> params =
        newParams != null ? newParams
                          : (opts.logger != null && opts.logger.parameters != null)
                            ? opts.logger.parameters
                            : Collections.emptyMap();

        opts.logger = new ParameterizedClass(className, params);
    }

    public static CompositeData toCompositeData(final AuditLogOptions opts)
    {
        try
        {
            final Map<String, Object> valueMap = new HashMap<>();

            for (final Pair<String, Function<AuditLogOptions, Object>> pair : Arrays.stream(options).map(o -> Pair.create(o.name, o.toCompositeMapping)).collect(Collectors.toList()))
            {
                valueMap.put(pair.left, pair.right.apply(opts));
            }

            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static AuditLogOptions fromCompositeData(final CompositeData data)
    {
        assert data.getCompositeType().equals(COMPOSITE_TYPE);

        final Object[] values = data.getAll(Arrays.stream(options).map(o -> o.name).toArray(String[]::new));
        final AuditLogOptions opts = new AuditLogOptions();

        for (int i = 0; i < values.length; i++)
        {
            options[i].fromCompositeMapping.accept(opts, values[i]);
        }

        return opts;
    }
}
