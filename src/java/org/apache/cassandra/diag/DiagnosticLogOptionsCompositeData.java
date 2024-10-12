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

package org.apache.cassandra.diag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.cassandra.audit.AuditLogOptions;
import org.apache.cassandra.config.ParameterizedClass;

import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.STRING;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.ARCHIVE_COMMAND;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.ENABLED;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.FIELD_SEPARATOR;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.KEY_VALUE_SEPARATOR;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.LOGGER;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.MAX_ARCHIVE_RETRIES;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.MAX_LOG_SIZE;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.MAX_QUEUE_WEIGHT;
import static org.apache.cassandra.diag.DiagnosticLogOptionsCompositeData.DiagnosticLogOption.ROLL_CYCLE;

public class DiagnosticLogOptionsCompositeData
{
    public static class DiagnosticLogOption
    {
        public static final String DIAGNOSTIC_LOGS_DIR = "diagnostic_logs_dir";
        public static final String ARCHIVE_COMMAND = "archive_command";
        public static final String ROLL_CYCLE = "roll_cycle";
        public static final String BLOCK = "block";
        public static final String MAX_QUEUE_WEIGHT = "max_queue_weight";
        public static final String MAX_LOG_SIZE = "max_log_size";
        public static final String MAX_ARCHIVE_RETRIES = "max_archive_retries";
        public static final String ENABLED = "enabled";
        public static final String LOGGER = "logger";
        public static final String KEY_VALUE_SEPARATOR = "key_value_separator";
        public static final String FIELD_SEPARATOR = "field_separator";

        private final String name;
        private final String description;
        private final OpenType<?> type;
        private final Function<DiagnosticLogOptions, Object> toCompositeMapping;
        private final BiConsumer<DiagnosticLogOptions, Object> fromCompositeMapping;

        public DiagnosticLogOption(final String name,
                                   final String description,
                                   final OpenType<?> type,
                                   final Function<DiagnosticLogOptions, Object> toCompositeMapping,
                                   final BiConsumer<DiagnosticLogOptions, Object> fromCompositeMapping)
        {
            this.name = name;
            this.description = description;
            this.type = type;
            this.toCompositeMapping = toCompositeMapping;
            this.fromCompositeMapping = fromCompositeMapping;
        }

        public static DiagnosticLogOption option(final String name,
                                                 final String description,
                                                 final OpenType<?> type,
                                                 final Function<DiagnosticLogOptions, Object> toCompositeMapping,
                                                 final BiConsumer<DiagnosticLogOptions, Object> fromCompositeMapping)
        {
            return new DiagnosticLogOption(name, description, type, toCompositeMapping, fromCompositeMapping);
        }
    }

    private static final DiagnosticLogOption[] options = new DiagnosticLogOption[]{
    DiagnosticLogOption.option(DiagnosticLogOption.DIAGNOSTIC_LOGS_DIR,
                               "directory where audit data are stored",
                               STRING,
                               o -> o.diagnostic_log_dir,
                               (opts, obj) -> opts.diagnostic_log_dir = (String) obj),

    DiagnosticLogOption.option(ARCHIVE_COMMAND,
                               "archive command for audit data",
                               STRING,
                               o -> o.archive_command,
                               (opts, obj) -> opts.archive_command = (String) obj),

    DiagnosticLogOption.option(ROLL_CYCLE,
                               "how often to roll BinLog segments so they can potentially be reclaimed",
                               STRING,
                               o -> o.roll_cycle,
                               (opts, obj) -> opts.roll_cycle = (String) obj),

    DiagnosticLogOption.option(DiagnosticLogOption.BLOCK,
                               "indicates if the BinLog should block if it falls behind or should drop bin log records",
                               BOOLEAN,
                               o -> o.block,
                               (opts, obj) -> opts.block = (Boolean) obj),

    DiagnosticLogOption.option(MAX_QUEUE_WEIGHT,
                               "maximum weight of in-memory queue for records waiting to be written to the binlog file before blocking or dropping the log records",
                               INTEGER,
                               o -> o.max_queue_weight,
                               (opts, obj) -> opts.max_queue_weight = (Integer) obj),

    DiagnosticLogOption.option(MAX_LOG_SIZE,
                               "maximum size of the rolled files to retain on disk before deleting the oldest file",
                               SimpleType.LONG,
                               o -> o.max_log_size,
                               (opts, obj) -> opts.max_log_size = (Long) obj),

    DiagnosticLogOption.option(MAX_ARCHIVE_RETRIES,
                               "number of times to retry an archive command",
                               INTEGER,
                               o -> o.max_archive_retries,
                               (opts, obj) -> opts.max_archive_retries = (Integer) obj),

    DiagnosticLogOption.option(ENABLED,
                               "boolean telling if we are enabled or not",
                               BOOLEAN,
                               o -> o.enabled,
                               (opts, obj) -> opts.enabled = (Boolean) obj),

    DiagnosticLogOption.option(LOGGER,
                               "audit logger implementation class name",
                               STRING,
                               o -> o.logger.class_name,
                               (opts, obj) -> opts.logger = new ParameterizedClass((String) obj, new HashMap<>())),

    DiagnosticLogOption.option(KEY_VALUE_SEPARATOR,
                               "separator for keys and their values",
                               STRING,
                               o -> o.key_value_separator,
                               (opts, obj) -> opts.key_value_separator = (String) obj),

    DiagnosticLogOption.option(FIELD_SEPARATOR,
                               "separator for fields",
                               STRING,
                               o -> o.field_separator,
                               (opts, obj) -> opts.field_separator = (String) obj)
    };

    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPE = new CompositeType(AuditLogOptions.class.getName(),
                                               "DiagnosticLogOptions",
                                               Arrays.stream(options).map(o -> o.name).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.description).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.type).toArray(OpenType[]::new));
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(final DiagnosticLogOptions opts)
    {
        try
        {
            Map<String, Object> valueMap = new HashMap<>();

            for (DiagnosticLogOption option : options)
                valueMap.put(option.name, option.toCompositeMapping.apply(opts));

            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static DiagnosticLogOptions fromCompositeData(final CompositeData data)
    {
        assert data.getCompositeType().equals(COMPOSITE_TYPE);

        List<Object> values = new ArrayList<>();
        for (DiagnosticLogOption option : options)
            values.add(data.get(option.name));

        final DiagnosticLogOptions opts = new DiagnosticLogOptions();

        for (int i = 0; i < values.size(); i++)
            options[i].fromCompositeMapping.accept(opts, values.get(i));

        return opts;
    }
}
