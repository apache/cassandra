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
package org.apache.cassandra.db.virtual;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.InMemoryVirtualTable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class Settings extends InMemoryVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(Settings.class);

    private static final String WRITABLE = "writable";
    private static final String VALUE = "value";
    private static final String SETTING = "setting";

    private static final String PHI_CONVICT_THRESHOLD = "phi_convict_threshold";
    private static final String REQUEST_TIMEOUT_IN_MS = "request_timeout_in_ms";
    private static final String READ_REQUEST_TIMEOUT_IN_MS = "read_request_timeout_in_ms";
    private static final String RANGE_REQUEST_TIMEOUT_IN_MS = "range_request_timeout_in_ms";
    private static final String WRITE_REQUEST_TIMEOUT_IN_MS = "write_request_timeout_in_ms";
    private static final String COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS = "counter_write_request_timeout_in_ms";
    private static final String CAS_CONTENTION_TIMEOUT_IN_MS = "cas_contention_timeout_in_ms";
    private static final String TRUNCATE_REQUEST_TIMEOUT_IN_MS = "truncate_request_timeout_in_ms";
    private static final String STREAM_THROUGHPUT_OUTBOUND_MEGABITS_PER_SEC = "stream_throughput_outbound_megabits_per_sec";
    private static final String INTER_DC_STREAM_THROUGHPUT_OUTBOUND_MEGABITS_PER_SEC = "inter_dc_stream_throughput_outbound_megabits_per_sec";
    private static final String INCREMENTAL_BACKUPS = "incremental_backups";
    private static final String HINTED_HANDOFF_THROTTLE_IN_KB = "hinted_handoff_throttle_in_kb";
    private static final String HINTED_HANDOFF_ENABLED = "hinted_handoff_enabled";
    private static final String TOMBSTONE_FAILURE_THRESHOLD = "tombstone_failure_threshold";
    private static final String TOMBSTONE_WARN_THRESHOLD = "tombstone_warn_threshold";
    private static final String CONCURRENT_VALIDATIONS = "concurrent_validations";
    private static final String CONCURRENT_COMPACTORS = "concurrent_compactors";
    private static final String COMPACTION_THROUGHPUT_MB_PER_SEC = "compaction_throughput_mb_per_sec";
    private static final String BATCH_SIZE_FAIL_THRESHOLD_IN_KB = "batch_size_fail_threshold_in_kb";
    private static final String BATCH_SIZE_WARN_THRESHOLD_IN_KB = "batch_size_warn_threshold_in_kb";

    private static final Field[] FIELDS = Config.class.getFields();

    private static Map<String, Consumer<String>> WRITABLES = ImmutableMap.<String, Consumer<String>>builder()
      .put(BATCH_SIZE_WARN_THRESHOLD_IN_KB, v -> DatabaseDescriptor.setBatchSizeWarnThresholdInKB(Integer.parseInt(v)))
      .put(BATCH_SIZE_FAIL_THRESHOLD_IN_KB, v -> DatabaseDescriptor.setBatchSizeFailThresholdInKB(Integer.parseInt(v)))

      .put(COMPACTION_THROUGHPUT_MB_PER_SEC, v -> StorageService.instance.setCompactionThroughputMbPerSec(Integer.parseInt(v)))
      .put(CONCURRENT_COMPACTORS, v -> StorageService.instance.setConcurrentCompactors(Integer.parseInt(v)))
      .put(CONCURRENT_VALIDATIONS, v -> StorageService.instance.setConcurrentValidators(Integer.parseInt(v)))

      .put(TOMBSTONE_WARN_THRESHOLD, v -> DatabaseDescriptor.setTombstoneWarnThreshold(Integer.parseInt(v)))
      .put(TOMBSTONE_FAILURE_THRESHOLD, v -> DatabaseDescriptor.setTombstoneFailureThreshold(Integer.parseInt(v)))

      .put(HINTED_HANDOFF_ENABLED, v -> StorageProxy.instance.setHintedHandoffEnabled(Boolean.parseBoolean(v)))
      .put(HINTED_HANDOFF_THROTTLE_IN_KB, v -> StorageService.instance.setHintedHandoffThrottleInKB(Integer.parseInt(v)))

      .put(INCREMENTAL_BACKUPS, v -> DatabaseDescriptor.setIncrementalBackupsEnabled(Boolean.parseBoolean(v)))

      .put(INTER_DC_STREAM_THROUGHPUT_OUTBOUND_MEGABITS_PER_SEC, v -> StorageService.instance.setInterDCStreamThroughputMbPerSec(Integer.parseInt(v)))
      .put(STREAM_THROUGHPUT_OUTBOUND_MEGABITS_PER_SEC, v -> StorageService.instance.setStreamThroughputMbPerSec(Integer.parseInt(v)))

      .put(TRUNCATE_REQUEST_TIMEOUT_IN_MS, v -> StorageService.instance.setTruncateRpcTimeout(Long.parseLong(v)))
      .put(CAS_CONTENTION_TIMEOUT_IN_MS, v -> StorageService.instance.setCasContentionTimeout(Long.parseLong(v)))
      .put(COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS, v -> StorageService.instance.setCounterWriteRpcTimeout(Long.parseLong(v)))
      .put(WRITE_REQUEST_TIMEOUT_IN_MS, v -> StorageService.instance.setWriteRpcTimeout(Long.parseLong(v)))
      .put(RANGE_REQUEST_TIMEOUT_IN_MS, v -> StorageService.instance.setRangeRpcTimeout(Long.parseLong(v)))
      .put(READ_REQUEST_TIMEOUT_IN_MS, v -> StorageService.instance.setReadRpcTimeout(Long.parseLong(v)))
      .put(REQUEST_TIMEOUT_IN_MS,v -> StorageService.instance.setRpcTimeout(Long.parseLong(v)))

      .put(PHI_CONVICT_THRESHOLD, v -> DatabaseDescriptor.setPhiConvictThreshold(Double.parseDouble(v)))
      .build();

    static
    {
        Map<String, CQL3Type> definitions = new HashMap<>();
        definitions.put(SETTING, CQL3Type.Native.TEXT);
        definitions.put(VALUE, CQL3Type.Native.TEXT);
        definitions.put(WRITABLE, CQL3Type.Native.BOOLEAN);

        schemaBuilder(definitions)
            .addKey(SETTING)
            .register();
    }

    ColumnMetadata valueColumn;
    public Settings(TableMetadata metadata)
    {
        super(metadata);
        valueColumn = metadata.getColumn(ColumnIdentifier.getInterned(VALUE, false));
    }

    public boolean writable()
    {
        return true;
    }

    /**
     * Execute an update operation.
     *
     * @param partitionKey partition key for the update.
     * @param params parameters of the update.
     */
    @Override
    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
        String setting = metadata.partitionKeyType.getString(partitionKey.getKey());
        if (WRITABLES.get(setting) == null)
            throw new InvalidRequestException(setting + " is not a writable setting.");
        if (row.getCell(valueColumn) == null)
            throw new InvalidRequestException("Only 'value' is updatable.");

        String value = valueColumn.type.getString(row.getCell(valueColumn).value());
        WRITABLES.get(setting).accept(value);
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        Config config = DatabaseDescriptor.getRawConfig();
        for (Field f : FIELDS)
        {
            if (!Modifier.isStatic(f.getModifiers()))
            {
                try
                {
                    Object value = f.get(config);
                    if (value != null && value.getClass().isArray())
                    {
                        StringBuilder s = new StringBuilder("[");
                        for (int i = 0; i < Array.getLength(value); i++)
                        {
                            s.append("'");
                            s.append(Array.get(value, i));
                            s.append("'");
                            if (i < Array.getLength(value) - 1)
                                s.append(", ");
                        }
                        value = s.append("]").toString();
                    }
                    result.row(f.getName())
                            .column(VALUE, value == null ? "--" : value.toString())
                            .column(WRITABLE, WRITABLES.get(f.getName()) != null)
                            .endRow();
                }
                catch (IllegalAccessException | IllegalArgumentException e)
                {
                    logger.error("", e);
                }
            }
        }
    }

}
