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

package org.apache.cassandra.sqel;

import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import static org.apache.cassandra.sqel.SampledQueryEventLoggerOptionsCompositeData.SampledQueryEventLoggerOption.option;

public class SampledQueryEventLoggerOptionsCompositeData
{
    public static class SampledQueryEventLoggerOption
    {
        public static final String ENABLED = "enabled";
        public static final String QUERY_SUCCESS_SAMPLE_RATE = "query_success_sample_rate";
        public static final String QUERY_FAILURE_SAMPLE_RATE = "query_failure_sample_rate";
        public static final String BATCH_SUCCESS_SAMPLE_RATE = "batch_success_sample_rate";
        public static final String BATCH_FAILURE_SAMPLE_RATE = "batch_failure_sample_rate";
        public static final String EXECUTE_SUCCESS_SAMPLE_RATE = "execute_success_sample_rate";
        public static final String EXECUTE_FAILURE_SAMPLE_RATE = "execute_failure_sample_rate";
        public static final String PREPARE_SUCCESS_SAMPLE_RATE = "prepare_success_sample_rate";
        public static final String PREPARE_FAILURE_SAMPLE_RATE = "prepare_failure_sample_rate";
        public static final String AUTH_SUCCESS_SAMPLE_RATE = "auth_success_sample_rate";
        public static final String AUTH_FAILURE_SAMPLE_RATE = "auth_failure_sample_rate";

        private final String name;
        private final String description;
        private final OpenType<?> type;
        private final Function<SampledQueryEventLoggerOptions, Object> toCompositeMapping;
        private final BiConsumer<SampledQueryEventLoggerOptions, Object> fromCompositeMapping;

        public SampledQueryEventLoggerOption(final String name,
                              final String description,
                              final OpenType<?> type,
                              final Function<SampledQueryEventLoggerOptions, Object> toCompositeMapping,
                              final BiConsumer<SampledQueryEventLoggerOptions, Object> fromCompositeMapping)
        {
            this.name = name;
            this.description = description;
            this.type = type;
            this.toCompositeMapping = toCompositeMapping;
            this.fromCompositeMapping = fromCompositeMapping;
        }

        public static SampledQueryEventLoggerOption option(final String name,
                                            final String description,
                                            final OpenType<?> type,
                                            final Function<SampledQueryEventLoggerOptions, Object> toCompositeMapping,
                                            final BiConsumer<SampledQueryEventLoggerOptions, Object> fromCompositeMapping)
        {
            return new SampledQueryEventLoggerOption(name, description, type, toCompositeMapping, fromCompositeMapping);
        }
    }
    private static final SampledQueryEventLoggerOption[] options = new SampledQueryEventLoggerOption[]{
        option(SampledQueryEventLoggerOption.QUERY_SUCCESS_SAMPLE_RATE,
            "The rate at which to log successful queries",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.query_success_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.query_success_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.QUERY_FAILURE_SAMPLE_RATE,
            "The rate at which to log failed queries",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.query_failure_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.query_failure_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.BATCH_SUCCESS_SAMPLE_RATE,
            "The rate at which to log successful batches",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.batch_success_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.batch_success_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.BATCH_FAILURE_SAMPLE_RATE,
            "The rate at which to log failed batches",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.batch_failure_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.batch_failure_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.EXECUTE_SUCCESS_SAMPLE_RATE,
            "The rate at which to log successful execute requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.execute_success_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.execute_success_sample_rate = (double) v
        ),

        option(SampledQueryEventLoggerOption.EXECUTE_FAILURE_SAMPLE_RATE,
            "The rate at which to log failed execute requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.execute_failure_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.execute_failure_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.PREPARE_SUCCESS_SAMPLE_RATE,
            "The rate at which to log successful prepare requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.prepare_success_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.prepare_success_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.PREPARE_FAILURE_SAMPLE_RATE,
            "The rate at which to log failed prepare requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.prepare_failure_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.prepare_failure_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.AUTH_SUCCESS_SAMPLE_RATE,
            "The rate at which to log successful auth requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.auth_success_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.auth_success_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.AUTH_FAILURE_SAMPLE_RATE,
            "The rate at which to log failed auth requests",
            SimpleType.DOUBLE,
            (SampledQueryEventLoggerOptions o) -> o.auth_failure_sample_rate,
            (SampledQueryEventLoggerOptions o, Object v) -> o.auth_failure_sample_rate = (double) v
        ),
        option(SampledQueryEventLoggerOption.ENABLED,
            "Whether the logger is enabled",
            SimpleType.BOOLEAN,
            (SampledQueryEventLoggerOptions o) -> o.enabled,
            (SampledQueryEventLoggerOptions o, Object v) -> o.enabled = (boolean) v
        )
    };

    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPE = new CompositeType(SampledQueryEventLoggerOptions.class.getName(),
                                               "SampledQueryEventLoggerOptions",
                                               Arrays.stream(options).map(o -> o.name).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.description).toArray(String[]::new),
                                               Arrays.stream(options).map(o -> o.type).toArray(OpenType[]::new));
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(final SampledQueryEventLoggerOptions opts)
    {
        try
        {
            final Map<String, Object> valueMap = new HashMap<>();
            for (final SampledQueryEventLoggerOption option : options)
            {
                valueMap.put(option.name, option.toCompositeMapping.apply(opts));
            }
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static SampledQueryEventLoggerOptions fromCompositeData(final CompositeData data)
    {
        assert data.getCompositeType().equals(COMPOSITE_TYPE);
        final Object[] values = data.getAll(Arrays.stream(options).map(o -> o.name).toArray(String[]::new));
        final SampledQueryEventLoggerOptions opts = new SampledQueryEventLoggerOptions();

        for (int i = 0; i < values.length; i++)
        {
            options[i].fromCompositeMapping.accept(opts, values[i]);
        }
        return opts;
    }
}