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

package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND;

/**
 * Converters for backward compatibility with the old cassandra.yaml where duration, data rate and
 * data storage configuration parameters were provided only by value and the expected unit was part of the configuration
 * parameter name(suffix). (CASSANDRA-15234)
 * It is important to be noted that this converter is not intended to be used when we don't change name of a configuration
 * parameter but we want to add unit. This would always default to the old value provided without a unit at the moment.
 * In case this functionality is needed at some point, please, raise a Jira ticket. There is only one exception handling
 * three parameters (key_cache_save_period, row_cache_save_period, counter_cache_save_period) - the SECONDS_CUSTOM_DURATION
 * converter.
 */
public enum Converters
{
    /**
     * This converter is used when we change the name of a cassandra.yaml configuration parameter but we want to be
     * able to still use the old name too. No units involved.
     */
    IDENTITY(null, null, o -> o, o -> o),
    INTEGER_PRIMITIVE_LONG(Integer.class, long.class, Integer::longValue, Long::intValue),
    MILLIS_DURATION_LONG(Long.class, DurationSpec.LongMillisecondsBound.class,
                         DurationSpec.LongMillisecondsBound::new,
                         o -> o == null ? null : o.toMilliseconds()),
    MILLIS_DURATION_INT(Integer.class, DurationSpec.IntMillisecondsBound.class,
                        DurationSpec.IntMillisecondsBound::new,
                        o -> o == null ? null : o.toMilliseconds()),
    MILLIS_DURATION_DOUBLE(Double.class, DurationSpec.IntMillisecondsBound.class,
                           o -> Double.isNaN(o) ? new DurationSpec.IntMillisecondsBound(0) :
                                new DurationSpec.IntMillisecondsBound(o, TimeUnit.MILLISECONDS),
                           o -> (double) o.toMilliseconds()),
    /**
     * This converter is used to support backward compatibility for parameters where in the past -1 was used as a value
     * Example: credentials_update_interval_in_ms = -1 and credentials_update_interval = null are equal.
     */
    MILLIS_CUSTOM_DURATION(Integer.class, DurationSpec.IntMillisecondsBound.class,
                           o -> o == -1 ? null : new DurationSpec.IntMillisecondsBound(o),
                           o -> o == null ? -1 : o.toMilliseconds()),
    SECONDS_DURATION(Integer.class, DurationSpec.IntSecondsBound.class,
                     DurationSpec.IntSecondsBound::new,
                     o -> o == null ? null : o.toSeconds()),
    NEGATIVE_SECONDS_DURATION(Integer.class, DurationSpec.IntSecondsBound.class,
                              o -> o < 0 ? new DurationSpec.IntSecondsBound(0) : new DurationSpec.IntSecondsBound(o),
                              o -> o == null ? null : o.toSeconds()),
    /**
     * This converter is used to support backward compatibility for Duration parameters where we added the opportunity
     * for the users to add a unit in the parameters' values but we didn't change the names. (key_cache_save_period,
     * row_cache_save_period, counter_cache_save_period)
     * Example: row_cache_save_period = 0 and row_cache_save_period = 0s (quantity of 0s) are equal.
     */
    SECONDS_CUSTOM_DURATION(String.class, DurationSpec.IntSecondsBound.class,
                            DurationSpec.IntSecondsBound::inSecondsString,
                            o -> o == null ? null : Long.toString(o.toSeconds())),
    /**
     * This converter is used to support backward compatibility for parameters where in the past -1 was used as a value
     * Example:  index_summary_resize_interval_in_minutes = -1 and  index_summary_resize_interval = null are equal.
     */
    MINUTES_CUSTOM_DURATION(Integer.class, DurationSpec.IntMinutesBound.class,
                            o -> o == -1 ? null : new DurationSpec.IntMinutesBound(o),
                            o -> o == null ? -1 : o.toMinutes()),
    MEBIBYTES_DATA_STORAGE_LONG(Long.class, DataStorageSpec.LongMebibytesBound.class,
                                DataStorageSpec.LongMebibytesBound::new,
                                o -> o == null ? null : o.toMebibytes()),
    MEBIBYTES_DATA_STORAGE_INT(Integer.class, DataStorageSpec.IntMebibytesBound.class,
                               DataStorageSpec.IntMebibytesBound::new,
                               o -> o == null ? null : o.toMebibytes()),
    NEGATIVE_MEBIBYTES_DATA_STORAGE_INT(Integer.class, DataStorageSpec.IntMebibytesBound.class,
                                        o -> o < 0 ? null : new DataStorageSpec.IntMebibytesBound(o),
                                        o -> o == null ? -1 : o.toMebibytes()),
    KIBIBYTES_DATASTORAGE(Integer.class, DataStorageSpec.IntKibibytesBound.class,
                          DataStorageSpec.IntKibibytesBound::new,
                          o -> o == null ? null : o.toKibibytes()),
    BYTES_DATASTORAGE(Integer.class, DataStorageSpec.IntBytesBound.class,
                      DataStorageSpec.IntBytesBound::new,
                      o -> o == null ? null : o.toBytes()),
    LONG_BYTES_DATASTORAGE_MEBIBYTES_INT(Integer.class, DataStorageSpec.LongBytesBound.class,
                                              o -> o == null ? null : new DataStorageSpec.LongBytesBound(o, DataStorageSpec.DataStorageUnit.MEBIBYTES),
                                              o -> o == null ? null : o.toMebibytesInt()),
    LONG_BYTES_DATASTORAGE_MEBIBYTES_DATASTORAGE(DataStorageSpec.IntMebibytesBound.class, DataStorageSpec.LongBytesBound.class,
                                                 o -> o == null ? null : new DataStorageSpec.LongBytesBound(o.toBytesInLong()),
                                                 o -> o == null ? null : new DataStorageSpec.IntMebibytesBound(o.toMebibytesInt())),
    /**
     * This converter is used to support backward compatibility for parameters where in the past negative number was used as a value
     * Example: native_transport_max_concurrent_requests_in_bytes_per_ip = -1 and native_transport_max_request_data_in_flight_per_ip = null
     * are equal. All negative numbers are printed as 0 in virtual tables.
     */
    BYTES_CUSTOM_DATASTORAGE(Long.class, DataStorageSpec.LongBytesBound.class,
                             o -> o == -1 ? null : new DataStorageSpec.LongBytesBound(o),
                             o -> o == null ? null : o.toBytes()),
    MEBIBYTES_PER_SECOND_DATA_RATE(Integer.class, DataRateSpec.LongBytesPerSecondBound.class,
                                   i -> new DataRateSpec.LongBytesPerSecondBound(i, MEBIBYTES_PER_SECOND),
                                   o -> o == null ? null : o.toMebibytesPerSecondAsInt()),
    /**
     * This converter is a custom one to support backward compatibility for stream_throughput_outbound and
     * inter_dc_stream_throughput_outbound which were provided in megabits per second prior CASSANDRA-15234.
     */
    MEGABITS_TO_BYTES_PER_SECOND_DATA_RATE(Integer.class, DataRateSpec.LongBytesPerSecondBound.class,
                                           i -> DataRateSpec.LongBytesPerSecondBound.megabitsPerSecondInBytesPerSecond(i),
                                           o -> o == null ? null : o.toMegabitsPerSecondAsInt()),
    KEYSPACE_COUNT_THRESHOLD_TO_GUARDRAIL(int.class, int.class, 
                                          i -> i - SchemaConstants.getLocalAndReplicatedSystemKeyspaceNames().size(),
                                          o -> o == null ? null : o + SchemaConstants.getLocalAndReplicatedSystemKeyspaceNames().size()),
    TABLE_COUNT_THRESHOLD_TO_GUARDRAIL(int.class, int.class, 
                                       i -> i - SchemaConstants.getLocalAndReplicatedSystemTableNames().size(), 
                                       o -> o == null ? null : o + SchemaConstants.getLocalAndReplicatedSystemTableNames().size());
    private final Class<?> oldType;
    private final Class<?> newType;
    private final Function<Object, Object> convert;
    private final Function<Object, Object> reverseConvert;

    <Old, New> Converters(Class<Old> oldType, Class<New> newType, Function<Old, New> convert, Function<New, Old> reverseConvert)
    {
        this.oldType = oldType;
        this.newType = newType;
        this.convert = (Function<Object, Object>) convert;
        this.reverseConvert = (Function<Object, Object>) reverseConvert;
    }

    /**
     * A method to identify what type is needed to be returned by the converter used for a configuration parameter
     * in {@link Replaces} annotation in {@link Config}
     *
     * @return class type
     */
    public Class<?> getOldType()
    {
        return oldType;
    }

    /**
     * Expected return type from {@link #convert(Object)}, and input type to {@link #unconvert(Object)}
     *
     * @return type that {@link #convert(Object)} is expected to return
     */
    public Class<?> getNewType()
    {
        return newType;
    }

    /**
     * Apply the converter specified as part of the {@link Replaces} annotation in {@link Config}
     *
     * @param value we will use from cassandra.yaml to create a new {@link Config} parameter of type {@link DurationSpec},
     *              {@link DataRateSpec} or {@link DataStorageSpec}
     * @return new object of type {@link DurationSpec}, {@link DataRateSpec} or {@link DataStorageSpec}
     */
    public Object convert(Object value)
    {
        if (value == null) return null;
        return convert.apply(value);
    }

    /**
     * Apply the converter specified as part of the {@link Replaces} annotation in {@link Config} to get config parameters'
     * values in the old format pre-CASSANDRA-15234 and in the right units, used in the Virtual Tables to ensure backward
     * compatibility
     *
     * @param value we will use to calculate the output value
     * @return the numeric value
     */
    public Object unconvert(Object value)
    {
        return reverseConvert.apply(value);
    }
}
