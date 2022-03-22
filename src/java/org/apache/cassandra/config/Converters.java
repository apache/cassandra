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

import java.util.function.Function;

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
    IDENTITY(null, null, o -> o, o-> o),
    MILLIS_DURATION(Long.class, SmallestDurationMilliseconds.class,
                    SmallestDurationMilliseconds::inMilliseconds,
                    o -> o.toMilliseconds()),
    MILLIS_DOUBLE_DURATION(Double.class, SmallestDurationMilliseconds.class,
                           o -> Double.isNaN(o) ? SmallestDurationMilliseconds.inMilliseconds(0) : SmallestDurationMilliseconds.inDoubleMilliseconds(o),
                           o -> (double) o.toMilliseconds()),
    /**
     * This converter is used to support backward compatibility for parameters where in the past -1 was used as a value
     * Example: credentials_update_interval_in_ms = -1 and credentials_update_interval = null are equal.
     */
    MILLIS_CUSTOM_DURATION(Long.class, SmallestDurationMilliseconds.class,
                           o -> o == -1 ? null : SmallestDurationMilliseconds.inMilliseconds(o),
                           o -> o == null ? -1 : o.toMilliseconds()),
    SECONDS_DURATION(Long.class, SmallestDurationSeconds.class,
                     SmallestDurationSeconds::inSeconds,
                     DurationSpec::toSeconds),
    NEGATIVE_SECONDS_DURATION(Long.class, SmallestDurationSeconds.class,
                              o -> o < 0 ? SmallestDurationSeconds.inSeconds(0) : SmallestDurationSeconds.inSeconds(o),
                              DurationSpec::toSeconds),
    /**
     * This converter is used to support backward compatibility for Duration parameters where we added the opportunity
     * for the users to add a unit in the parameters' values but we didn't change the names. (key_cache_save_period,
     * row_cache_save_period, counter_cache_save_period)
     * Example: row_cache_save_period = 0 and row_cache_save_period = 0s (quantity of 0s) are equal.
     */
    SECONDS_CUSTOM_DURATION(String.class, SmallestDurationSeconds.class,
                            SmallestDurationSeconds::inSecondsString,
                            o -> Long.toString(o.toSeconds())),
    MINUTES_DURATION(Long.class, SmallestDurationMinutes.class,
                     SmallestDurationMinutes::inMinutes,
                     DurationSpec::toMinutes),
    MEBIBYTES_DATA_STORAGE(Long.class, SmallestDataStorageMebibytes.class,
                          SmallestDataStorageMebibytes::inMebibytes,
                          DataStorageSpec::toMebibytes),
    KIBIBYTES_DATASTORAGE(Long.class, SmallestDataStorageKibibytes.class,
                          SmallestDataStorageKibibytes::inKibibytes,
                          DataStorageSpec::toKibibytes),
    BYTES_DATASTORAGE(Long.class, DataStorageSpec.class,
                      DataStorageSpec::inBytes,
                      DataStorageSpec::toBytes),
    /**
     * This converter is used to support backward compatibility for parameters where in the past negative number was used as a value
     * Example: native_transport_max_concurrent_requests_in_bytes_per_ip = -1 and native_transport_max_request_data_in_flight_per_ip = null
     * are equal. All negative numbers are printed as 0 in virtual tables.
     */
    BYTES_CUSTOM_DATASTORAGE(Long.class, DataStorageSpec.class,
                             o -> o == -1 ? null : DataStorageSpec.inBytes(o),
                             DataStorageSpec::toBytes),
    MEBIBYTES_PER_SECOND_DATA_RATE(Long.class, DataRateSpec.class,
                                   DataRateSpec::inMebibytesPerSecond,
                                   o -> (long) o.toMebibytesPerSecondAsInt()),
    /**
     * This converter is a custom one to support backward compatibility for stream_throughput_outbound and
     * inter_dc_stream_throughput_outbound which were provided in megatibs per second prior CASSANDRA-15234.
     */
    MEGABITS_TO_MEBIBYTES_PER_SECOND_DATA_RATE(Long.class, DataRateSpec.class,
                                               DataRateSpec::megabitsPerSecondInMebibytesPerSecond,
                                               o -> (long) o.toMegabitsPerSecondAsInt());

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
     * Expected return type from {@link #convert(Object)}, and input type to {@link #deconvert(Object)}
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
     * {@link DataRateSpec} or {@link DataStorageSpec}
     *
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
     *
     * @return the numeric value
     */
    public Object deconvert(Object value)
    {
        if (value == null) return 0;
        return reverseConvert.apply(value);
    }
}
