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
    IDENTITY(null, o -> o, o-> o),
    MILLIS_DURATION(Long.class,
                    o -> DurationSpec.inMilliseconds((Long) o),
                    o -> ((DurationSpec)o).toMilliseconds()),
    MILLIS_DOUBLE_DURATION(Double.class,
                           o ->  DurationSpec.inDoubleMilliseconds((Double) o),
                           o -> ((DurationSpec)o).toMilliseconds()),
    /**
     * This converter is used to support backward compatibility for parameters where in the past -1 was used as a value
     * Example: credentials_update_interval_in_ms = -1 and credentials_update_interval = null (quantity of 0ms) are equal.
     */
    MILLIS_CUSTOM_DURATION(Long.class,
                           o -> (Long)o == -1 ? new DurationSpec("0ms") : DurationSpec.inMilliseconds((Long) o),
                           o -> ((DurationSpec)o).toMilliseconds() == 0 ? -1 : ((DurationSpec)o).toMilliseconds()),
    /**
     * This converter is used to support backward compatibility for Duration parameters where we added the opportunity
     * for the users to add a unit in the parameters' values but we didn't change the names. (key_cache_save_period,
     * row_cache_save_period, counter_cache_save_period)
     * Example: row_cache_save_period = 0 and row_cache_save_period = 0h (quantity of 0h) are equal.
     */
    SECONDS_CUSTOM_DURATION(String.class,
                           o -> DurationSpec.inSecondsString((String) o),
                           o -> ((DurationSpec)o).toSeconds()),
    SECONDS_DURATION(Long.class,
                     o -> DurationSpec.inSeconds((Long) o),
                     o -> ((DurationSpec)o).toSeconds()),
    MINUTES_DURATION(Long.class,
                     o -> DurationSpec.inMinutes((Long) o),
                     o -> ((DurationSpec)o).toMinutes()),
    MEBIBYTES_DATA_STORAGE(Long.class,
                          o -> DataStorageSpec.inMebibytes((Long) o),
                          o -> ((DataStorageSpec)o).toMebibytes()),
    KIBIBYTES_DATASTORAGE(Long.class,
                          o -> DataStorageSpec.inKibibytes((Long) o),
                          o -> ((DataStorageSpec)o).toKibibytes()),
    BYTES_DATASTORAGE(Long.class,
                      o -> DataStorageSpec.inBytes((Long) o),
                      o -> ((DataStorageSpec)o).toBytes()),
    MEBIBYTES_PER_SECOND_DATA_RATE(Long.class,
                                   o -> DataRateSpec.inMebibytesPerSecond((Long) o),
                                   o -> ((DataRateSpec)o).toMebibytesPerSecond()),
    /**
     * This converter is a custom one to support backward compatibility for stream_throughput_outbound and
     * inter_dc_stream_throughput_outbound which were provided in megatibs per second prior CASSANDRA-15234.
     */
    MEGABITS_TO_MEBIBYTES_PER_SECOND_DATA_RATE(Long.class,
                                               o -> DataRateSpec.megabitsPerSecondInMebibytesPerSecond((Long)o),
                                               o -> ((DataRateSpec)o).toMegabitsPerSecond());

    private final Class<?> inputType;
    private final Function<Object, Object> convert;
    private final Function<Object, Object> reverseConvert;

    Converters(Class<?> inputType, Function<Object, Object> convert, Function<Object, Object> reverseConvert)
    {
        this.inputType = inputType;
        this.convert = convert;
        this.reverseConvert = reverseConvert;
    }

    /**
     * A method to identify what type is needed to be returned by the converter used for a configuration parameter
     * in {@link Replaces} annotation in {@link Config}
     *
     * @return class type
     */
    public Class<?> getInputType()
    {
        return inputType;
    }

    /**
     * Apply the converter specified as part of the {@link Replaces} annotation in {@link Config}
     *
     * @param value we will use from cassandra.yaml to create a new {@link Config} parameter of type {@link DurationSpec},
     * {@link DataRateSpec} or {@link DataStorageSpec}
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
    public Object deconvert(Object value)
    {
        if (value == null) return 0;
        return reverseConvert.apply(value);
    }
}
