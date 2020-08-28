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

package org.apache.cassandra.fql;

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class FullQueryLoggerOptionsCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{ "log_dir",
                                                             "archive_command",
                                                             "roll_cycle",
                                                             "block",
                                                             "max_queue_weight",
                                                             "max_log_size",
                                                             "max_archive_retries" };

    private static final String[] ITEM_DESC = new String[]{ "directory where FQL data are stored",
                                                            "archive command for FQL data",
                                                            "how often to roll BinLog segments so they can potentially be reclaimed",
                                                            "indicates if the BinLog should block if the it falls behind or should drop bin log records",
                                                            "maximum weight of in memory queue for records waiting to be written to the binlog file before blocking or dropping the log records",
                                                            "maximum size of the rolled files to retain on disk before deleting the oldest file",
                                                            "number of times to retry an archive command" };

    private static final OpenType<?>[] ITEM_TYPES;

    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.BOOLEAN,
                                         SimpleType.INTEGER,
                                         SimpleType.LONG,
                                         SimpleType.INTEGER };

            COMPOSITE_TYPE = new CompositeType(FullQueryLoggerOptions.class.getName(),
                                               "FullQueryLoggerOptions",
                                               ITEM_NAMES,
                                               ITEM_DESC,
                                               ITEM_TYPES);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(final FullQueryLoggerOptions options)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], options.log_dir);
        valueMap.put(ITEM_NAMES[1], options.archive_command);
        valueMap.put(ITEM_NAMES[2], options.roll_cycle);
        valueMap.put(ITEM_NAMES[3], options.block);
        valueMap.put(ITEM_NAMES[4], options.max_queue_weight);
        valueMap.put(ITEM_NAMES[5], options.max_log_size);
        valueMap.put(ITEM_NAMES[6], options.max_archive_retries);

        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static FullQueryLoggerOptions fromCompositeData(final CompositeData data)
    {
        assert data.getCompositeType().equals(COMPOSITE_TYPE);

        final Object[] values = data.getAll(ITEM_NAMES);

        final FullQueryLoggerOptions options = new FullQueryLoggerOptions();

        options.log_dir = (String) values[0];
        options.archive_command = (String) values[1];
        options.roll_cycle = (String) values[2];
        options.block = (Boolean) values[3];
        options.max_queue_weight = (Integer) values[4];
        options.max_log_size = (Long) values[5];
        options.max_archive_retries = (Integer) values[6];

        return options;
    }
}
