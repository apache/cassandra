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

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.cassandra.config.ParameterizedClass;

public class AuditLogOptionsCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{ "audit_logs_dir",
                                                             "archive_command",
                                                             "roll_cycle",
                                                             "block",
                                                             "max_queue_weight",
                                                             "max_log_size",
                                                             "max_archive_retries",
                                                             "enabled",
                                                             "included_keyspaces",
                                                             "excluded_keyspaces",
                                                             "included_categories",
                                                             "excluded_categories",
                                                             "included_users",
                                                             "excluded_users",
                                                             "logger" };

    private static final String[] ITEM_DESC = new String[]{ "directory where audit data are stored",
                                                            "archive command for audit data",
                                                            "how often to roll BinLog segments so they can potentially be reclaimed",
                                                            "indicates if the BinLog should block if the it falls behind or should drop bin log records",
                                                            "maximum weight of in memory queue for records waiting to be written to the binlog file before blocking or dropping the log records",
                                                            "maximum size of the rolled files to retain on disk before deleting the oldest file",
                                                            "number of times to retry an archive command",
                                                            "boolean telling if we are enabled or not",
                                                            "included keyspaces",
                                                            "excluded keyspaces",
                                                            "included categories",
                                                            "excluded categories",
                                                            "included users",
                                                            "excluded users",
                                                            "audit logger implementation class name" };
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
                                         SimpleType.INTEGER,
                                         SimpleType.BOOLEAN,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.STRING };

            COMPOSITE_TYPE = new CompositeType(AuditLogOptions.class.getName(),
                                               "AuditLogOptions",
                                               ITEM_NAMES,
                                               ITEM_DESC,
                                               ITEM_TYPES);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(final AuditLogOptions options)
    {
        try
        {
            Map<String, Object> valueMap = new HashMap<>();
            valueMap.put(ITEM_NAMES[0], options.audit_logs_dir);
            valueMap.put(ITEM_NAMES[1], options.archive_command);
            valueMap.put(ITEM_NAMES[2], options.roll_cycle);
            valueMap.put(ITEM_NAMES[3], options.block);
            valueMap.put(ITEM_NAMES[4], options.max_queue_weight);
            valueMap.put(ITEM_NAMES[5], options.max_log_size);
            valueMap.put(ITEM_NAMES[6], options.max_archive_retries);
            valueMap.put(ITEM_NAMES[7], options.enabled);
            valueMap.put(ITEM_NAMES[8], options.included_keyspaces);
            valueMap.put(ITEM_NAMES[9], options.excluded_keyspaces);
            valueMap.put(ITEM_NAMES[10], options.included_categories);
            valueMap.put(ITEM_NAMES[11], options.excluded_categories);
            valueMap.put(ITEM_NAMES[12], options.included_users);
            valueMap.put(ITEM_NAMES[13], options.excluded_users);
            valueMap.put(ITEM_NAMES[14], options.logger.class_name);

            // we are not taking parameters of logger into account intentionally,
            // this is (the most probably) going to be read by nodetool getauditlog,
            // if we returned map with options, custom implementations might put there e.g. credentials
            // and we do not want them to be leaked via jmx like that so better to just not return it at all

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

        final Object[] values = data.getAll(ITEM_NAMES);

        final AuditLogOptions options = new AuditLogOptions();

        options.audit_logs_dir = (String) values[0];
        options.archive_command = (String) values[1];
        options.roll_cycle = (String) values[2];
        options.block = (Boolean) values[3];
        options.max_queue_weight = (Integer) values[4];
        options.max_log_size = (Long) values[5];
        options.max_archive_retries = (Integer) values[6];
        options.enabled = (Boolean) values[7];
        options.included_keyspaces = (String) values[8];
        options.excluded_keyspaces = (String) values[9];
        options.included_categories = (String) values[10];
        options.excluded_categories = (String) values[11];
        options.included_users = (String) values[12];
        options.excluded_users = (String) values[13];
        options.logger = new ParameterizedClass((String) values[14], new HashMap<>());

        return options;
    }
}
