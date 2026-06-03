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

package org.apache.cassandra.telemetry;

import io.opentelemetry.api.common.AttributeKey;

public final class CassandraAttributes
{
    // Until thread attributes are stabilized, we use these.
    // See https://opentelemetry.io/docs/specs/semconv/registry/attributes/thread/
    public static final AttributeKey<Long> THREAD_ID = AttributeKey.longKey("thread.id");
    public static final AttributeKey<String> THREAD_NAME = AttributeKey.stringKey("thread.name");

    public static final AttributeKey<String> CASSANDRA_DC = AttributeKey.stringKey("cassandra.dc");
    public static final AttributeKey<String> CASSANDRA_RACK = AttributeKey.stringKey("cassandra.rack");
    public static final AttributeKey<String> CASSANDRA_QUERY_TYPE = AttributeKey.stringKey("cassandra.query.type");
    public static final AttributeKey<Long> CASSANDRA_PAGE_SIZE = AttributeKey.longKey("cassandra.page.size");
    public static final AttributeKey<String> CASSANDRA_CONSISTENCY_LEVEL = AttributeKey.stringKey("cassandra.consistency.level");
    public static final AttributeKey<String> CASSANDRA_SERIAL_CONSISTENCY_LEVEL = AttributeKey.stringKey("cassandra.serial.consistency.level");
    public static final AttributeKey<String> CASSANDRA_NET_VERB = AttributeKey.stringKey("cassandra.net.verb");
    public static final AttributeKey<String> CASSANDRA_COORDINATOR_ADDRESS = AttributeKey.stringKey("cassandra.coordinator.address");
    public static final AttributeKey<Long> CASSANDRA_COORDINATOR_PORT = AttributeKey.longKey("cassandra.coordinator.port");

    /** Value for {@code db.system.name} identifying Apache Cassandra. */
    public static final String DB_SYSTEM_NAME_CASSANDRA = "cassandra";

    private CassandraAttributes()
    {
    }
}
