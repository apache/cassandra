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

package org.apache.cassandra.distributed.test.log;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_ALLOW_SIMPLE_STRATEGY;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_MINIMUM_REPLICATION_FACTOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;
import static org.apache.cassandra.config.CassandraRelevantProperties.IO_NETTY_TRANSPORT_NONATIVE;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J2_DISABLE_JMX_LEGACY;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG4J_SHUTDOWN_HOOK_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public class FuzzTestBase extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        init();
    }

    public static void init()
    {
        // setting both ways as changes between versions
        LOG4J2_DISABLE_JMX.setBoolean(true);
        LOG4J2_DISABLE_JMX_LEGACY.setBoolean(true);
        LOG4J_SHUTDOWN_HOOK_ENABLED.setBoolean(false);
        CASSANDRA_ALLOW_SIMPLE_STRATEGY.setBoolean(true);
        CASSANDRA_MINIMUM_REPLICATION_FACTOR.setInt(0);
        DISABLE_TCACTIVE_OPENSSL.setBoolean(true);
        IO_NETTY_TRANSPORT_NONATIVE.setBoolean(true);
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
    }


    @Override
    public Cluster.Builder builder() {
        return super.builder()
                    .withConfig(cfg -> cfg.set("cms_default_max_retries", Integer.MAX_VALUE)
                                          .set("cms_default_retry_backoff", "1000ms")
                                          // Since we'll be pausing the commit request, it may happen that it won't get
                                          // unpaused before event expiration.
                                          .set("cms_await_timeout", String.format("%dms", TimeUnit.MINUTES.toMillis(10))));
    }
}
