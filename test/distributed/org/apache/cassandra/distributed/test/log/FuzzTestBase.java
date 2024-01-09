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
import org.apache.cassandra.harry.HarryHelper;

public class FuzzTestBase extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        HarryHelper.init();
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
