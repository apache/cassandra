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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;

public class AlterTableCassandraYamlPropertiesTest extends TestBaseImpl
{
    @Test
    public void testCdcFlag() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config -> {
            if (config.num() == 3)
            {
                config.set("cdc_enabled", true);
            }
        }).start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl1 (id int primary key)");

            Assertions.assertThatThrownBy(() -> cluster.get(1).schemaChangeInternal("alter table " + KEYSPACE + ".tbl1 WITH cdc=true"))
                      .describedAs("Should not be able to enable cdc on a node")
                      .hasRootCauseMessage("cdc_enabled must be set to true to enable cdc on tables")
                      .rootCause().has(new Condition<Throwable>(t -> t.getClass().getCanonicalName()
                                                                      .equals(InvalidRequestException.class.getCanonicalName()), "is instance of InvalidRequestException"));
        }
    }
}
