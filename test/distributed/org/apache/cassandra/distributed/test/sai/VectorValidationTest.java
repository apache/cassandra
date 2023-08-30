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

package org.apache.cassandra.distributed.test.sai;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.utils.AssertionUtils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VectorValidationTest extends TestBaseImpl
{
    @Test
    public void vectorIndexNotAllowedWithMultipleDataDirectories() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withTokenCount(1)
                                      .withDataDirCount(3)
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.data_dir_test (pk int primary key, val vector<float, 10>)"));
            assertThatThrownBy(() -> cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.data_dir_test(val) USING 'sai'")))
            .is(AssertionUtils.is(InvalidRequestException.class))
            .hasMessage(StorageAttachedIndex.VECTOR_MULTIPLE_DATA_DIRECTORY_ERROR);
        }
    }
}
