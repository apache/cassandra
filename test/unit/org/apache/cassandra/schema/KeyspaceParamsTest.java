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

package org.apache.cassandra.schema;

import java.util.Map;

import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

public class KeyspaceParamsTest
{
    private static void setValues(String dc, String rf)
    {
        if (dc == null)
            System.getProperties().remove(SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY.getKey());
        else
            SYSTEM_DISTRIBUTED_NTS_DC_OVERRIDE_PROPERTY.setString(dc);

        if (rf == null)
            System.getProperties().remove(SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.getKey());
        else
            SYSTEM_DISTRIBUTED_NTS_RF_OVERRIDE_PROPERTY.setString(rf);
    }

    @Test
    public void testEmpty()
    {
        setValues(null, null);
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testNoRF()
    {
        setValues("dc1,dc2,dc3", null);
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testNoDC()
    {
        setValues(null, "1");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testEmptyDC()
    {
        setValues("", "1");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testInvalidRF1()
    {
        setValues("dc1,dc2,dc3", "0");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testInvalidRF2()
    {
        setValues("dc1,dc2,dc3", "100");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void testMalformedRF()
    {
        setValues("dc1,dc2,dc3", "asdf");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEmpty();
    }

    @Test
    public void test()
    {
        setValues("dc1,dc2,dc3", "4");
        Map<String, String> result = KeyspaceParams.getSystemDistributedNtsOverride();
        assertThat(result).isEqualTo(ReplicationParams.nts("dc1", "4", "dc2", "4", "dc3", "4").asMap());
    }
}