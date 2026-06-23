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

package org.apache.cassandra.management;

import org.junit.Test;

import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InternalNodeMBeanAccessorTest extends CQLTester
{
    private final InternalNodeMBeanAccessor accessor = new InternalNodeMBeanAccessor();

    @Test
    public void testFindColumnFamilyResolvesSecondaryIndexStores()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index = createIndex("CREATE INDEX ON %s(v) USING 'legacy_local_table'");
        String indexStoreName = currentTable() + '.' + index;

        assertThat(accessor.findColumnFamily("ColumnFamilies", KEYSPACE, currentTable())).isNotNull();

        ColumnFamilyStoreMBean indexStore = accessor.findColumnFamily("IndexColumnFamilies", KEYSPACE, indexStoreName);
        assertThat(indexStore).isNotNull();
        assertThat(indexStore.getTableName()).isEqualTo(indexStoreName);

        assertThatThrownBy(() -> accessor.findColumnFamily("IndexColumnFamilies", KEYSPACE, currentTable() + ".missing_idx"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Index not found");
    }

    @Test
    public void testFindColumnFamiliesListsSecondaryIndexStores()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index = createIndex("CREATE INDEX ON %s(v) USING 'legacy_local_table'");
        String indexStoreName = currentTable() + '.' + index;

        assertThat(accessor.findColumnFamilies("IndexColumnFamilies"))
        .anySatisfy(e -> {
            assertThat(e.getKey()).isEqualTo(KEYSPACE);
            assertThat(e.getValue().getTableName()).isEqualTo(indexStoreName);
        });

        assertThat(accessor.findColumnFamilies("ColumnFamilies"))
        .anySatisfy(e -> {
            assertThat(e.getKey()).isEqualTo(KEYSPACE);
            assertThat(e.getValue().getTableName()).isEqualTo(currentTable());
        })
        .noneSatisfy(e -> assertThat(e.getValue().getTableName()).isEqualTo(indexStoreName));
    }

    @Test
    public void testFindMBeanResolvesJmxPermissionsCache()
    {
        AuthorizationProxy.JmxPermissionsCache expected = AuthorizationProxy.jmxPermissionsCache;
        assertThat(accessor.findMBean(AuthorizationProxy.JmxPermissionsCacheMBean.class)).isSameAs(expected);
    }

    @Test
    public void testFindCompressionDictionaryContract()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        assertThatThrownBy(() -> accessor.findCompressionDictionary(KEYSPACE, currentTable()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is not enabled or SSTable compressor is not a dictionary compressor");

        assertThatThrownBy(() -> accessor.findCompressionDictionary(KEYSPACE, "table_does_not_exist"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist");

        assertThatThrownBy(() -> accessor.findCompressionDictionary("keyspace_does_not_exist", currentTable()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist");
    }
}
