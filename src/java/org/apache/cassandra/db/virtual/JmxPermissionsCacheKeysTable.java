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
package org.apache.cassandra.db.virtual;

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

final class JmxPermissionsCacheKeysTable extends AbstractMutableVirtualTable
{
    private static final String ROLE = "role";

    JmxPermissionsCacheKeysTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "jmx_permissions_cache_keys")
                .comment("keys in the JMX permissions cache")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn(ROLE, UTF8Type.instance)
                .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        AuthorizationProxy.jmxPermissionsCache.getAll()
                .forEach((roleResource, ignored) -> result.row(roleResource.getRoleName()));

        return result;
    }

    @Override
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        RoleResource roleResource = RoleResource.role(partitionKey.value(0));

        AuthorizationProxy.jmxPermissionsCache.invalidate(roleResource);
    }

    @Override
    public void truncate()
    {
        AuthorizationProxy.jmxPermissionsCache.invalidate();
    }
}
