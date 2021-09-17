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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

final class PermissionsCacheKeysTable extends AbstractMutableVirtualTable
{
    private static final String ROLE = "role";
    private static final String RESOURCE = "resource";

    PermissionsCacheKeysTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "permissions_cache_keys")
                .comment("keys in the permissions cache")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn(ROLE, UTF8Type.instance)
                .addPartitionKeyColumn(RESOURCE, UTF8Type.instance)
                .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        AuthenticatedUser.permissionsCache.getAll()
                .forEach((userResoursePair, ignored) ->
                        result.row(userResoursePair.left.getName(), userResoursePair.right.getName()));

        return result;
    }

    @Override
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        AuthenticatedUser user = new AuthenticatedUser(partitionKey.value(0));
        IResource resource = resourceFromNameIfExists(partitionKey.value(1));
        // no need to delete invalid resource
        if (resource == null)
            return;

        AuthenticatedUser.permissionsCache.invalidate(Pair.create(user, resource));
    }

    @Override
    public void truncate()
    {
        AuthenticatedUser.permissionsCache.invalidate();
    }

    private IResource resourceFromNameIfExists(String name)
    {
        try
        {
            return Resources.fromName(name);
        }
        catch (IllegalArgumentException e)
        {
            return null;
        }
    }
}
