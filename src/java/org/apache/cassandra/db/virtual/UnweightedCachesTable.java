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
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;
import org.apache.cassandra.schema.TableMetadata;

final class UnweightedCachesTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String CAPACITY = "capacity";
    private static final String ENTRY_COUNT = "entry_count";
    private static final String HIT_COUNT = "hit_count";
    private static final String HIT_RATIO = "hit_ratio";
    private static final String RECENT_REQUEST_RATE_PER_SECOND = "recent_request_rate_per_second";
    private static final String RECENT_HIT_RATE_PER_SECOND = "recent_hit_rate_per_second";
    private static final String REQUEST_COUNT = "request_count";

    UnweightedCachesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "unweighted_caches")
                           .comment("system unweighted caches")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(CAPACITY, LongType.instance)
                           .addRegularColumn(ENTRY_COUNT, Int32Type.instance)
                           .addRegularColumn(HIT_COUNT, LongType.instance)
                           .addRegularColumn(HIT_RATIO, DoubleType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND, LongType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND, LongType.instance)
                           .addRegularColumn(REQUEST_COUNT, LongType.instance)
                           .build());
    }

    private void addRow(SimpleDataSet result, String name, UnweightedCacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY, metrics.capacity.getValue())
              .column(ENTRY_COUNT, metrics.entries.getValue())
              .column(HIT_COUNT, metrics.hits.getCount())
              .column(HIT_RATIO, metrics.hitRate.getValue())
              .column(RECENT_HIT_RATE_PER_SECOND, (long) metrics.hits.getFifteenMinuteRate())
              .column(RECENT_REQUEST_RATE_PER_SECOND, (long) metrics.requests.getFifteenMinuteRate())
              .column(REQUEST_COUNT, metrics.requests.getCount());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator instanceof PasswordAuthenticator)
            addRow(result, "credentials", ((PasswordAuthenticator) authenticator).getCredentialsCache().getMetrics());
        addRow(result, "jmx_permissions", AuthorizationProxy.jmxPermissionsCache.getMetrics());
        addRow(result, "network_permissions", AuthenticatedUser.networkPermissionsCache.getMetrics());
        addRow(result, "permissions", AuthenticatedUser.permissionsCache.getMetrics());
        addRow(result, "roles", Roles.cache.getMetrics());

        return result;
    }
}
