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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CIDRPermissionsCache;
import org.apache.cassandra.auth.CassandraCIDRAuthorizer;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.ICIDRAuthorizer;
import org.apache.cassandra.auth.MutualTlsAuthenticator;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

final class UnweightedCachesTable extends AbstractCacheTable<UnweightedCacheMetrics>
{
    public static final String TABLE_NAME = "unweighted_caches";
    public static final String TABLE_DESCRIPTION = "system unweighted caches";

    public static final String NAME_COLUMN = "name";
    public static final String CAPACITY_COLUMN = "capacity";
    public static final String ENTRY_COUNT_COLUMN = "entry_count";
    public static final String HIT_COUNT_COLUMN = "hit_count";
    public static final String HIT_RATIO_COLUMN = "hit_ratio";
    public static final String RECENT_REQUEST_RATE_PER_SECOND_COLUMN = "recent_request_rate_per_second";
    public static final String RECENT_HIT_RATE_PER_SECOND_COLUMN = "recent_hit_rate_per_second";
    public static final String REQUEST_COUNT_COLUMN = "request_count";

    private static final Collection<Supplier<Optional<Pair<String, UnweightedCacheMetrics>>>> DEFAULT_METRICS_SUPPLIERS = Set.of(
    () -> Optional.of(Pair.create(AuthorizationProxy.JmxPermissionsCacheMBean.CACHE_NAME, AuthorizationProxy.jmxPermissionsCache.getMetrics())),
    () -> Optional.of(Pair.create(NetworkPermissionsCacheMBean.CACHE_NAME, AuthenticatedUser.networkPermissionsCache.getMetrics())),
    () -> Optional.of(Pair.create(PermissionsCacheMBean.CACHE_NAME, AuthenticatedUser.permissionsCache.getMetrics())),
    () -> Optional.of(Pair.create(RolesCacheMBean.CACHE_NAME, Roles.cache.getMetrics())),
    () -> {
        ICIDRAuthorizer cidrAuthorizer = DatabaseDescriptor.getCIDRAuthorizer();
        if (cidrAuthorizer instanceof CassandraCIDRAuthorizer)
            return Optional.of(Pair.create(CIDRPermissionsCache.CACHE_NAME, ((CassandraCIDRAuthorizer) cidrAuthorizer).getCIDRPermissionsCache().getMetrics()));
        else
            return Optional.empty();
    },
    () -> {
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator instanceof PasswordAuthenticator)
        {
            return Optional.of(Pair.create(PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME,
                                           ((PasswordAuthenticator) authenticator).getCredentialsCache().getMetrics()));
        }
        else if (authenticator instanceof MutualTlsAuthenticator)
        {
            return Optional.of(Pair.create(MutualTlsAuthenticator.CACHE_NAME,
                                           ((MutualTlsAuthenticator) authenticator).getIdentityCache().getMetrics()));
        }
        else
            return Optional.empty();
    });

    @VisibleForTesting
    UnweightedCachesTable(String keyspace,
                          Collection<Supplier<Optional<Pair<String, UnweightedCacheMetrics>>>> metricsSuppliers)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_DESCRIPTION)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(CAPACITY_COLUMN, Int32Type.instance)
                           .addRegularColumn(ENTRY_COUNT_COLUMN, Int32Type.instance)
                           .addRegularColumn(HIT_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_RATIO_COLUMN, DoubleType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(REQUEST_COUNT_COLUMN, LongType.instance)
                           .build(),
              metricsSuppliers);
    }


    UnweightedCachesTable(String keyspace)
    {
        this(keyspace, DEFAULT_METRICS_SUPPLIERS);
    }

    @Override
    protected void addRow(SimpleDataSet result, String name, UnweightedCacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY_COLUMN, metrics.maxEntries.getValue())
              .column(ENTRY_COUNT_COLUMN, metrics.entries.getValue())
              .column(HIT_COUNT_COLUMN, metrics.hits.getCount())
              .column(HIT_RATIO_COLUMN, metrics.hitRate.getValue())
              .column(RECENT_HIT_RATE_PER_SECOND_COLUMN, (long) metrics.hits.getFifteenMinuteRate())
              .column(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, (long) metrics.requests.getFifteenMinuteRate())
              .column(REQUEST_COUNT_COLUMN, metrics.requests.getCount());
    }
}
