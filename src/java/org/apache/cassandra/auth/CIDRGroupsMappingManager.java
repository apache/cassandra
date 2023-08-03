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

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;


public class CIDRGroupsMappingManager implements CIDRGroupsMappingManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=CIDRGroupsMappingManager";

    private SelectStatement getCidrGroupsStatement;
    private SelectStatement getCidrsForCidrGroupStatement;

    private static final int PAGE_SIZE = 128;

    public void setup()
    {
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        String getCidrGroupsQuery = String.format("SELECT %s FROM %s.%s",
                                                  AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME,
                                                  SchemaConstants.AUTH_KEYSPACE_NAME,
                                                  AuthKeyspace.CIDR_GROUPS);
        getCidrGroupsStatement = (SelectStatement) QueryProcessor.getStatement(getCidrGroupsQuery,
                                                                               ClientState.forInternalCalls());

        String getCidrsForCidrGroupQuery = String.format("SELECT %s FROM %s.%s where %s = ?",
                                                         AuthKeyspace.CIDR_GROUPS_TBL_CIDRS_COL_NAME,
                                                         SchemaConstants.AUTH_KEYSPACE_NAME,
                                                         AuthKeyspace.CIDR_GROUPS,
                                                         AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME);
        getCidrsForCidrGroupStatement = (SelectStatement) QueryProcessor.getStatement(getCidrsForCidrGroupQuery,
                                                                                      ClientState.forInternalCalls());
    }

    @VisibleForTesting
    ResultMessage.Rows select(SelectStatement statement, QueryOptions options)
    {
        return statement.execute(forInternalCalls(), options, nanoTime());
    }

    UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return QueryProcessor.process(query, cl);
    }

    protected static String getCidrTuplesSetString(List<CIDR> cidrs)
    {
        String inner = cidrs.stream().map(CIDR::asCqlTupleString)
                            .collect(Collectors.joining(", "));
        return '{' + inner + '}';
    }

    public UntypedResultSet getCidrGroupsTableEntries()
    {
        String getAllRowsQuery = String.format("SELECT * FROM %s.%s",
                                               SchemaConstants.AUTH_KEYSPACE_NAME,
                                               AuthKeyspace.CIDR_GROUPS);

        return QueryProcessor.executeInternalWithPaging(getAllRowsQuery, PAGE_SIZE);
    }

    public Set<String> getAvailableCidrGroups()
    {
        Set<String> availableCidrGroups = new HashSet<>();

        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Collections.emptyList());

        ResultMessage.Rows rows = select(getCidrGroupsStatement, options);
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        for (UntypedResultSet.Row row : result)
        {
            if (!row.has(AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME))
                throw new IllegalStateException("Invalid row " + row + " in table: " +
                                                SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.CIDR_GROUPS);

            availableCidrGroups.add(row.getString(AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME));
        }

        return availableCidrGroups;
    }

    public Set<Pair<InetAddress, Short>> retrieveCidrsFromRow(UntypedResultSet.Row row)
    {
        if (!row.has(AuthKeyspace.CIDR_GROUPS_TBL_CIDRS_COL_NAME))
            throw new RuntimeException("Invalid row, doesn't have column " +
                                       AuthKeyspace.CIDR_GROUPS_TBL_CIDRS_COL_NAME);

        Set<Pair<InetAddress, Short>> cidrs = new HashSet<>();
        TupleType tupleType = new TupleType(Arrays.asList(InetAddressType.instance, ShortType.instance));

        Set<ByteBuffer> cidrAsTuples = row.getFrozenSet(AuthKeyspace.CIDR_GROUPS_TBL_CIDRS_COL_NAME, tupleType);
        for (ByteBuffer cidrAsTuple : cidrAsTuples)
        {
            ByteBuffer[] splits = tupleType.split(ByteBufferAccessor.instance, cidrAsTuple);
            InetAddress ip = InetAddressType.instance.compose(splits[0]);
            short netMask = ShortType.instance.compose(splits[1]);
            cidrs.add(Pair.create(ip, netMask));
        }

        return cidrs;
    }

    public Set<String> getCidrsOfCidrGroupAsStrings(String cidrGroupName)
    {
        QueryOptions options = QueryOptions.forInternalCalls(CassandraAuthorizer.authReadConsistencyLevel(),
                                                             Lists.newArrayList(ByteBufferUtil.bytes(cidrGroupName)));
        ResultMessage.Rows rows = select(getCidrsForCidrGroupStatement, options);
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        if (result.isEmpty())
            return Collections.emptySet();

        Set<Pair<InetAddress, Short>> allCidrs = retrieveCidrsFromRow(result.one());
        Set<String> cidrStrs = new HashSet<>();
        for (Pair<InetAddress, Short> cidr : allCidrs)
            cidrStrs.add(cidr.left().getHostAddress() + '/' + cidr.right());

        return cidrStrs;
    }

    public void updateCidrGroup(String cidrGroupName, List<String> cidrs)
    {
        List<CIDR> validCidrs = new ArrayList<>();
        for (String cidr : cidrs)
        {
            validCidrs.add(CIDR.getInstance(cidr));
        }

        String query = String.format("UPDATE %s.%s SET %s = %s WHERE %s = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.CIDR_GROUPS,
                                     AuthKeyspace.CIDR_GROUPS_TBL_CIDRS_COL_NAME,
                                     getCidrTuplesSetString(validCidrs),
                                     AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME,
                                     cidrGroupName);

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    @VisibleForTesting
    void dropCidrGroupIfExists(String cidrGroupName)
    {
        String query = String.format("DELETE FROM %s.%s WHERE cidr_group = '%s'",
                                     SchemaConstants.AUTH_KEYSPACE_NAME,
                                     AuthKeyspace.CIDR_GROUPS,
                                     cidrGroupName);

        process(query, CassandraAuthorizer.authWriteConsistencyLevel());
    }

    public void dropCidrGroup(String cidrGroupName)
    {
        Set<String> cidrs = getCidrsOfCidrGroupAsStrings(cidrGroupName);
        if (cidrs.isEmpty())
            throw new RuntimeException("CIDR group '" + cidrGroupName + "' doesn't exists");

        dropCidrGroupIfExists(cidrGroupName);
    }

    public void recreateCidrGroupsMapping(Map<String, List<String>> cidrGroupsMapping)
    {
        Set<String> existingMappings = getAvailableCidrGroups();

        // Overwrites mappings of existing cidr groups and inserts new cidr groups
        for (Map.Entry<String, List<String>> cidrGroupMapping : cidrGroupsMapping.entrySet())
        {
            String cidrGroupName = cidrGroupMapping.getKey();
            updateCidrGroup(cidrGroupName, cidrGroupMapping.getValue());
            existingMappings.remove(cidrGroupName);
        }

        // Delete old CIDR groups which do not exist in new mappings
        for (String cidrGroupName : existingMappings)
        {
            dropCidrGroupIfExists(cidrGroupName);
        }
    }

    public Set<String> getCidrGroupsOfIP(String ipStr)
    {
        try
        {
            return DatabaseDescriptor.getCIDRAuthorizer().lookupCidrGroupsForIp(InetAddress.getByName(ipStr));
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException("Invalid IP " + ipStr, e);
        }
    }

    public void loadCidrGroupsCache()
    {
        DatabaseDescriptor.getCIDRAuthorizer().loadCidrGroupsCache();
    }
}
