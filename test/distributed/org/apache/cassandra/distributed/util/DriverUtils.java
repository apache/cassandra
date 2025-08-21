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

package org.apache.cassandra.distributed.util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Maps;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import org.apache.cassandra.cql3.ast.Statement;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.exceptions.RequestFailureReason;

import static org.apache.cassandra.distributed.test.JavaDriverUtils.toDriverCL;

public class DriverUtils
{
    public static ByteBuffer[][] getRowsAsByteBuffer(ResultSet result)
    {
        ColumnDefinitions columns = result.getColumnDefinitions();
        List<ByteBuffer[]> ret = new ArrayList<>();
        for (Row rowVal : result)
        {
            ByteBuffer[] row = new ByteBuffer[columns.size()];
            for (int i = 0; i < columns.size(); i++)
                row[i] = rowVal.getBytesUnsafe(i);
            ret.add(row);
        }
        ByteBuffer[][] a = new ByteBuffer[ret.size()][];
        return ret.toArray(a);
    }

    public static ByteBuffer[][] executeQuery(Session session,
                                              IInstance instance,
                                              int fetchSize,
                                              ConsistencyLevel cl,
                                              Statement stmt)
    {
        SimpleStatement ss = new SimpleStatement(stmt.toCQL(), (Object[]) stmt.bindsEncoded());
        if (fetchSize != Integer.MAX_VALUE)
            ss.setFetchSize(fetchSize);
        if (stmt.kind() == Statement.Kind.MUTATION)
        {
            switch (cl)
            {
                case SERIAL:
                    ss.setSerialConsistencyLevel(toDriverCL(cl));
                    ss.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);
                    break;
                case LOCAL_SERIAL:
                    ss.setSerialConsistencyLevel(toDriverCL(cl));
                    ss.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM);
                    break;
                default:
                    ss.setConsistencyLevel(toDriverCL(cl));
            }
        }
        else
        {
            ss.setConsistencyLevel(toDriverCL(cl));
        }

        var host = getHost(session, instance);
        ss.setHost(host);
        ResultSet result;
        try
        {
            result = session.execute(ss);
        }
        catch (ReadFailureException t)
        {
            throw new AssertionError("failed from=" + Maps.transformValues(t.getFailuresMap(), DriverUtils::safeErrorCode), t);
        }
        catch (WriteFailureException t)
        {
            throw new AssertionError("failed from=" + Maps.transformValues(t.getFailuresMap(), DriverUtils::safeErrorCode), t);
        }
        return getRowsAsByteBuffer(result);
    }

    private static Host getHost(Session session, IInstance instance)
    {
        InetSocketAddress broadcastAddress = instance.config().broadcastAddress();
        return session.getCluster().getMetadata().getAllHosts().stream()
                      .filter(h -> h.getBroadcastSocketAddress().getAddress().equals(broadcastAddress.getAddress()))
                      .filter(h -> h.getBroadcastSocketAddress().getPort() == broadcastAddress.getPort())
                      .findAny()
                      .get();
    }

    private static String safeErrorCode(Integer code)
    {
        try
        {
            return RequestFailureReason.fromCode(code).name();
        }
        catch (IllegalArgumentException e)
        {
            return "Unexpected code " + code + ": " + e.getMessage();
        }
    }
}
