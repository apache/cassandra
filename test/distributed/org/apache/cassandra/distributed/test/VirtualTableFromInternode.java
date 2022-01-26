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
import java.io.Serializable;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Future;

public class VirtualTableFromInternode extends TestBaseImpl implements Serializable
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start())
        {
            // can we do a cql query to self?
            cluster.get(1).runOnInstance(() -> {


                for (InetAddressAndPort address : Gossiper.instance.getLiveMembers())
                {
                    UntypedResultSet rs = QueryProcessor.execute(address, "SELECT * FROM system_views.settings WHERE name=?", "rpc_address")
                                                        .syncUninterruptibly().getNow();
                    StringBuilder sb = new StringBuilder();
                    sb.append(address).append('\n');
                    for (UntypedResultSet.Row row : rs)
                    {
                        sb.append(row.getString("value"));
                    }
                    System.err.println(sb);
//                    rs.
//                    Future<Message<ReadResponse>> promise = MessagingService.instance().sendWithResult(msg, address);
//                    ReadResponse rsp = promise.syncUninterruptibly().getNow().payload;
//
//                    UnfilteredPartitionIterator it = rsp.makeIterator(rc);
//                    while (it.hasNext())
//                    {
//                        try (UnfilteredRowIterator partition = it.next())
//                        {
//
//                        }
//                    }

//                    StringBuilder sb = new StringBuilder();
//                    sb.append(address).append("\n");
//                    while (it.hasNext())
//                    {
//                        UnfilteredRowIterator row = it.next();
//                        while (row.hasNext())
//                        {
//                            Unfiltered cell = row.next();
//                            cell.
//                        }
//                    }
//                    if (rsp instanceof ErrorMessage)
//                    {
//                        ErrorMessage error = (ErrorMessage) rsp;
//                        if (error.error instanceof Throwable)
//                            throw Throwables.unchecked((Throwable) error.error);
//                    }
//                    assert rsp instanceof ResultMessage.Rows: String.format("Unsupported response type: %s", rsp.getClass());
//                    ResultSet rs = ((ResultMessage.Rows) rsp).result;
//
//                    System.err.println("Settings from " + address + "\n" + QueryResultUtil.formattedTable(rs));
                }

            });
        }
    }
}
