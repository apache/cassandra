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
import java.util.Collections;
import java.util.List;

import accord.api.ConfigurationService;
import accord.topology.TopologyManager;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;

public class AccordVirtualTables
{

    public static final String SUCCESS = "success";

    private AccordVirtualTables()
    {
    }

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return Collections.emptyList();

        return List.of(new EpochTable(keyspace)
        );
    }

    private static TableMetadata.Builder parse(String keyspace, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .kind(TableMetadata.Kind.VIRTUAL);
    }

    public static class EpochTable extends AbstractVirtualTable
    {
        public EpochTable(String keyspace)
        {
            super(parse(keyspace, "CREATE TABLE accord_epoch (\n" +
                                      "  epoch bigint PRIMARY KEY,\n" +
                                      "  ready_metadata text,\n" +
                                      "  ready_coordinate text,\n" +
                                      "  ready_data text,\n" +
                                      "  ready_reads text,\n" +
                                      "  ready boolean,\n" +
                                      ")")
                  .partitioner(new LocalPartitioner(LongType.instance))
                  .comment("Exposes the epoch state for recieved epochs in Accord")
                  .build());
        }

        @Override
        public DataSet data()
        {
            // This table focuses on epochs that have already been received and does not include inflight epochs nor does it include acknowledge status.
            AccordService service = (AccordService) AccordService.instance();
            SimpleDataSet ds = new SimpleDataSet(metadata());
            TopologyManager tm = service.node().topology();
            long minEpoch = tm.minEpoch();
            long maxEpoch = tm.epoch();
            for (long epoch = minEpoch; epoch <= maxEpoch; epoch++)
            {
                TopologyManager.EpochState state = tm.getEpochStateUnsafe(epoch);
                if (state == null)
                    continue;
                // When state is null there are 2 possible things going on
                // 1) race condition with epoch evicition; this should impact the starting epochs such as min.  If this happens there isn't a reason to display the epochs as they were evicited.
                // 2) gap!  A gap should not be possible and would be a bug (N exists, N + 2 exists, N + 1 does not exist).  This table exposes such a gap by having a missing row.
                ds.row(epoch);
                ConfigurationService.EpochReady ready = state.ready();
                if (ready != null)
                {
                    ds.column("ready_metadata", resultToString(ready.metadata));
                    ds.column("ready_coordinate", resultToString(ready.coordinate));
                    ds.column("ready_data", resultToString(ready.data));
                    ds.column("ready_reads", resultToString(ready.reads));
                    boolean success = ready.reads.isSuccess();
                    ds.column("ready", success);
                    // There is a race condition given these fields are AsyncResults; checking it twice could get different results!
                    // If ready_reads was set to "pending", then the next check its "success", make sure to update the field
                    // to avoid any confussion.
                    if (success)
                        ds.column("ready_reads", SUCCESS);
                }
                else
                {
                    ds.column("ready", false);
                }
                //TODO (operations): include syncTracker? Would give visibility on stuck epochs due to accord's gossip not making progress
                //TODO (operations): include synced, closed, and complete.  If user txn are timing out synced is useful to know.  If sync points are timing out then closed is useful.
            }
            return ds;
        }
    }

    private static String resultToString(AsyncResult<?> result)
    {
        if (result.isDone())
            return result.isSuccess() ? SUCCESS : "failed";
        return "pending";
    }
}
