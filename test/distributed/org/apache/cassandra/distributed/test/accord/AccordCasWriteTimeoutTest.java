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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A write timeout from an Accord LWT has writeType CAS, so on protocol v5 it must carry the contentions short
 * that clients (and our own decoder) read for that write type.
 */
public class AccordCasWriteTimeoutTest extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClassClientInit() // must not hide TestBaseImpl.beforeClass
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void lwtWriteTimeoutIsDecodedOnV5() throws IOException
    {
        // the server must time out before SimpleClient gives up waiting (SimpleClient.TIMEOUT_SECONDS)
        try (Cluster cluster = init(Cluster.build(2)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                             .set("write_request_timeout", "2s"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int) WITH " + TransactionalMode.full.asCqlParam()));
            cluster.filters().outbound().verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id).from(1).to(2).drop();

            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).protocolVersion(ProtocolVersion.V5).build().connect(false))
            {
                Throwable thrown = catchThrowable(() -> client.execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1) IF NOT EXISTS"), ConsistencyLevel.QUORUM));

                assertThat(thrown).hasCauseInstanceOf(CasWriteTimeoutException.class);
                CasWriteTimeoutException timeout = (CasWriteTimeoutException) thrown.getCause();
                assertThat(timeout.writeType).isEqualTo(WriteType.CAS);
                assertThat(timeout.contentions).isZero();
            }

            // make sure the timeout came from Accord and not from Paxos
            long accordWriteTimeouts = cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.accordWriteMetrics.timeouts.getCount());
            assertThat(accordWriteTimeouts).isEqualTo(1);
        }
    }
}
