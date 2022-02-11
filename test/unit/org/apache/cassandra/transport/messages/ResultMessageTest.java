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

package org.apache.cassandra.transport.messages;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.transport.Envelope;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.transport.messages.ResultMessage.SchemaChange;
import org.apache.cassandra.transport.messages.ResultMessage.SetKeyspace;
import org.apache.cassandra.utils.MD5Digest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ResultMessageTest
{
    @Test
    public void testSchemaChange()
    {
        Event.SchemaChange scEvent = new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, "ks", "test");
        SchemaChange sc1 = new SchemaChange(scEvent);
        assertThat(sc1.change.keyspace).isEqualTo("ks");
        SchemaChange sc2 = overrideKeyspace(sc1);
        assertThat(sc2.change.keyspace).isEqualTo("ks_123");
    }

    @Test
    public void testPrepared()
    {
        ColumnSpecification cs1 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("a", true), Int32Type.instance);
        ColumnSpecification cs2 = new ColumnSpecification("ks2", "cf2", new ColumnIdentifier("b", true), Int32Type.instance);
        ResultSet.PreparedMetadata preparedMetadata = new ResultSet.PreparedMetadata(Arrays.asList(cs1, cs2), new short[]{ 2, 4, 6 });
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Arrays.asList(cs1, cs2));
        ResultMessage.Prepared p1 = new ResultMessage.Prepared(mock(MD5Digest.class), mock(MD5Digest.class), preparedMetadata, resultMetadata);
        ResultMessage.Prepared p2 = overrideKeyspace(p1);
        assertThat(p2.metadata.names.stream().map(cs -> cs.ksName)).containsExactly("ks1_123", "ks2_123");
        assertThat(p2.resultMetadata.names.stream().map(cs -> cs.ksName)).containsExactly("ks1_123", "ks2_123");
    }

    @Test
    public void testSetKeyspace()
    {
        SetKeyspace sk1 = new SetKeyspace("ks");
        SetKeyspace sk2 = overrideKeyspace(sk1);
        assertThat(sk2.keyspace).isEqualTo("ks_123");
    }

    @Test
    public void testRows()
    {
        ColumnSpecification cs1 = new ColumnSpecification("ks1", "cf1", new ColumnIdentifier("a", true), Int32Type.instance);
        ColumnSpecification cs2 = new ColumnSpecification("ks2", "cf2", new ColumnIdentifier("b", true), Int32Type.instance);
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(Arrays.asList(cs1, cs2));
        ResultSet rs = new ResultSet(resultMetadata, mock(List.class));
        Rows r1 = new Rows(rs);
        Rows r2 = overrideKeyspace(r1);
        assertThat(r2.result.metadata.names.stream().map(cs -> cs.ksName)).containsExactly("ks1_123", "ks2_123");
    }

    private <T extends ResultMessage<T>> T overrideKeyspace(ResultMessage<T> rm)
    {
        T rm2 = rm.withOverriddenKeyspace(Constants.IDENTITY_STRING_MAPPER);
        assertThat(rm2).isSameAs(rm);
        T rm3 = rm2.withOverriddenKeyspace(ks -> ks);
        assertThat(rm3).isSameAs(rm);

        rm.setWarnings(mock(List.class));
        rm.setCustomPayload(mock(Map.class));
        rm.setSource(mock(Envelope.class));
        rm.setStreamId(123);
        T rm4 = rm3.withOverriddenKeyspace(ks -> ks + "_123");
        assertThat(rm4).isNotSameAs(rm);
        assertThat(rm4.getWarnings()).isSameAs(rm.getWarnings());
        assertThat(rm4.getCustomPayload()).isSameAs(rm.getCustomPayload());
        assertThat(rm4.getSource()).isSameAs(rm.getSource());
        assertThat(rm4.getStreamId()).isSameAs(rm.getStreamId());
        return rm4;
    }
}
