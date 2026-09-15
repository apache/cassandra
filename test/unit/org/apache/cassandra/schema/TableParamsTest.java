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

package org.apache.cassandra.schema;

import java.io.IOException;

import org.junit.Test;

import accord.utils.Gen;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CassandraGenerators.TableParamsBuilder;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;


public class TableParamsTest
{
    @Test
    public void serdeLatest()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tableParams()).check(params -> {
            AsymmetricMetadataSerializers.testSerde(output, TableParams.serializer, params, NodeVersion.CURRENT_METADATA_VERSION);
        });
    }

    @Test
    public void serdeFlushCompressionAtEveryVersionSinceV11() throws IOException
    {
        for (Version version : Version.values())
        {
            if (version == Version.UNKNOWN || !version.isAtLeast(Version.V11))
                continue;

            for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
            {
                TableParams params = TableParams.builder().flushCompression(FlushCompressionParams.fromString(option.name())).build();
                TableParams deserialized = serde(params, version);
                assertThat(deserialized.flushCompression).describedAs("version %s", version).isEqualTo(params.flushCompression);
                assertThat(deserialized).describedAs("version %s", version).isEqualTo(params);
            }
        }
    }

    @Test
    public void flushCompressionIsNotSerializedBeforeV11() throws IOException
    {
        TableParams params = TableParams.builder().flushCompression(FlushCompressionParams.fromString("none")).build();
        TableParams expected = params.unbuild().flushCompression(FlushCompressionParams.DEFAULT).build();
        for (Version version : Version.values())
        {
            if (version == Version.UNKNOWN || version.isAtLeast(Version.V11))
                continue;

            TableParams deserialized = serde(params, version);
            assertThat(deserialized.flushCompression).describedAs("version %s", version).isEqualTo(FlushCompressionParams.DEFAULT);
            assertThat(deserialized).describedAs("version %s", version).isEqualTo(expected);
        }
    }

    @Test
    public void flushCompressionDefaultsToAuto()
    {
        TableParams params = TableParams.builder().build();
        assertThat(params.flushCompression).isEqualTo(FlushCompressionParams.DEFAULT);
        assertThat(params.flushCompression.configurationKey).isEqualTo(FlushCompressionParams.Option.auto);
    }

    @Test
    public void flushCompressionParticipatesInEqualsAndHashCode()
    {
        TableParams auto = TableParams.builder().build();
        TableParams none = TableParams.builder().flushCompression(FlushCompressionParams.fromString("none")).build();
        TableParams noneAgain = TableParams.builder().flushCompression(FlushCompressionParams.fromString("none")).build();

        assertThat(none).isNotEqualTo(auto);
        assertThat(none).isEqualTo(noneAgain);
        assertThat(none.hashCode()).isEqualTo(noneAgain.hashCode());
    }

    @Test
    public void unbuildPreservesFlushCompression()
    {
        for (FlushCompressionParams.Option option : FlushCompressionParams.Option.values())
        {
            TableParams params = TableParams.builder().flushCompression(FlushCompressionParams.fromString(option.name())).build();
            assertThat(params.unbuild().build()).isEqualTo(params);
            assertThat(TableParams.builder(params).build().flushCompression.configurationKey).isEqualTo(option);
        }
    }

    @Test
    public void toStringAndCqlStringContainFlushCompression()
    {
        TableParams params = TableParams.builder().flushCompression(FlushCompressionParams.fromString("fast")).build();
        assertThat(params.toString()).contains("flush_compression=fast");

        CqlBuilder builder = new CqlBuilder();
        params.appendCqlTo(builder, false);
        assertThat(builder.toString()).contains("AND flush_compression = 'fast'");
    }

    private static TableParams serde(TableParams params, Version version) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        TableParams.serializer.serialize(params, out, version);
        assertThat(out.getLength()).isEqualTo(TableParams.serializer.serializedSize(params, version));
        return TableParams.serializer.deserialize(new DataInputBuffer(out.toByteArray()), version);
    }

    private static Gen<TableParams> tableParams()
    {
        return Generators.toGen(new TableParamsBuilder()
                                .withKnownMemtables()
                                .withTransactionalMode()
                                .withFastPathStrategy()
                                .withFlushCompression()
                                .build());
    }
}