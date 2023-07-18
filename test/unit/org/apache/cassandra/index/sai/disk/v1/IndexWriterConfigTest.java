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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.lucene.index.VectorSimilarityFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IndexWriterConfigTest
{
    @Test
    public void defaultsTest()
    {
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), new HashMap<>());

        assertThat(config.getMaximumNodeConnections()).isEqualTo(16);
        assertThat(config.getConstructionBeamWidth()).isEqualTo(100);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.COSINE);
    }

    @Test
    public void maximumNodeConnectionsTest()
    {
        CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.setBoolean(true);

        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "10");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getMaximumNodeConnections()).isEqualTo(10);

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "-1");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections for index test cannot be <= 0 or > 512, was -1");

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, Integer.toString(IndexWriterConfig.MAXIMUM_MAXIMUM_NODE_CONNECTIONS + 1));
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections for index test cannot be <= 0 or > 512, was " + (IndexWriterConfig.MAXIMUM_MAXIMUM_NODE_CONNECTIONS + 1));

        options.put(IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Maximum number of connections abc is not a valid integer for index test");
    }

    @Test
    public void queueSizeTest()
    {
        CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.setBoolean(true);

        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "150");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getConstructionBeamWidth()).isEqualTo(150);

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "-1");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width for index test cannot be <= 0 or > 3200, was -1");

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, Integer.toString(IndexWriterConfig.MAXIMUM_CONSTRUCTION_BEAM_WIDTH + 1));
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width for index test cannot be <= 0 or > 3200, was " + (IndexWriterConfig.MAXIMUM_CONSTRUCTION_BEAM_WIDTH + 1));

        options.put(IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH, "abc");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Construction beam width abc is not a valid integer for index test");
    }

    @Test
    public void similarityFunctionTest()
    {
        // Similarity function may be changed even when allow_custom_parameters is false
        CassandraRelevantProperties.SAI_HNSW_ALLOW_CUSTOM_PARAMETERS.setBoolean(false);

        Map<String, String> options = new HashMap<>();
        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "DOT_PRODUCT");
        IndexWriterConfig config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);

        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "euclidean");
        config = IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options);
        assertThat(config.getSimilarityFunction()).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);

        options.put(IndexWriterConfig.SIMILARITY_FUNCTION, "blah");
        assertThatThrownBy(() -> IndexWriterConfig.fromOptions("test", VectorType.getInstance(FloatType.instance, 3), options))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Similarity function BLAH was not recognized for index test. Valid values are: EUCLIDEAN, DOT_PRODUCT, COSINE");
    }
}
