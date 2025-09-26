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
 * distributed under this work is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

public class QueryAnalyticsDatapointTest
{
    @Test
    public void testBasicBuilder() {
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .build();

        assertEquals("test-instance", datapoint.getInstance());
        assertEquals("test-cluster", datapoint.getCluster());
        assertEquals("test-keyspace", datapoint.getKeyspace());
        assertEquals("test-table", datapoint.getTable());
        assertEquals("test-partition", datapoint.getPartition());
        assertEquals(Long.valueOf(123456789L), datapoint.getTimestamp());
        assertEquals(Long.valueOf(100L), datapoint.getLatency());
        assertNotNull(datapoint.getProperties());
        assertTrue(datapoint.getProperties().isEmpty());
    }

    @Test
    public void testBuilderWithProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", 42);

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        assertEquals("value1", datapoint.getProperty("key1"));
        assertEquals(42, datapoint.getProperty("key2"));
        assertEquals(2, datapoint.getProperties().size());
    }

    @Test
    public void testBuilderWithPropertiesMap() {
        Map<String, Object> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", 42);

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        assertEquals("value1", datapoint.getProperty("key1"));
        assertEquals(42, datapoint.getProperty("key2"));
        assertEquals(2, datapoint.getProperties().size());
    }

    @Test
    public void testBuilderWithoutOptionalFields() {
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .build();

        assertEquals("test-instance", datapoint.getInstance());
        assertEquals("test-cluster", datapoint.getCluster());
    }

    @Test
    public void testNullPropertyValues() {
        Map<String, Object> props = new HashMap<>();
        props.put("null-key", null);
        props.put("valid-key", "valid-value");

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        assertNull(datapoint.getProperty("null-key"));
        assertEquals("valid-value", datapoint.getProperty("valid-key"));
        assertEquals(1, datapoint.getProperties().size()); 
    }

    @Test
    public void testGetPropertyWithDefault() {
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .build();

        assertNull(datapoint.getProperty("non-existent"));

        // Test with a new datapoint that has properties
        Map<String, Object> props = new HashMap<>();
        props.put("existing", "value");

        QueryAnalyticsDatapoint datapointWithProps = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        assertEquals("value", datapointWithProps.getProperty("existing"));
    }

    @Test
    public void testPropertyMethods() {
        Map<String, Object> props = new HashMap<>();
        props.put("test-angel", "angel-value");

        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        assertFalse(datapoint.hasProperty("non-existent"));
        assertTrue(datapoint.hasProperty("test-angel"));

        assertEquals("angel-value", datapoint.getProperty("test-angel", String.class));
        assertNull(datapoint.getProperty("test-angel", Integer.class)); // wrong type
    }

    @Test
    public void testToString() {
        QueryAnalyticsDatapoint datapoint = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .build();

        String result = datapoint.toString();
        assertTrue(result.contains("test-instance"));
        assertTrue(result.contains("test-cluster"));
        assertTrue(result.contains("test-keyspace"));
        assertTrue(result.contains("test-table"));
        assertTrue(result.contains("test-partition"));
        assertTrue(result.contains("123456789"));
        assertTrue(result.contains("100"));
        assertTrue(result.contains("properties="));
    }

    @Test
    public void testBuilderCopy() {
        Map<String, Object> props = new HashMap<>();
        props.put("key1", "value1");

        QueryAnalyticsDatapoint original = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(props)
            .build();

        QueryAnalyticsDatapoint copy = QueryAnalyticsDatapoint.builder(original).build();

        assertEquals(original.getInstance(), copy.getInstance());
        assertEquals(original.getCluster(), copy.getCluster());
        assertEquals(original.getKeyspace(), copy.getKeyspace());
        assertEquals(original.getTable(), copy.getTable());
        assertEquals(original.getPartition(), copy.getPartition());
        assertEquals(original.getTimestamp(), copy.getTimestamp());
        assertEquals(original.getLatency(), copy.getLatency());
        assertEquals(original.getProperties(), copy.getProperties());
    }

    @Test
    public void testUnbuild() {
        Map<String, Object> originalProps = new HashMap<>();
        originalProps.put("key1", "value1");

        QueryAnalyticsDatapoint original = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(originalProps)
            .build();

        Map<String, Object> modifiedProps = new HashMap<>();
        modifiedProps.put("key1", "value1");
        modifiedProps.put("key2", "value2");

        QueryAnalyticsDatapoint modified = QueryAnalyticsDatapoint.builder()
            .instance("test-instance")
            .cluster("test-cluster")
            .keyspace("test-keyspace")
            .table("test-table")
            .partition("test-partition")
            .timestamp(123456789L)
            .latency(100L)
            .properties(modifiedProps)
            .build();

        assertEquals("value1", modified.getProperty("key1"));
        assertEquals("value2", modified.getProperty("key2"));
        assertEquals(2, modified.getProperties().size());
    }
}
