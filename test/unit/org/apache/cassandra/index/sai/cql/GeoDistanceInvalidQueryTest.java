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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GeoDistanceInvalidQueryTest extends VectorTester
{
    @Test
    public void geoDistanceRequiresSearchVectorSizeTwo() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5]) < 1000"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for v of type vector<float, 2>; expected 2 elements, but given 1");
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1, 1]) < 1000"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for v of type vector<float, 2>; expected 2 elements, but given 3");
    }

    @Test
    public void geoDistanceRequiresVectorIndexSizeTwo() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        // Even though the search vector size is 2, the index vector size is not 2, so the query is not valid.
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < 1000"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE is only supported against vector<float, 2> columns");

        // Even though the search vector matches the index vector size, the index vector size is not 2, so the query is
        // not valid.
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1, 1]) < 1000"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE is only supported against vector<float, 2> columns");
    }

    @Test
    public void geoDistanceRequiresPositiveSearchRadius() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < 0"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE radius must be non-negative, got 0.0");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) <= 0"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE radius must be non-negative, got 0.0");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < -1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE radius must be non-negative, got -1.0");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) <= -1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE radius must be non-negative, got -1.0");
    }

    @Test
    public void geoDistanceRequiresValidLatLonPositions() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [-90.1, 1]) < 100"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE latitude must be between -90 and 90 degrees, got -90.1");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [90.1, 0]) < 100"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE latitude must be between -90 and 90 degrees, got 90.1");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0, 180.1]) < 100"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE longitude must be between -180 and 180 degrees, got 180.1");

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0, -180.1]) < 100"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("GEO_DISTANCE longitude must be between -180 and 180 degrees, got -180.1");
    }

    @Test
    public void geoDistanceMissingOrIncorrectlyConfiguredIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, x int, v vector<float, 2>, PRIMARY KEY(pk))");

        // Query without index
        assertThatThrownBy(() -> execute( "SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < 1000"))
        .hasMessage(StatementRestrictions.GEO_DISTANCE_REQUIRES_INDEX_MESSAGE)
        .isInstanceOf(InvalidRequestException.class);

        // Intentionally create index with incorrect similarity function
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Query with incorrectly configured index
        assertThatThrownBy(() -> execute( "SELECT pk FROM %s WHERE GEO_DISTANCE(v, [1, 1]) < 1000"))
        .hasMessage(StatementRestrictions.VECTOR_INDEX_PRESENT_NOT_SUPPORT_GEO_DISTANCE_MESSAGE)
        .isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void geoDistanceUsageInIfClause() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, x int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        // GEO_DISTANCE is not parsable at this part of the CQL
        assertThatThrownBy(() -> execute("UPDATE %s SET x = 100 WHERE pk = 1 IF GEO_DISTANCE(v, [1, 1]) < 1000"))
        .isInstanceOf(SyntaxException.class);
    }
}
