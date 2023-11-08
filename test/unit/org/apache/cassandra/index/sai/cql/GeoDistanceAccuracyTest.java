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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;
import org.apache.lucene.geo.GeoUtils;

import static org.junit.Assert.assertTrue;

public class GeoDistanceAccuracyTest extends VectorTester
{
    // Number represents the number of results that are within the search radius divided by the number of expected results
    // Note that this recall number is just for random vectors in a box around NYC. These vectors might not
    // be representative of real data, so this test mostly serves to verify the status quo.
    private final static float MIN_EXPECTED_RECALL = 0.85f;

    // Number represents the percent of actual results that are incorrect (i.e. outside the search radius)
    private final static float MAX_EXPECTED_FALSE_POSITIVE_RATE = 0.01f;

    @Test
    public void testRandomVectorsAgainstHaversineDistance()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();
        int numVectors = 20000;
        var vectors = IntStream.range(0, numVectors).mapToObj(s -> Pair.create(s, createRandomNYCVector())).collect(Collectors.toList());

        // Insert the vectors
        for (var vector : vectors)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", vector.left, vector(vector.right));

        double recallRate = 0;
        double falsePositiveRate = 0;
        int queryCount = 100;
        for (int i = 0; i < queryCount; i++)
        {
            var searchVector = createRandomNYCVector();
            // Pick a random distance between 1km and 10km
            var distanceInMeters = getRandom().nextIntBetween(1000, 10000);
            // Get the "correct" results using the great circle distance or the haversine formula
            var closeVectors = vectors.stream()
                                      .filter(v -> isWithinDistance(v.right, searchVector, distanceInMeters))
                                      .collect(Collectors.toList());

            var results = execute("SELECT pk FROM %s WHERE GEO_DISTANCE(val, ?) < " + distanceInMeters, vector(searchVector));

            // Get the collection of expected rows. This uses a more expensive and more correct haversine distance
            // formula, which is why we calculate the false-positive ratio as well.
            var expected = closeVectors.stream().map(v -> v.left).collect(Collectors.toSet());
            var actual = results.stream().map(r -> r.getInt("pk")).collect(Collectors.toSet());
            var incorrectMatches = actual.stream().filter(pk -> !expected.contains(pk)).count();
            var correctMatches = actual.size() - incorrectMatches;

            if (!expected.isEmpty())
                recallRate += correctMatches / (double) expected.size();
            else if (actual.isEmpty())
                // We expected none (recall was empty) and we got none, so that is 100% recall. If we got some,
                // recall is 0%, which is a no-op here.
                recallRate += 1;

            // If actual is empty, then we have no false positives, so this is a no-op.
            if (!actual.isEmpty())
                falsePositiveRate += incorrectMatches / (double) actual.size();
        }
        double observedRecall = recallRate / queryCount;
        double observedFalsePositiveAccuracy = falsePositiveRate / queryCount;
        logger.info("Observed recall rate: {}", observedRecall);
        logger.info("Observed false positive rate: {}", observedFalsePositiveAccuracy);
        assertTrue("Recall should be greater than " + MIN_EXPECTED_RECALL + " but found " + observedRecall,
                   observedRecall > MIN_EXPECTED_RECALL);
        assertTrue("False positive rate should be less than " + MAX_EXPECTED_FALSE_POSITIVE_RATE + " but found " + observedFalsePositiveAccuracy,
                   observedFalsePositiveAccuracy < MAX_EXPECTED_FALSE_POSITIVE_RATE);
    }

    public static boolean isWithinDistance(float[] vector, float[] searchVector, float distanceInMeters)
    {
        return strictHaversineDistance(vector[0], vector[1], searchVector[0], searchVector[1]) < distanceInMeters;
    }

    private float[] createRandomNYCVector()
    {
        // Approximate bounding box for contiguous NYC locations
        var lat = getRandom().nextFloatBetween(39, 41);
        var lon = getRandom().nextFloatBetween(-74, -72);
        return new float[] {lat, lon};
    }

    // In the production code, we use a haversine distance formula from lucene, which prioritizes speed over some
    // accuracy. This is the strict formula.
    private static double strictHaversineDistance(float lat1, float lon1, float lat2, float lon2)
    {
        // This implementation is based on information from https://www.movable-type.co.uk/scripts/latlong.html
        double phi1 = lat1 * Math.PI/180; // phi, lambda in radians
        double phi2 = lat2 * Math.PI/180;
        double deltaPhi = (lat2 - lat1) * Math.PI/180;
        double deltaLambda = (lon2 - lon1) * Math.PI/180;

        double a = Math.sin(deltaPhi / 2.0) * Math.sin(deltaPhi / 2.0) +
                   Math.cos(phi1) * Math.cos(phi2) *
                   Math.sin(deltaLambda / 2.0) * Math.sin(deltaLambda / 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));

        return GeoUtils.EARTH_MEAN_RADIUS_METERS * c; // in meters
    }
}
