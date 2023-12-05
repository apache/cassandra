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

package org.apache.cassandra.index.sai.utils;

public class GeoUtil
{
    // The distance in meters between two lines of longitude at the equator. We round down slightly to be more conservative
    // and therefore include more results.
    private static final double DISTANCE_PER_DEGREE_LONGITUDE_AT_EQUATOR = 110_000;

    /**
     * Determines the worst ratio for meters to degrees for a given latitude. The worst ratio will be the distance in
     * meters of 1 degree longitude.
     * @param vector the search vector
     * @return
     */
    private static double metersToDegreesRatioForLatitude(float[] vector)
    {
        // Got this formula from https://sciencing.com/what-parallels-maps-4689046.html. It seems
        // to produce accurate results, but it'd be good to find additional support for its correctness.
        return Math.cos(Math.toRadians(vector[0])) * DISTANCE_PER_DEGREE_LONGITUDE_AT_EQUATOR;
    }

    /**
     * Calculate the maximum bound for a squared distance between lat/long points on the earth. The result is
     * increased proportionally to the latitude of the search vector because the distance between two lines of
     * longitude decreases as you move away from the equator.
     * @param vector search vector
     * @param distanceInMeters the search radius
     * @return the threshold to use for the given geo point and distance
     */
    public static float amplifiedEuclideanSimilarityThreshold(float[] vector, float distanceInMeters)
    {
        // Get the conversion ratio for meters to degrees at the given latitude.
        double distanceBetweenDegreeLatitude = metersToDegreesRatioForLatitude(vector);

        // Calculate the number of degrees that the search radius represents because we're finding the distance between
        // two points that are also using degrees as their units.
        double degrees = distanceInMeters / distanceBetweenDegreeLatitude;

        return (float) (1.0 / (1 + Math.pow((float) degrees, 2)));
    }
}
