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

package org.apache.cassandra.locator;

import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

public class SnitchUtils
{
    private SnitchUtils() {}

    static Pair<String, String> parseDcAndRack(String response, String dcSuffix)
    {
        String[] splits = response.split("/");
        String az = splits[splits.length - 1];

        splits = az.split("-");
        String localRack = splits[splits.length - 1];

        int lastRegionIndex = az.lastIndexOf('-');

        // we would hit StringIndexOutOfBoundsException on the az.substring method if we did not do this
        if (lastRegionIndex == -1)
            throw new IllegalStateException(format("%s does not contain at least one '-' to differentiate " +
                                                   "between datacenter and rack", response));

        String localDc = az.substring(0, lastRegionIndex);

        localDc = localDc.concat(dcSuffix);

        return Pair.create(localDc, localRack);
    }
}
