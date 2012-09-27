/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.service;

import org.junit.Test;
import static org.junit.Assert.*;

public class PBSPredictorTest
{
    private static PBSPredictor predictor = PBSPredictor.instance();

    private void createWriteResponse(long W, long A, String id)
    {
        predictor.startWriteOperation(id, 0);
        predictor.logWriteResponse(id, W, W+A);
    }

    private void createReadResponse(long R, long S, String id)
    {
        predictor.startReadOperation(id, 0);
        predictor.logReadResponse(id, R, R+S);
    }

    @Test
    public void testDoPrediction()
    {
        try {
            predictor.enableConsistencyPredictionLogging();
            predictor.init();

            /*
                Ensure accuracy given a set of basic latencies
                Predictions here match a prior Python implementation
             */

            for (int i = 0; i < 10; ++i)
            {
                createWriteResponse(10, 0, String.format("W%d", i));
                createReadResponse(0, 0, String.format("R%d", i));
            }

            for (int i = 0; i < 10; ++i)
            {
                createWriteResponse(0, 0, String.format("WS%d", i));
            }

            // 10ms after write
            PBSPredictionResult result = predictor.doPrediction(2,1,1,10.0f,1, 0.99f);

            assertEquals(1, result.getConsistencyProbability(), 0);
            assertEquals(2.5, result.getAverageWriteLatency(), .5);

            // 0ms after write
            result = predictor.doPrediction(2,1,1,0f,1, 0.99f);

            assertEquals(.75, result.getConsistencyProbability(), 0.05);

            // k=5 versions staleness
            result = predictor.doPrediction(2,1,1,5.0f,5, 0.99f);
            assertEquals(.98, result.getConsistencyProbability(), 0.05);
            assertEquals(2.5, result.getAverageWriteLatency(), .5);

            for (int i = 0; i < 10; ++i)
            {
                createWriteResponse(20, 0, String.format("WL%d", i));
            }

            // 5ms after write
            result = predictor.doPrediction(2,1,1,5.0f,1, 0.99f);

            assertEquals(.67, result.getConsistencyProbability(), .05);

            // N = 5
            result = predictor.doPrediction(5,1,1,5.0f,1, 0.99f);

            assertEquals(.42, result.getConsistencyProbability(), .05);
            assertEquals(1.33, result.getAverageWriteLatency(), .5);

            for (int i = 0; i < 10; ++i)
            {
                createWriteResponse(100, 100, String.format("WVL%d", i));
                createReadResponse(100, 100, String.format("RL%d", i));
            }

            result = predictor.doPrediction(2,1,1,0f,1, 0.99f);

            assertEquals(.860, result.getConsistencyProbability(), .05);
            assertEquals(26.5, result.getAverageWriteLatency(), 1);
            assertEquals(100.33, result.getAverageReadLatency(), 4);

            result = predictor.doPrediction(2,2,1,0f,1, 0.99f);

            assertEquals(1, result.getConsistencyProbability(), 0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
