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

package org.apache.cassandra.db.compression;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingManager.CompressionRatioEvaluator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the adoption decision in {@link CompressionDictionaryAutoTrainingManager#newDictionaryBetterThanLatest}:
 * the freshly-trained candidate is promoted only if it improves the compression ratio over the current
 * dictionary by at least the configured threshold, where {@code improvement = (latest - candidate) / latest}
 * (ratios are compressed/uncompressed, so lower is better).
 * <p>
 * Both dictionaries and the held-out evaluation are mocked (via the {@code createTrainer}/{@code createEvaluator}
 * seams), so this exercises purely the improvement math and threshold comparison - no sampling, training or
 * real compression happens.
 */
public class CompressionDictionaryAutoTrainingEvaluationTest
{
    private static final float THRESHOLD = 0.15f;

    @BeforeClass
    public static void setUpClass()
    {
        // needed so ColumnFamilyStore can be class-loaded for mocking; creates no keyspace/table
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void candidateThatDoesNotBeatLatestEnoughIsNotPromoted()
    {
        // latest=0.50, candidate=0.49 -> improvement = 0.02, below the 0.15 threshold
        assertThat(isPromoted(0.50, 0.49)).isFalse();
    }

    @Test
    public void candidateThatBeatsLatestByThresholdIsPromoted()
    {
        // latest=0.50, candidate=0.30 -> improvement = 0.40, at/above the 0.15 threshold
        assertThat(isPromoted(0.50, 0.30)).isTrue();
    }

    /**
     * Invokes the real {@code newDictionaryBetterThanLatest} with the held-out ratios for the current (latest)
     * and freshly-trained (candidate) dictionaries mocked to the given values.
     */
    private boolean isPromoted(double latestRatio, double candidateRatio)
    {
        ColumnFamilyStore cfs = mock(ColumnFamilyStore.class);
        when(cfs.getKeyspaceName()).thenReturn("ks");
        when(cfs.getTableName()).thenReturn("tbl");

        CompressionDictionary latest = mock(CompressionDictionary.class);
        CompressionDictionary candidate = mock(CompressionDictionary.class);

        ICompressionDictionaryTrainer trainer = mock(ICompressionDictionaryTrainer.class);
        ColumnFamilyStore.RefViewFragment refViewFragment = mock(ColumnFamilyStore.RefViewFragment.class);

        // the evaluation itself is mocked: it returns a fixed held-out ratio per dictionary
        CompressionRatioEvaluator evaluator = mock(CompressionRatioEvaluator.class);
        when(evaluator.evaluate(latest)).thenReturn(latestRatio);
        when(evaluator.evaluate(candidate)).thenReturn(candidateRatio);

        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                     .autoTrainingImprovementThreshold(THRESHOLD)
                                                     .build();

        CompressionDictionaryAutoTrainingManager autoTrainer = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            ICompressionDictionaryTrainer createTrainer(ColumnFamilyStore c)
            {
                return trainer;
            }

            @Override
            CompressionRatioEvaluator createEvaluator(ColumnFamilyStore c,
                                                      ColumnFamilyStore.RefViewFragment r,
                                                      ICompressionDictionaryTrainer t,
                                                      CompressionDictionaryTrainingConfig cfg)
            {
                return evaluator;
            }
        };

        return autoTrainer.newDictionaryBetterThanLatest(cfs, latest, candidate, refViewFragment, config);
    }
}
