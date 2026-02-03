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

package org.apache.cassandra.utils;

import java.util.Random;
import java.util.UUID;

import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryTrainingConfig;
import org.apache.cassandra.db.compression.ZstdDictionaryTrainer;
import org.apache.cassandra.db.marshal.UTF8Type;

/**
 * Simple generator of random data for Zstd compression dictionary and dictionary trainer.
 */
public class CompressionDictionaryHelper
{
    public static final CompressionDictionaryHelper INSTANCE = new CompressionDictionaryHelper();
    private static final Random random = new Random();

    private static final String[] dates = new String[] {"2025-10-20","2025-10-19","2025-10-18","2025-10-17","2025-10-16"};
    private static final String[] times = new String[] {"11:00:01","11:00:02","11:00:03","11:00:04","11:00:05"};
    private static final String[] levels = new String[] {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
    private static final String[] services = new String[] {"com.example.UserService", "com.example.DatabasePool", "com.example.PaymentService", "com.example.OrderService"};

    public String getRandomSample()
    {
        return dates[random.nextInt(dates.length)] + ' ' +
               times[random.nextInt(times.length)] + ' ' +
               levels[random.nextInt(levels.length)] + ' ' +
               services[random.nextInt(services.length)] + ' ' +
               UUID.randomUUID(); // message
    }

    public CompressionDictionary trainDictionary(String keyspace, String table)
    {
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig
                                                     .builder()
                                                     .maxDictionarySize(65536)
                                                     .maxTotalSampleSize(1024 * 1024) // 1MB total
                                                     .build();

        try (ZstdDictionaryTrainer trainer = new ZstdDictionaryTrainer(keyspace, table, 3))
        {
            trainer.start(config);
            for (int i = 0; i < 25000; i++)
            {
                trainer.addSample(UTF8Type.instance.fromString(CompressionDictionaryHelper.INSTANCE.getRandomSample()));
            }

            return trainer.trainDictionary(false);
        }
    }
}
