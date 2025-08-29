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

import java.util.HashMap;
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;

/**
 * Represents the current state of compression dictionary training.
 * This class encapsulates training status, progress information, and failure details in a single snapshot.
 */
public class TrainingState
{
    public final TrainingStatus status;
    public final String failureMessage;  // null unless status is FAILED
    public final long sampleCount;
    public final long totalSampleSize;

    // JMX CompositeData support
    private static final String[] ITEM_NAMES = new String[]{ "status",
                                                             "failure_message",
                                                             "sample_count",
                                                             "total_sample_size" };

    private static final String[] ITEM_DESC = new String[]{ "current training status",
                                                            "failure message if training failed, null otherwise",
                                                            "number of samples collected",
                                                            "total size of samples collected in bytes" };

    private static final OpenType<?>[] ITEM_TYPES;

    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{ SimpleType.STRING,
                                         SimpleType.STRING,
                                         SimpleType.LONG,
                                         SimpleType.LONG };

            COMPOSITE_TYPE = new CompositeType(TrainingState.class.getName(),
                                               "TrainingState",
                                               ITEM_NAMES,
                                               ITEM_DESC,
                                               ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    private TrainingState(TrainingStatus status, String failureMessage, long sampleCount, long totalSampleSize)
    {
        this.status = status;
        this.failureMessage = failureMessage;
        this.sampleCount = sampleCount;
        this.totalSampleSize = totalSampleSize;
    }

    // Factory methods for clarity and type safety
    public static TrainingState notStarted()
    {
        return new TrainingState(TrainingStatus.NOT_STARTED, null, 0, 0);
    }

    public static TrainingState sampling(long samples, long totalSize)
    {
        return new TrainingState(TrainingStatus.SAMPLING, null, samples, totalSize);
    }

    public static TrainingState training(long samples, long totalSize)
    {
        return new TrainingState(TrainingStatus.TRAINING, null, samples, totalSize);
    }

    public static TrainingState completed(long samples, long totalSize)
    {
        return new TrainingState(TrainingStatus.COMPLETED, null, samples, totalSize);
    }

    public static TrainingState failed(String message, long samples, long totalSize)
    {
        return new TrainingState(TrainingStatus.FAILED, message, samples, totalSize);
    }

    public boolean isFailed()
    {
        return status == TrainingStatus.FAILED;
    }

    public boolean isCompleted()
    {
        return status == TrainingStatus.COMPLETED;
    }

    public TrainingStatus getStatus()
    {
        return status;
    }

    public long getSampleCount()
    {
        return sampleCount;
    }

    public long getTotalSampleSize()
    {
        return totalSampleSize;
    }

    // returns null unless status is FAILED
    public String getFailureMessage()
    {
        return isFailed() ? failureMessage : null;
    }

    // JMX CompositeData conversion methods

    /**
     * Converts this TrainingState to JMX CompositeData format.
     *
     * @return CompositeData representation of this training state
     */
    public CompositeData toCompositeData()
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], status.toString());
        valueMap.put(ITEM_NAMES[1], getFailureMessage());
        valueMap.put(ITEM_NAMES[2], sampleCount);
        valueMap.put(ITEM_NAMES[3], totalSampleSize);

        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (final OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts JMX CompositeData back to TrainingState.
     *
     * @param data the CompositeData to convert
     * @return TrainingState reconstructed from the CompositeData
     */
    public static TrainingState fromCompositeData(final CompositeData data)
    {
        assert data.getCompositeType().equals(COMPOSITE_TYPE);

        final Object[] values = data.getAll(ITEM_NAMES);

        TrainingStatus status = TrainingStatus.valueOf((String) values[0]);
        String failureMessage = (String) values[1];
        long sampleCount = (Long) values[2];
        long totalSampleSize = (Long) values[3];

        // Reconstruct TrainingState based on status
        switch (status)
        {
            case NOT_STARTED:
                return TrainingState.notStarted();
            case SAMPLING:
                return TrainingState.sampling(sampleCount, totalSampleSize);
            case TRAINING:
                return TrainingState.training(sampleCount, totalSampleSize);
            case COMPLETED:
                return TrainingState.completed(sampleCount, totalSampleSize);
            case FAILED:
                return TrainingState.failed(failureMessage, sampleCount, totalSampleSize);
            default:
                throw new IllegalStateException("Unknown training status: " + status);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TrainingState{status=");
        sb.append(status);
        sb.append(", samples=").append(sampleCount);
        sb.append(", totalSize=").append(totalSampleSize);
        if (isFailed() && failureMessage != null)
        {
            sb.append(", failure='").append(failureMessage).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }
}
