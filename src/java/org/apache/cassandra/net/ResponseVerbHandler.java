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
package org.apache.cassandra.net;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.paxos.v1.AbstractPaxosCallback;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

public class ResponseVerbHandler implements IVerbHandler
{
    public static final ResponseVerbHandler instance = new ResponseVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(ResponseVerbHandler.class);

    @Override
    public void doVerb(Message message)
    {
        RequestCallbacks.CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id(), message.from());
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id(), message.from());
            Tracing.trace(msg, message.id(), message.from());
            return;
        }

        // Use the precise clock: createdAtNanos may come from a precise-clock RequestTime (see Message.outWithFlags),
        // and approxTime can lag behind it, which would yield a negative latency.
        long latencyNanos = preciseTime.now() - callbackInfo.createdAtNanos;
        Tracing.trace("Processing response from {}", message.from());

        RequestCallback<?> cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            cb.onFailure(message.from(), (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latencySubscribers.maybeAdd(cb, message.verb(), message.from(), latencyNanos, NANOSECONDS, false);
            trackReplicaSensors(callbackInfo, message);
            cb.onResponse(message);
        }
    }

    /**
     * Accumulates sensors from a replica internode response into the coordinator's {@link RequestSensors}.
     *
     * <p><em>Bytes</em> sensors ({@link Type#WRITE_BYTES}, {@link Type#READ_BYTES},
     * {@link Type#INDEX_WRITE_BYTES}, {@link Type#INTERNODE_BYTES}) are accumulated by addition
     * because replicas execute sequentially from the coordinator's perspective and their byte
     * contributions are independent.</p>
     *
     * <p><em>Execution-time</em> sensors ({@link Type#READ_EXECUTION_TIME},
     * {@link Type#WRITE_EXECUTION_TIME}) are accumulated by {@code max} because replicas execute
     * in parallel — only the slowest replica's time is relevant.</p>
     *
     * <p>Please note {@link RequestSensors#syncAllSensors()} is not invoked here, but at the CQL statement layer:
     * this is to reduce number of calls, and because local-only requests would not go through this handler.</p>
     */
    private void trackReplicaSensors(RequestCallbacks.CallbackInfo callbackInfo, Message<?> message)
    {
        try
        {
            RequestSensors sensors = callbackInfo.callback.getRequestSensors();
            if (sensors == null)
                return;

            if (callbackInfo instanceof RequestCallbacks.WriteCallbackInfo)
            {
                RequestCallbacks.WriteCallbackInfo writerInfo = (RequestCallbacks.WriteCallbackInfo) callbackInfo;
                IMutation mutation = writerInfo.iMutation();
                if (mutation == null)
                    return;

                Collection<PartitionUpdate> nonIndexUpdates = mutation.getPartitionUpdates().stream()
                                                                      .filter(pu -> !pu.metadata().isIndex())
                                                                      .collect(Collectors.toList());
                int allTablesCount = mutation.getPartitionUpdates().size();
                double internodeBytesPerTable = allTablesCount == 0 ? 0
                                                                    : (double) writerInfo.sentPayloadSize / allTablesCount;
                for (PartitionUpdate pu : nonIndexUpdates)
                {
                    Context context = Context.from(pu.metadata());
                    incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                    incrementSensor(sensors, context, Type.INDEX_WRITE_BYTES, message);
                    sensors.incrementSensor(context, Type.INTERNODE_BYTES, internodeBytesPerTable);
                    accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
                }
            }
            else if (callbackInfo.callback instanceof ReadRequestCallback)
            {
                ReadRequestCallback<?> readCallback = (ReadRequestCallback<?>) callbackInfo.callback;
                Context context = readCallback.sensorsContext();
                incrementSensor(sensors, context, Type.READ_BYTES, message);
                incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.READ_EXECUTION_TIME, message);
            }
            // Covers Paxos V1 Prepare and Propose callbacks. Paxos V1 Commit callback is a regular WriteCallbackInfo
            // INDEX_WRITE_BYTES is not tracked here: prepare/propose only write to system.paxos, which has no indexes.
            // READ_EXECUTION_TIME tracks the system.paxos read time (loadPaxosState) measured in the verb handler.
            else if (callbackInfo.callback instanceof AbstractPaxosCallback)
            {
                AbstractPaxosCallback<?> paxosCallback = (AbstractPaxosCallback<?>) callbackInfo.callback;
                Context context = Context.from(paxosCallback.getMetadata());
                incrementSensor(sensors, context, Type.READ_BYTES, message);
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.READ_EXECUTION_TIME, message);
            }
            // Covers Paxos V2 Prepare, Propose and Commit callbacks.
            // INDEX_WRITE_BYTES is tracked in case PaxosCommitAndPrepare executes a commit as part of a prepare
            // INTERNODE_BYTES is tracked (same as V1 and regular reads/writes) to account for the internode cost of each round-trip.
            // READ_EXECUTION_TIME tracks: for Prepare, both the system.paxos read (loadPaxosState) and the user-table
            // precondition read (request.read.executeLocally()) when present; for Propose, the system.paxos read only.
            else if (callbackInfo.callback instanceof org.apache.cassandra.service.paxos.PaxosPrepare)
            {
                org.apache.cassandra.service.paxos.PaxosPrepare paxosCallback = (org.apache.cassandra.service.paxos.PaxosPrepare) callbackInfo.callback;
                Context context = Context.from(paxosCallback.getTableMetadata());
                incrementSensor(sensors, context, Type.READ_BYTES, message);
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INDEX_WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.READ_EXECUTION_TIME, message);
            }
            else if (callbackInfo.callback instanceof org.apache.cassandra.service.paxos.PaxosPropose)
            {
                org.apache.cassandra.service.paxos.PaxosPropose<?> paxosCallback = (org.apache.cassandra.service.paxos.PaxosPropose<?>) callbackInfo.callback;
                Context context = Context.from(paxosCallback.getTableMetadata());
                incrementSensor(sensors, context, Type.READ_BYTES, message);
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.READ_EXECUTION_TIME, message);
            }
            // Commit writes to the user table and system.paxos but never reads system.paxos, so READ_BYTES is not tracked.
            // INDEX_WRITE_BYTES covers secondary index writes on the user-table mutation.
            else if (callbackInfo.callback instanceof org.apache.cassandra.service.paxos.PaxosCommit)
            {
                org.apache.cassandra.service.paxos.PaxosCommit<?> paxosCallback = (org.apache.cassandra.service.paxos.PaxosCommit<?>) callbackInfo.callback;
                Context context = Context.from(paxosCallback.getTableMetadata());
                incrementSensor(sensors, context, Type.WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INDEX_WRITE_BYTES, message);
                incrementSensor(sensors, context, Type.INTERNODE_BYTES, message);
                accumulateExecutionTimeSensor(callbackInfo.callback, sensors, context, Type.WRITE_EXECUTION_TIME, message);
            }
            else
                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, MINUTES, "Ignored response callback {} for message {}, no sensors will be tracked.", callbackInfo.callback, message);
        }
        catch (Throwable e)
        {
            // Do not let sensor tracking break response handling:
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, MINUTES, "Error for response callback {} and message {}, no sensors will be tracked.", callbackInfo.callback, message, e);
        }
    }

    /**
     * Increments the sensor for the given context and type by adding the value encoded in the replica response message.
     */
    private void incrementSensor(RequestSensors sensors, Context context, Type type, Message<?> message)
    {
        Optional<Sensor> sensor = sensors.getSensor(context, type);
        if (sensor.isEmpty())
            return;

        Optional<String> customParam = SensorsCustomParams.paramForRequestSensor(sensor.get());
        if (customParam.isEmpty())
            return;

        double sensorValue = SensorsCustomParams.sensorValueFromInternodeResponse(message, customParam.get());
        sensors.incrementSensor(context, type, sensorValue);
    }

    /**
     * Reads the execution-time value from the replica response message and feeds it into the
     * callback's per-context running max. The response count is incremented separately inside
     * the callback's {@code onResponse} implementation.
     */
    private void accumulateExecutionTimeSensor(RequestCallback callback, RequestSensors sensors, Context context, Type executionType, Message<?> message)
    {
        Optional<Sensor> sensor = sensors.getSensor(context, executionType);
        if (sensor.isEmpty())
            return;

        Optional<String> customParam = SensorsCustomParams.paramForRequestSensor(sensor.get());
        if (customParam.isEmpty())
            return;

        double sensorValue = SensorsCustomParams.sensorValueFromInternodeResponse(message, customParam.get());
        callback.accumulateExecutionTimeSensor(context, executionType, sensorValue);
    }
}
