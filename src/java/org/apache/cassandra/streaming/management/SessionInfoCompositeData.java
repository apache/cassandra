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
package org.apache.cassandra.streaming.management;

import java.net.UnknownHostException;
import java.util.*;
import javax.management.openmbean.*;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.TimeUUID;

public class SessionInfoCompositeData
{
    private static final String[] ITEM_NAMES = new String[]{"planId",
                                                            "peer",
                                                            "peer_port",
                                                            "connecting",
                                                            "connecting_port",
                                                            "receivingSummaries",
                                                            "sendingSummaries",
                                                            "state",
                                                            "receivingFiles",
                                                            "sendingFiles",
                                                            "sessionIndex"};
    private static final String[] ITEM_DESCS = new String[]{"Plan ID",
                                                            "Session peer",
                                                            "Session peer storage port",
                                                            "Connecting address",
                                                            "Connecting storage port",
                                                            "Summaries of receiving data",
                                                            "Summaries of sending data",
                                                            "Current session state",
                                                            "Receiving files",
                                                            "Sending files",
                                                            "Session index"};
    private static final OpenType<?>[] ITEM_TYPES;

    public static final CompositeType COMPOSITE_TYPE;
    static
    {
        try
        {
            ITEM_TYPES = new OpenType[]{SimpleType.STRING,
                                        SimpleType.STRING,
                                        SimpleType.INTEGER,
                                        SimpleType.STRING,
                                        SimpleType.INTEGER,
                                        ArrayType.getArrayType(StreamSummaryCompositeData.COMPOSITE_TYPE),
                                        ArrayType.getArrayType(StreamSummaryCompositeData.COMPOSITE_TYPE),
                                        SimpleType.STRING,
                                        ArrayType.getArrayType(ProgressInfoCompositeData.COMPOSITE_TYPE),
                                        ArrayType.getArrayType(ProgressInfoCompositeData.COMPOSITE_TYPE),
                                        SimpleType.INTEGER};
            COMPOSITE_TYPE = new CompositeType(SessionInfo.class.getName(),
                                               "SessionInfo",
                                               ITEM_NAMES,
                                               ITEM_DESCS,
                                               ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData toCompositeData(final TimeUUID planId, SessionInfo sessionInfo)
    {
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ITEM_NAMES[0], planId.toString());
        valueMap.put(ITEM_NAMES[1], sessionInfo.peer.getAddress().getHostAddress());
        valueMap.put(ITEM_NAMES[2], sessionInfo.peer.getPort());
        valueMap.put(ITEM_NAMES[3], sessionInfo.connecting.getAddress().getHostAddress());
        valueMap.put(ITEM_NAMES[4], sessionInfo.connecting.getPort());
        Function<StreamSummary, CompositeData> fromStreamSummary = new Function<StreamSummary, CompositeData>()
        {
            public CompositeData apply(StreamSummary input)
            {
                return StreamSummaryCompositeData.toCompositeData(input);
            }
        };
        valueMap.put(ITEM_NAMES[5], toArrayOfCompositeData(sessionInfo.receivingSummaries, fromStreamSummary));
        valueMap.put(ITEM_NAMES[6], toArrayOfCompositeData(sessionInfo.sendingSummaries, fromStreamSummary));
        valueMap.put(ITEM_NAMES[7], sessionInfo.state.name());
        Function<ProgressInfo, CompositeData> fromProgressInfo = new Function<ProgressInfo, CompositeData>()
        {
            public CompositeData apply(ProgressInfo input)
            {
                return ProgressInfoCompositeData.toCompositeData(planId, input);
            }
        };
        valueMap.put(ITEM_NAMES[8], toArrayOfCompositeData(sessionInfo.getReceivingFiles(), fromProgressInfo));
        valueMap.put(ITEM_NAMES[9], toArrayOfCompositeData(sessionInfo.getSendingFiles(), fromProgressInfo));
        valueMap.put(ITEM_NAMES[10], sessionInfo.sessionIndex);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, valueMap);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static SessionInfo fromCompositeData(CompositeData cd)
    {
        assert cd.getCompositeType().equals(COMPOSITE_TYPE);

        Object[] values = cd.getAll(ITEM_NAMES);
        InetAddressAndPort peer, connecting;
        try
        {
            peer = InetAddressAndPort.getByNameOverrideDefaults((String) values[1], (Integer)values[2]);
            connecting = InetAddressAndPort.getByNameOverrideDefaults((String) values[3], (Integer)values[4]);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        Function<CompositeData, StreamSummary> toStreamSummary = new Function<CompositeData, StreamSummary>()
        {
            public StreamSummary apply(CompositeData input)
            {
                return StreamSummaryCompositeData.fromCompositeData(input);
            }
        };
        SessionInfo info = new SessionInfo(peer,
                                           (int)values[10],
                                           connecting,
                                           fromArrayOfCompositeData((CompositeData[]) values[5], toStreamSummary),
                                           fromArrayOfCompositeData((CompositeData[]) values[6], toStreamSummary),
                                           StreamSession.State.valueOf((String) values[7]), null); // null is here to maintain backwards compatibility
        Function<CompositeData, ProgressInfo> toProgressInfo = new Function<CompositeData, ProgressInfo>()
        {
            public ProgressInfo apply(CompositeData input)
            {
                return ProgressInfoCompositeData.fromCompositeData(input);
            }
        };
        for (ProgressInfo progress : fromArrayOfCompositeData((CompositeData[]) values[8], toProgressInfo))
        {
            info.updateProgress(progress);
        }
        for (ProgressInfo progress : fromArrayOfCompositeData((CompositeData[]) values[9], toProgressInfo))
        {
            info.updateProgress(progress);
        }
        return info;
    }

    private static <T> Collection<T> fromArrayOfCompositeData(CompositeData[] cds, Function<CompositeData, T> func)
    {
        return Lists.newArrayList(Iterables.transform(Arrays.asList(cds), func));
    }

    private static <T> CompositeData[] toArrayOfCompositeData(Collection<T> toConvert, Function<T, CompositeData> func)
    {
        CompositeData[] composites = new CompositeData[toConvert.size()];
        return Lists.newArrayList(Iterables.transform(toConvert, func)).toArray(composites);
    }
}
