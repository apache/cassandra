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

package org.apache.cassandra.distributed.shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.Futures;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class ClusterUtils
{

    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, Consumer<WithProperties> fn)
    {
        return start(inst, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, BiConsumer<I, WithProperties> fn)
    {
        try (WithProperties properties = new WithProperties())
        {
            fn.accept(inst, properties);
            inst.startup();
            return inst;
        }
    }

    /**
     * Stop an instance in a blocking manner.
     *
     * The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     */
    public static void stopUnchecked(IInstance i)
    {
        Futures.getUnchecked(i.shutdown());
    }

    public static List<RingInstanceDetails> awaitRingStatus(IInstance instance, IInstance expectedInRing, String status)
    {
        return awaitInstanceMatching(instance, expectedInRing, d -> d.status.equals(status),
                                     "Timeout waiting for " + expectedInRing + " to have status " + status);
    }

    /**
     * Wait for the ring to have the target instance with the provided state.
     *
     * @param instance instance to check on
     * @param expectedInRing to look for
     * @param state expected
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingState(IInstance instance, IInstance expectedInRing, String state)
    {
        return awaitInstanceMatching(instance, expectedInRing, d -> d.state.equals(state),
                                     "Timeout waiting for " + expectedInRing + " to have state " + state);
    }

    private static List<RingInstanceDetails> awaitInstanceMatching(IInstance instance,
                                                                   IInstance expectedInRing,
                                                                   Predicate<RingInstanceDetails> predicate,
                                                                   String errorMessage)
    {
        return awaitRing(instance,
                         errorMessage,
                         ring -> ring.stream()
                                     .filter(d -> d.address.equals(getBroadcastAddressHostString(expectedInRing)))
                                     .anyMatch(predicate));
    }

    private static List<RingInstanceDetails> awaitRing(IInstance src, String errorMessage, Predicate<List<RingInstanceDetails>> fn)
    {
        List<RingInstanceDetails> ring = null;
        for (int i = 0; i < 100; i++)
        {
            ring = ring(src);
            if (fn.test(ring))
            {
                return ring;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new AssertionError(errorMessage + "\n" + ring);
    }

    /**
     * Get the broadcast address host address only (ex. 127.0.0.1)
     */
    private static String getBroadcastAddressHostString(IInstance target)
    {
        return target.config().broadcastAddress().getAddress().getHostAddress();
    }

    /**
     * Get the ring from the perspective of the instance.
     */
    public static List<RingInstanceDetails> ring(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }

    private static List<RingInstanceDetails> parseRing(String str)
    {
        // 127.0.0.3  rack0       Up     Normal  46.21 KB        100.00%             -1
        // /127.0.0.1:7012  Unknown     ?      Normal  ?               100.00%             -3074457345618258603
        Pattern pattern = Pattern.compile("^(/?[0-9.:]+)\\s+(\\w+|\\?)\\s+(\\w+|\\?)\\s+(\\w+|\\?).*?(-?\\d+)\\s*$");
        List<RingInstanceDetails> details = new ArrayList<>();
        String[] lines = str.split("\n");
        for (String line : lines)
        {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find())
            {
                continue;
            }
            details.add(new RingInstanceDetails(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5)));
        }

        return details;
    }

    public static final class RingInstanceDetails
    {
        private final String address;
        private final String rack;
        private final String status;
        private final String state;
        private final String token;

        private RingInstanceDetails(String address, String rack, String status, String state, String token)
        {
            this.address = address;
            this.rack = rack;
            this.status = status;
            this.state = state;
            this.token = token;
        }

        public String getAddress()
        {
            return address;
        }

        public String getRack()
        {
            return rack;
        }

        public String getStatus()
        {
            return status;
        }

        public String getState()
        {
            return state;
        }

        public String getToken()
        {
            return token;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RingInstanceDetails that = (RingInstanceDetails) o;
            return Objects.equals(address, that.address) &&
                   Objects.equals(rack, that.rack) &&
                   Objects.equals(status, that.status) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(token, that.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, rack, status, state, token);
        }

        public String toString()
        {
            return Arrays.asList(address, rack, status, state, token).toString();
        }
    }

}
