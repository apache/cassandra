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

package org.apache.cassandra.metrics;

import java.util.function.Function;

import accord.local.durability.DurabilityService;
import accord.topology.TopologyManager;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;

public class AccordMetricUtils
{
    static <T> Gauge<T> fromAccordService(Function<IAccordService, T> ifSetup, T ifNotSetup)
    {
        return fromAccordService(ifSetup, Function.identity(), ifNotSetup);
    }

    static Gauge<Long> fromTopologyManager(Function<TopologyManager, Long> ifSetup)
    {
        return fromAccordService(service -> service.node().topology(), ifSetup, 0L);
    }

    static Gauge<Long> fromDurabilityService(Function<DurabilityService, Long> ifSetup)
    {
        return fromAccordService(service -> service.node().durability(), ifSetup, 0L);
    }

    static <I, T> Gauge<T> fromAccordService(Function<IAccordService, I> initialStep, Function<I, T> finalStep, T ifNotSetup)
    {
        if (AccordService.isSetup())
        {
            IAccordService service = AccordService.instance();
            if (!service.isEnabled())
                return () -> ifNotSetup;
            I intermediate = initialStep.apply(service);
            return () -> finalStep.apply(intermediate);
        }
        return () -> {
            IAccordService service = AccordService.instance();
            if (!service.isEnabled())
                return ifNotSetup;
            return finalStep.apply(initialStep.apply(service));
        };
    }
}
