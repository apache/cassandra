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

package org.apache.cassandra.tcm.listeners;

import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.guardrails.CustomGuardrailConfig;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Guardrails.CustomGuardrailsMapper;
import org.apache.cassandra.db.guardrails.Guardrails.ValuesGuardrailsMapper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.GuardrailsMetadata.ValuesHolder;
import org.apache.cassandra.utils.Pair;

public class GuardrailsListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsListener.class);

    private static final String NOT_FOUND_GUARDRAIL_MESSAGE_TEMPLATE = "Skipping guardrail {} for epoch {} with values {} as it does not exist on this node.";

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        diffFlags(prev, next);
        diffValues(prev, next);
        diffThresholds(prev, next);
        diffCustom(prev, next);
    }

    private void diffFlags(ClusterMetadata prev, ClusterMetadata next)
    {
        ImmutableMap<String, Boolean> flagsDiff = prev.guardrailsMetadata.diffFlags(next.guardrailsMetadata);

        for (Map.Entry<String, Boolean> entry : flagsDiff.entrySet())
        {
            try
            {
                String guardrailName = entry.getKey();
                Pair<Consumer<Boolean>, BooleanSupplier> setterAndGetter = Guardrails.getEnableFlagGuardrails().get(guardrailName);
                if (setterAndGetter == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(NOT_FOUND_GUARDRAIL_MESSAGE_TEMPLATE,
                                     guardrailName, next.epoch, entry.getValue().toString());

                    continue;
                }

                Consumer<Boolean> setter = setterAndGetter.left;
                setter.accept(entry.getValue());
            }
            catch (Exception ex)
            {
                logger.warn(ex.getMessage());
            }
        }
    }

    private void diffValues(ClusterMetadata prev, ClusterMetadata next)
    {

        ImmutableMap<String, ValuesHolder> valuesDiff = prev.guardrailsMetadata.diffValues(next.guardrailsMetadata);

        for (Map.Entry<String, ValuesHolder> entry : valuesDiff.entrySet())
        {
            try
            {
                String guardrailName = entry.getKey();
                ValuesGuardrailsMapper setterAndGetter = Guardrails.getValueGuardrails().get(guardrailName);

                if (setterAndGetter == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(NOT_FOUND_GUARDRAIL_MESSAGE_TEMPLATE,
                                     guardrailName, next.epoch, entry.getValue().toString());

                    continue;
                }

                ValuesHolder valuesHolder = entry.getValue();

                if (valuesHolder.ignored != null)
                    setterAndGetter.ignoredValuesConsumer.accept(valuesHolder.ignored);
                if (valuesHolder.warned != null)
                    setterAndGetter.warnedValuesConsumer.accept(valuesHolder.warned);
                if (valuesHolder.disallowed != null)
                    setterAndGetter.disallowedValuesConsumer.accept(valuesHolder.disallowed);
            }
            catch (Exception ex)
            {
                logger.warn(ex.getMessage());
            }
        }
    }

    private void diffThresholds(ClusterMetadata prev, ClusterMetadata next)
    {

        ImmutableMap<String, Pair<Long, Long>> thresholdsDiff = prev.guardrailsMetadata.diffThresholds(next.guardrailsMetadata);

        for (Map.Entry<String, Pair<Long, Long>> entry : thresholdsDiff.entrySet())
        {
            try
            {
                String guardrailName = entry.getKey();
                Guardrails.ThresholdsGuardrailsMapper mapper = Guardrails.getThresholdGuardails().get(guardrailName);

                if (mapper == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(NOT_FOUND_GUARDRAIL_MESSAGE_TEMPLATE,
                                     guardrailName, next.epoch, entry.getValue().toString());

                    continue;
                }

                Pair<Long, Long> newThresholds = entry.getValue();
                mapper.setter.accept(newThresholds.left, newThresholds.right);
            }
            catch (Exception ex)
            {
                logger.warn(ex.getMessage());
            }
        }
    }

    private void diffCustom(ClusterMetadata prev, ClusterMetadata next)
    {

        ImmutableMap<String, CustomGuardrailConfig> customDiff = prev.guardrailsMetadata.diffCustom(next.guardrailsMetadata);

        for (Map.Entry<String, CustomGuardrailConfig> entry : customDiff.entrySet())
        {
            try
            {
                String guardrailName = entry.getKey();
                CustomGuardrailsMapper customMapper = Guardrails.getCustomGuardrails().get(guardrailName);
                if (customMapper == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(NOT_FOUND_GUARDRAIL_MESSAGE_TEMPLATE,
                                     guardrailName, next.epoch, entry.getValue().toString());

                    continue;
                }

                customMapper.setter.accept(entry.getValue());
            }
            catch (Exception ex)
            {
                logger.warn(ex.getMessage());
            }
        }
    }
}
