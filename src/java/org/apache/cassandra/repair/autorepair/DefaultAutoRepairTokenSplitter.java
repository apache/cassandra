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
package org.apache.cassandra.repair.autorepair;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.service.AutoRepairService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Pair;

public class DefaultAutoRepairTokenSplitter implements IAutoRepairTokenRangeSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultAutoRepairTokenSplitter.class);


    @Override
    public List<Pair<Token, Token>> getRange(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, String keyspaceName, String tableName)
    {
        List<Pair<Token, Token>> range = new ArrayList<>();

        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(keyspaceName);
        if (!primaryRangeOnly)
        {
            // if we need to repair non-primary token ranges, then change the tokens accrodingly
            tokens = StorageService.instance.getLocalReplicas(keyspaceName).ranges();
        }
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        int numberOfSubranges = config.getRepairSubRangeNum(repairType);
        for (Range<Token> token : tokens)
        {
            Murmur3Partitioner.LongToken l = (Murmur3Partitioner.LongToken) (token.left);
            Murmur3Partitioner.LongToken r = (Murmur3Partitioner.LongToken) (token.right);
            //        Token.TokenFactory factory = ClusterMetadata.current().partitioner.getTokenFactory();

            Token parentStartToken = ClusterMetadata.current().partitioner.getTokenFactory().fromString("" + l.getTokenValue());
            Token parentEndToken = ClusterMetadata.current().partitioner.getTokenFactory().fromString("" + r.getTokenValue());
            logger.debug("Parent Token Left side {}, right side {}", parentStartToken.toString(),
                         parentEndToken.toString());

            long left = (Long) l.getTokenValue();
            long right = (Long) r.getTokenValue();
            long repairTokenWidth = (right - left) / numberOfSubranges;
            for (int i = 0; i < numberOfSubranges; i++)
            {
                long curLeft = left + (i * repairTokenWidth);
                long curRight = curLeft + repairTokenWidth;

                if ((i + 1) == numberOfSubranges)
                {
                    curRight = right;
                }

                Token childStartToken = ClusterMetadata.current().partitioner.getTokenFactory().fromString("" + curLeft);
                Token childEndToken = ClusterMetadata.current().partitioner.getTokenFactory().fromString("" + curRight);
                logger.debug("Current Token Left side {}, right side {}", childStartToken
                                                                          .toString(), childEndToken.toString());
                range.add(Pair.create(childStartToken, childEndToken));
            }
        }
        return range;
    }
}