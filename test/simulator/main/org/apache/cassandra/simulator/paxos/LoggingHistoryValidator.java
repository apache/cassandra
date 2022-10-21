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

package org.apache.cassandra.simulator.paxos;

import java.util.Arrays;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingHistoryValidator implements HistoryValidator
{
    private static final Logger logger = LoggerFactory.getLogger(LoggingHistoryValidator.class);
    private final HistoryValidator delegate;

    public LoggingHistoryValidator(HistoryValidator delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public Checker witness(int start, int end)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Witness(start=").append(start).append(", end=").append(end).append(")\n");
        Checker sub = delegate.witness(start, end);
        return new Checker()
        {
            @Override
            public void read(int pk, int id, int count, int[] seq)
            {
                sb.append("\tread(pk=").append(pk).append(", id=").append(id).append(", count=").append(count).append(", seq=").append(Arrays.toString(seq)).append(")\n");
                sub.read(pk, id, count, seq);
            }

            @Override
            public void write(int pk, int id, boolean success)
            {
                sb.append("\twrite(pk=").append(pk).append(", id=").append(id).append(", success=").append(success).append(")\n");
                sub.write(pk, id, success);
            }

            @Override
            public void close()
            {
                logger.info(sb.toString());
                sub.close();
            }
        };
    }

    @Override
    public void print(@Nullable Integer pk)
    {
        delegate.print(pk);
    }
}
