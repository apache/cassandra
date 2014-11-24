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
package org.apache.cassandra.repair;

import java.util.LinkedList;
import java.util.Queue;

public class SequentialRequestCoordinator<R> implements IRequestCoordinator<R>
{
    private final Queue<R> requests = new LinkedList<>();
    private final IRequestProcessor<R> processor;

    public SequentialRequestCoordinator(IRequestProcessor<R> processor)
    {
        this.processor = processor;
    }

    @Override
    public void add(R request)
    {
        requests.add(request);
    }

    @Override
    public void start()
    {
        if (requests.isEmpty())
            return;

        processor.process(requests.peek());
    }

    @Override
    public int completed(R request)
    {
        assert request.equals(requests.peek());
        requests.poll();
        int remaining = requests.size();
        if (remaining != 0)
            processor.process(requests.peek());
        return remaining;
    }
}
