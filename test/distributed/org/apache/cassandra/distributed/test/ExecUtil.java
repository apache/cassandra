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

package org.apache.cassandra.distributed.test;

import java.io.Serializable;

import org.apache.cassandra.distributed.api.IIsolatedExecutor;

public class ExecUtil
{

    public interface ThrowingSerializableRunnable<T extends Throwable> extends Serializable
    {
        public void run() throws T;
    }

    public static <T extends Throwable> IIsolatedExecutor.SerializableRunnable rethrow(ThrowingSerializableRunnable<T> run)
    {
        return () -> {
            try
            {
                run.run();
            }
            catch (RuntimeException | Error t)
            {
                throw t;
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        };
    }

}
