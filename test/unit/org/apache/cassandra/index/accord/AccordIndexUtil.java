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

package org.apache.cassandra.index.accord;

import javax.annotation.Nullable;

import accord.local.MaxDecidedRX;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;

public class AccordIndexUtil
{
    public static String normalize(TokenRange range)
    {
        return "R:" + range.toString().replace(range.prefix()  + ":", "");
    }

    public static String normalize(TokenKey key)
    {
        return "K:" + key.toString().replace(key.prefix()  + ":", "");
    }

    public static String normalize(@Nullable TxnId txnId)
    {
        return "T:" + (txnId == null ? "null" : Long.toString(txnId.hlc()));
    }

    public static String normalize(@Nullable MaxDecidedRX.DecidedRX decidedRX)
    {
        return "T:" + (decidedRX == null ? "null" : Long.toString(decidedRX.any.hlc()));
    }
}
