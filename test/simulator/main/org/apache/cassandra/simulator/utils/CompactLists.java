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

package org.apache.cassandra.simulator.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IIsolatedExecutor.TriConsumer;
import org.apache.cassandra.utils.Throwables;

public class CompactLists
{
    public static <I> List<I> append(List<I> in, I append)
    {
        if (in == null) return Collections.singletonList(append);
        else  if (in.size() == 1)
        {
            List<I> out = new ArrayList<>(2);
            out.add(in.get(0));
            out.add(append);
            return out;
        }
        else
        {
            in.add(append);
            return in;
        }
    }

    public static <I> List<I> remove(List<I> in, I remove)
    {
        if (in == null) return null;
        else if (in.size() == 1) return in.contains(remove) ? null : in;
        else
        {
            in.remove(remove);
            return in;
        }
    }

    public static <I> Throwable safeForEach(List<I> list, Consumer<I> forEach)
    {
        if (list == null)
            return null;

        if (list.size() == 1)
        {
            try
            {
                forEach.accept(list.get(0));
                return null;
            }
            catch (Throwable t)
            {
                return t;
            }
        }

        Throwable result = null;
        for (int i = 0, maxi = list.size() ; i < maxi ; ++i)
        {
            try
            {
                forEach.accept(list.get(i));
            }
            catch (Throwable t)
            {
                result = Throwables.merge(result, t);
            }
        }
        return result;
    }

    public static <I1, I2> Throwable safeForEach(List<I1> list, BiConsumer<I1, I2> forEach, I2 i2)
    {
        if (list == null)
            return null;

        if (list.size() == 1)
        {
            try
            {
                forEach.accept(list.get(0), i2);
                return null;
            }
            catch (Throwable t)
            {
                return t;
            }
        }

        Throwable result = null;
        for (int i = 0, maxi = list.size() ; i < maxi ; ++i)
        {
            try
            {
                forEach.accept(list.get(i), i2);
            }
            catch (Throwable t)
            {
                result = Throwables.merge(result, t);
            }
        }
        return result;
    }

    public static <I1, I2, I3> Throwable safeForEach(List<I1> list, TriConsumer<I1, I2, I3> forEach, I2 i2, I3 i3)
    {
        if (list == null)
            return null;

        if (list.size() == 1)
        {
            try
            {
                forEach.accept(list.get(0), i2, i3);
                return null;
            }
            catch (Throwable t)
            {
                return t;
            }
        }

        Throwable result = null;
        for (int i = 0, maxi = list.size() ; i < maxi ; ++i)
        {
            try
            {
                forEach.accept(list.get(i), i2, i3);
            }
            catch (Throwable t)
            {
                result = Throwables.merge(result, t);
            }
        }
        return result;
    }


}
