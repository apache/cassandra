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
package org.apache.cassandra.index.sasi.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.serializers.MarshalException;

public class TypeUtil
{
    public static boolean isValid(ByteBuffer term, AbstractType<?> validator)
    {
        try
        {
            validator.validate(term);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    public static ByteBuffer tryUpcast(ByteBuffer term, AbstractType<?> validator)
    {
        if (term.remaining() == 0)
            return null;

        try
        {
            if (validator instanceof Int32Type && term.remaining() == 2)
            {
                return Int32Type.instance.decompose((int) term.getShort(term.position()));
            }
            else if (validator instanceof LongType)
            {
                long upcastToken;

                switch (term.remaining())
                {
                    case 2:
                        upcastToken = (long) term.getShort(term.position());
                        break;

                    case 4:
                        upcastToken = (long) Int32Type.instance.compose(term);
                        break;

                    default:
                        upcastToken = Long.valueOf(UTF8Type.instance.getString(term));
                }

                return LongType.instance.decompose(upcastToken);
            }
            else if (validator instanceof DoubleType && term.remaining() == 4)
            {
                return DoubleType.instance.decompose((double) FloatType.instance.compose(term));
            }

            // maybe it was a string after all
            return validator.fromString(UTF8Type.instance.getString(term));
        }
        catch (Exception e)
        {
            return null;
        }
    }
}
