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
package org.apache.cassandra.serializers;

import java.nio.charset.StandardCharsets;

import org.apache.cassandra.db.marshal.ValueAccessor;

public class UTF8Serializer extends AbstractTextSerializer
{
    public static final UTF8Serializer instance = new UTF8Serializer();

    private UTF8Serializer()
    {
        super(StandardCharsets.UTF_8);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        if (!UTF8Validator.validate(value, accessor))
            throw new MarshalException("String didn't validate.");
    }

    static class UTF8Validator
    {
        enum State
        {
            START,
            TWO,
            TWO_80,
            THREE_a0bf,
            THREE_80bf_1,
            THREE_80bf_2,
            FOUR_90bf,
            FOUR_80bf_3,
        };

        // since we're not converting to java strings, we don't need to worry about converting to surrogates.
        // buf has already been sliced/duplicated.
        static <V> boolean validate(V value, ValueAccessor<V> accessor)
        {
            if (value == null)
                return false;

            int b = 0;
            int offset = 0;
            State state = State.START;
            while (!accessor.isEmptyFromOffset(value, offset))
            {
                b = accessor.getByte(value, offset++);
                switch (state)
                {
                    case START:
                        if (b >= 0)
                        {
                            // ascii, state stays start.
                            if (b > 127)
                                return false;
                        }
                        else if ((b >> 5) == -2)
                        {
                            // validate first byte of 2-byte char, 0xc2-0xdf
                            if (b == (byte) 0xc0)
                                // special case: modified utf8 null is 0xc080.
                                state = State.TWO_80;
                            else if ((b & 0x1e) == 0)
                                return false;
                            else
                                state = State.TWO;
                        }
                        else if ((b >> 4) == -2)
                        {
                            // 3 bytes. first byte will be 0xe0 or 0xe1-0xef. handling of second byte will differ.
                            // so 0xe0,0xa0-0xbf,0x80-0xbf or 0xe1-0xef,0x80-0xbf,0x80-0xbf.
                            if (b == (byte)0xe0)
                                state = State.THREE_a0bf;
                            else
                                state = State.THREE_80bf_2;
                            break;
                        }
                        else if ((b >> 3) == -2)
                        {
                            // 4 bytes. this is where the fun starts.
                            if (b == (byte)0xf0)
                                // 0xf0, 0x90-0xbf, 0x80-0xbf, 0x80-0xbf
                                state = State.FOUR_90bf;
                            else
                                // 0xf4, 0x80-0xbf, 0x80-0xbf, 0x80-0xbf
                                // 0xf1-0xf3, 0x80-0xbf, 0x80-0xbf, 0x80-0xbf
                                state = State.FOUR_80bf_3;
                            break;
                        }
                        else
                            return false; // malformed.
                        break;
                    case TWO:
                        // validate second byte of 2-byte char, 0x80-0xbf
                        if ((b & 0xc0) != 0x80)
                            return false;
                        state = State.START;
                        break;
                    case TWO_80:
                        if (b != (byte)0x80)
                            return false;
                        state = State.START;
                        break;
                    case THREE_a0bf:
                        if ((b & 0xe0) == 0x80)
                            return false;
                        state = State.THREE_80bf_1;
                        break;
                    case THREE_80bf_1:
                        // expecting 0x80-0xbf
                        if ((b & 0xc0) != 0x80)
                            return false;
                        state = State.START;
                        break;
                    case THREE_80bf_2:
                        // expecting 0x80-bf and then another of the same.
                        if ((b & 0xc0) != 0x80)
                            return false;
                        state = State.THREE_80bf_1;
                        break;
                    case FOUR_90bf:
                        // expecting 0x90-bf. 2nd byte of 4byte sequence. after that it should degrade to 80-bf,80-bf (like 3byte seq).
                        if ((b & 0x30) == 0)
                            return false;
                        state = State.THREE_80bf_2;
                        break;
                    case FOUR_80bf_3:
                        // expecting 0x80-bf 3 times. degenerates to THREE_80bf_2.
                        if ((b & 0xc0) != 0x80)
                            return false;
                        state = State.THREE_80bf_2;
                        break;
                    default:
                        return false; // invalid state.
                }
            }
            // if state != start, we've got underflow. that's an error.
            return state == State.START;
        }
    }
}
