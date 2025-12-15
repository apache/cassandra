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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
public class UTF8ValidatorTest
{
    @Test
    public void testValidStrings()
    {
        assertValidUtf8String("");
        assertValidUtf8String("ASCII text");
        assertValidUtf8String("\n\r");
        assertValidUtf8String("a hierarchy of number systems: ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ");
        assertValidUtf8String("ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ");
        assertValidUtf8String("⡍⠜⠇⠑⠹ ⠺⠁⠎ ⠙⠑⠁⠙⠒ ⠞⠕ ⠃⠑⠛⠔ ⠺⠊⠹⠲ ⡹⠻⠑ ⠊⠎ ⠝⠕ ⠙⠳⠃⠞\n" +
                              "  ⠱⠁⠞⠑⠧⠻ ⠁⠃⠳⠞ ⠹⠁⠞⠲ ⡹⠑ ⠗⠑⠛⠊⠌⠻ ⠕⠋ ⠙⠊⠎ ⠃⠥⠗⠊⠁⠇ ⠺⠁⠎\n" +
                              "  ⠎⠊⠛⠝⠫ ⠃⠹ ⠹⠑ ⠊⠇⠻⠛⠹⠍⠁⠝⠂ ⠹⠑ ⠊⠇⠻⠅⠂ ⠹⠑ ⠥⠝⠙⠻⠞⠁⠅⠻⠂\n" +
                              "  ⠁⠝⠙ ⠹⠑ ⠡⠊⠑⠋ ⠍⠳⠗⠝⠻⠲ ⡎⠊⠗⠕⠕⠛⠑ ⠎⠊⠛⠝⠫ ⠊⠞⠲ ⡁⠝⠙\n" +
                              "  ⡎⠊⠗⠕⠕⠛⠑⠰⠎ ⠝⠁⠍⠑ ⠺⠁⠎ ⠛⠕⠕⠙ ⠥⠏⠕⠝ ⠰⡡⠁⠝⠛⠑⠂ ⠋⠕⠗ ⠁⠝⠹⠹⠔⠛ ⠙⠑ \n" +
                              "  ⠡⠕⠎⠑ ⠞⠕ ⠏⠥⠞ ⠙⠊⠎ ⠙⠁⠝⠙ ⠞⠕⠲");
        assertValidUtf8String("Excerpt from a poetry on The Romance of The Three Kingdoms (a Chinese\n" +
                              "  classic 'San Gua'):\n" +
                              "\n" +
                              "  [----------------------------|------------------------]\n" +
                              "    ๏ แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช  พระปกเกศกองบู๊กู้ขึ้นใหม่\n" +
                              "  สิบสองกษัตริย์ก่อนหน้าแลถัดไป       สององค์ไซร้โง่เขลาเบาปัญญา\n" +
                              "    ทรงนับถือขันทีเป็นที่พึ่ง           บ้านเมืองจึงวิปริตเป็นนักหนา\n" +
                              "  โฮจิ๋นเรียกทัพทั่วหัวเมืองมา         หมายจะฆ่ามดชั่วตัวสำคัญ\n" +
                              "    เหมือนขับไสไล่เสือจากเคหา      รับหมาป่าเข้ามาเลยอาสัญ\n" +
                              "  ฝ่ายอ้องอุ้นยุแยกให้แตกกัน          ใช้สาวนั้นเป็นชนวนชื่นชวนใจ\n" +
                              "    พลันลิฉุยกุยกีกลับก่อเหตุ          ช่างอาเพศจริงหนาฟ้าร้องไห้\n" +
                              "  ต้องรบราฆ่าฟันจนบรรลัย           ฤๅหาใครค้ำชูกู้บรรลังก์ ฯ");

        assertValidUtf8String("ᚻᛖ ᚳᚹᚫᚦ ᚦᚫᛏ ᚻᛖ ᛒᚢᛞᛖ ᚩᚾ ᚦᚫᛗ ᛚᚪᚾᛞᛖ ᚾᚩᚱᚦᚹᛖᚪᚱᛞᚢᛗ ᚹᛁᚦ ᚦᚪ ᚹᛖᛥᚫ");
        assertValidUtf8String("Box drawing alignment tests:                                          █\n" +
                              "                                                                      ▉\n" +
                              "  ╔══╦══╗  ┌──┬──┐  ╭──┬──╮  ╭──┬──╮  ┏━━┳━━┓  ┎┒┏┑   ╷  ╻ ┏┯┓ ┌┰┐    ▊ ╱╲╱╲╳╳╳\n" +
                              "  ║┌─╨─┐║  │╔═╧═╗│  │╒═╪═╕│  │╓─╁─╖│  ┃┌─╂─┐┃  ┗╃╄┙  ╶┼╴╺╋╸┠┼┨ ┝╋┥    ▋ ╲╱╲╱╳╳╳\n" +
                              "  ║│╲ ╱│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╿ │┃  ┍╅╆┓   ╵  ╹ ┗┷┛ └┸┘    ▌ ╱╲╱╲╳╳╳\n" +
                              "  ╠╡ ╳ ╞╣  ├╢   ╟┤  ├┼─┼─┼┤  ├╫─╂─╫┤  ┣┿╾┼╼┿┫  ┕┛┖┚     ┌┄┄┐ ╎ ┏┅┅┓ ┋ ▍ ╲╱╲╱╳╳╳\n" +
                              "  ║│╱ ╲│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╽ │┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▎\n" +
                              "  ║└─╥─┘║  │╚═╤═╝│  │╘═╪═╛│  │╙─╀─╜│  ┃└─╂─┘┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▏\n" +
                              "  ╚══╩══╝  └──┴──┘  ╰──┴──╯  ╰──┴──╯  ┗━━┻━━┛           └╌╌┘ ╎ ┗╍╍┛ ┋  ▁▂▃▄▅▆▇█\n");

    }

    @Test // https://www.w3.org/2001/06/utf-8-wrong/UTF-8-test.html
    public void testInvalidStrings()
    {
        // continuation bytes only
        assertInvalidUtf8String(0x80);
        assertInvalidUtf8String(0xbf);
        assertInvalidUtf8String(0x80,0x80);
        // Bad trailing bytes
        assertInvalidUtf8String(0xF0, 0xA4, 0xAD, 0x7F);
        assertInvalidUtf8String(0xF0, 0xA4, 0xAD, 0x7F);
        // first bytes of 2-byte sequences (0xc0-0xdf), each followed by a space character
        assertInvalidUtf8String(0xc0, ' ', 0xdf, ' ');
        // first bytes of 3-byte sequences (0xe0-0xef), each followed by a space character
        assertInvalidUtf8String(0xe0, ' ', 0xe1, ' ');
        // first bytes of 4-byte sequences (0xf0-0xf7), each followed by a space character
        assertInvalidUtf8String(0xf0, ' ', 0xf7, ' ');
        // first bytes of 5-byte sequences (0xf8-0xfb), each followed by a space character
        assertInvalidUtf8String(0xf8, ' ', 0xfb, ' ');
        // first bytes of 6-byte sequences (0xfc-0xfd), each followed by a space character
        assertInvalidUtf8String(0xfc, ' ', 0xfd, ' ');
        //  Impossible bytes
        assertInvalidUtf8String(0xfe);
        assertInvalidUtf8String(0xff);
        assertInvalidUtf8String(0xfe, 0xfe, 0xff, 0xff);
        // Sequences with last continuation byte missing
        assertInvalidUtf8String(0xc0);
        assertInvalidUtf8String(0xe0, 0x80);
        // Maximum overlong sequences
        assertInvalidUtf8String(0xc1, 0xbf);

        // 'ASCII' + continuation byte at the end
        assertInvalidUtf8String(0x41, 0x53, 0x43, 0x49, 0x49, 0x80);
    }

    public void assertValidUtf8String(String value)
    {
        byte[] byteArrayValue = value.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bufferValue = ByteBuffer.wrap(byteArrayValue);
        ByteBuffer bufferValueInTheMiddle = ByteBuffer.allocate(byteArrayValue.length + 2 + 2);
        wrapValueWithImpossibleBytes(bufferValueInTheMiddle, byteArrayValue);

        ByteBuffer directBufferValue = ByteBuffer.allocateDirect(byteArrayValue.length);
        directBufferValue.put(byteArrayValue);
        directBufferValue.rewind();
        ByteBuffer directBufferValueInTheMiddle = ByteBuffer.allocate(byteArrayValue.length + 2 + 2);
        wrapValueWithImpossibleBytes(directBufferValueInTheMiddle, byteArrayValue);

        assertTrue(UTF8Serializer.UTF8Validator.validate(byteArrayValue, ByteArrayAccessor.instance));

        assertTrue(UTF8Serializer.UTF8Validator.validate(bufferValue, ByteBufferAccessor.instance));
        assertTrue(UTF8Serializer.UTF8Validator.validate(bufferValueInTheMiddle, ByteBufferAccessor.instance));

        assertTrue(UTF8Serializer.UTF8Validator.validate(directBufferValue, ByteBufferAccessor.instance));
        assertTrue(UTF8Serializer.UTF8Validator.validate(bufferValueInTheMiddle, ByteBufferAccessor.instance));
    }

    private static void wrapValueWithImpossibleBytes(ByteBuffer bufferValueInTheMiddle, byte[] byteArrayValue)
    {
        // bufferValue wrapped by impossible bytes
        // to ensure that validate method does not read outside of buffer boundaries
        bufferValueInTheMiddle.put((byte)0xfe);
        bufferValueInTheMiddle.put((byte)0xfe);
        bufferValueInTheMiddle.put(byteArrayValue);
        bufferValueInTheMiddle.put((byte)0xfe);
        bufferValueInTheMiddle.put((byte)0xfe);
        bufferValueInTheMiddle.rewind();
        bufferValueInTheMiddle.position(2);
        bufferValueInTheMiddle.limit(bufferValueInTheMiddle.limit() - 2);
    }

    public void assertInvalidUtf8String(int ... bytes)
    {
        byte[] byteArrayValue = toByteArray(bytes);
        ByteBuffer bufferValue = ByteBuffer.wrap(byteArrayValue);
        ByteBuffer directBufferValue = ByteBuffer.allocateDirect(byteArrayValue.length);
        directBufferValue.put(byteArrayValue);
        directBufferValue.rewind();

        assertFalse(UTF8Serializer.UTF8Validator.validate(byteArrayValue, ByteArrayAccessor.instance));
        assertFalse(UTF8Serializer.UTF8Validator.validate(bufferValue, ByteBufferAccessor.instance));
        assertFalse(UTF8Serializer.UTF8Validator.validate(directBufferValue, ByteBufferAccessor.instance));
    }

    private static byte[] toByteArray(int... bytes)
    {
        byte[] value = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++)
            value[i] = (byte) bytes[i];
        return value;
    }
}
