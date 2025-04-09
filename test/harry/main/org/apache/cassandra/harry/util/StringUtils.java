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

package org.apache.cassandra.harry.util;

import org.apache.cassandra.harry.MagicConstants;

public class StringUtils
{
    /**
     * When printing out UTF-8 Strings to humans there can be issues with utf-8 control chars, which leads to
     * tools (such as Intellij) printing out things incorrectly.  The motivating case for this was the string {@code s\u000D^vf \u001F\u000CtU},
     * this string caused Intellij to delete the content in front of the string, so the output only included the parts after this string!
     *
     * This method is only expected to be used when reporting to humans, and not for internal logic.
     */
    public static String escapeControlChars(String input)
    {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < input.length(); i++)
        {
            char c = input.charAt(i);
            if (Character.isISOControl(c) && c != '\n')
                result.append(String.format("\\u%04X", (int) c));
            else
                result.append(c);
        }

        return result.toString();
    }

    public static String toString(long[] arr)
    {
        if (arr.length == 0)
            return "EMPTY";
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < arr.length; i++)
        {
            if (arr[i] == MagicConstants.UNSET_DESCR)
                s.append("UNSET");
            else if (arr[i] == MagicConstants.UNKNOWN_DESCR)
                s.append("UNKNOWN");
            else if (arr[i] == MagicConstants.NIL_DESCR)
                s.append("NIL");
            else
            {
                s.append(arr[i]);
                s.append("L");
            }
            if (i < (arr.length - 1))
                s.append(',');
        }
        return s.toString();
    }

    public static String toString(int[] arr)
    {
        if (arr.length == 0)
            return "EMPTY";
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < arr.length; i++)
        {
            s.append(toString(arr[i]));
            if (i < (arr.length - 1))
                s.append(',');
        }
        return s.toString();
    }

    public static String toString(int idx)
    {
        if (idx == MagicConstants.UNSET_IDX)
            return "UNSET";
        else if (idx == MagicConstants.UNKNOWN_IDX)
            return "UNKNOWN";
        else if (idx == MagicConstants.NIL_IDX)
            return "NIL";
        else
            return Integer.toString(idx);
    }
}
