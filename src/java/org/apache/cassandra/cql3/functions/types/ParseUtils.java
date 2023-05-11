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
package org.apache.cassandra.cql3.functions.types;

import java.text.*;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Simple utility class used to help parsing CQL values (mainly UDT and collection ones).
 */
public abstract class ParseUtils
{

    /**
     * Valid ISO-8601 patterns for CQL timestamp literals.
     */
    private static final String[] iso8601Patterns =
    new String[]{
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mmZ",
    "yyyy-MM-dd HH:mm:ssZ",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.SSSZ",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd'T'HH:mmZ",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssZ",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    "yyyy-MM-dd",
    "yyyy-MM-ddZ"
    };

    /**
     * Returns the index of the first character in toParse from idx that is not a "space".
     *
     * @param toParse the string to skip space on.
     * @param idx     the index to start skipping space from.
     * @return the index of the first character in toParse from idx that is not a "space.
     */
    static int skipSpaces(String toParse, int idx)
    {
        while (isBlank(toParse.charAt(idx))) ++idx;
        return idx;
    }

    /**
     * Assuming that idx points to the beginning of a CQL value in toParse, returns the index of the
     * first character after this value.
     *
     * @param toParse the string to skip a value form.
     * @param idx     the index to start parsing a value from.
     * @return the index ending the CQL value starting at {@code idx}.
     * @throws IllegalArgumentException if idx doesn't point to the start of a valid CQL value.
     */
    static int skipCQLValue(String toParse, int idx)
    {
        if (idx >= toParse.length()) throw new IllegalArgumentException();

        if (isBlank(toParse.charAt(idx))) throw new IllegalArgumentException();

        int cbrackets = 0;
        int sbrackets = 0;
        int parens = 0;
        boolean inString = false;

        do
        {
            char c = toParse.charAt(idx);
            if (inString)
            {
                if (c == '\'')
                {
                    if (idx + 1 < toParse.length() && toParse.charAt(idx + 1) == '\'')
                    {
                        ++idx; // this is an escaped quote, skip it
                    }
                    else
                    {
                        inString = false;
                        if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
                    }
                }
                // Skip any other character
            }
            else if (c == '\'')
            {
                inString = true;
            }
            else if (c == '{')
            {
                ++cbrackets;
            }
            else if (c == '[')
            {
                ++sbrackets;
            }
            else if (c == '(')
            {
                ++parens;
            }
            else if (c == '}')
            {
                if (cbrackets == 0) return idx;

                --cbrackets;
                if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
            }
            else if (c == ']')
            {
                if (sbrackets == 0) return idx;

                --sbrackets;
                if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
            }
            else if (c == ')')
            {
                if (parens == 0) return idx;

                --parens;
                if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx + 1;
            }
            else if (isBlank(c) || !isIdentifierChar(c))
            {
                if (cbrackets == 0 && sbrackets == 0 && parens == 0) return idx;
            }
        } while (++idx < toParse.length());

        if (inString || cbrackets != 0 || sbrackets != 0 || parens != 0)
            throw new IllegalArgumentException();
        return idx;
    }

    /**
     * Assuming that idx points to the beginning of a CQL identifier in toParse, returns the index of
     * the first character after this identifier.
     *
     * @param toParse the string to skip an identifier from.
     * @param idx     the index to start parsing an identifier from.
     * @return the index ending the CQL identifier starting at {@code idx}.
     * @throws IllegalArgumentException if idx doesn't point to the start of a valid CQL identifier.
     */
    static int skipCQLId(String toParse, int idx)
    {
        if (idx >= toParse.length()) throw new IllegalArgumentException();

        char c = toParse.charAt(idx);
        if (isIdentifierChar(c))
        {
            while (idx < toParse.length() && isIdentifierChar(toParse.charAt(idx))) idx++;
            return idx;
        }

        if (c != '"') throw new IllegalArgumentException();

        while (++idx < toParse.length())
        {
            c = toParse.charAt(idx);
            if (c != '"') continue;

            if (idx + 1 < toParse.length() && toParse.charAt(idx + 1) == '\"')
                ++idx; // this is an escaped double quote, skip it
            else return idx + 1;
        }
        throw new IllegalArgumentException();
    }

    /**
     * Return {@code true} if the given character is allowed in a CQL identifier, that is, if it is in
     * the range: {@code [0..9a..zA..Z-+._&]}.
     *
     * @param c The character to inspect.
     * @return {@code true} if the given character is allowed in a CQL identifier, {@code false}
     * otherwise.
     */
    static boolean isIdentifierChar(int c)
    {
        return (c >= '0' && c <= '9')
               || (c >= 'a' && c <= 'z')
               || (c >= 'A' && c <= 'Z')
               || c == '-'
               || c == '+'
               || c == '.'
               || c == '_'
               || c == '&';
    }

    /**
     * Return {@code true} if the given character is a valid whitespace character in CQL, that is, if
     * it is a regular space, a tabulation sign, or a new line sign.
     *
     * @param c The character to inspect.
     * @return {@code true} if the given character is a valid whitespace character, {@code false}
     * otherwise.
     */
    static boolean isBlank(int c)
    {
        return c == ' ' || c == '\t' || c == '\n';
    }

    /**
     * Check whether the given string corresponds to a valid CQL long literal. Long literals are
     * composed solely by digits, but can have an optional leading minus sign.
     *
     * @param str The string to inspect.
     * @return {@code true} if the given string corresponds to a valid CQL integer literal, {@code
     * false} otherwise.
     */
    static boolean isLongLiteral(String str)
    {
        if (str == null || str.isEmpty()) return false;
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++)
        {
            char c = chars[i];
            if ((c < '0' && (i != 0 || c != '-')) || c > '9') return false;
        }
        return true;
    }

    /**
     * Return {@code true} if the given string is surrounded by single quotes, and {@code false}
     * otherwise.
     *
     * @param value The string to inspect.
     * @return {@code true} if the given string is surrounded by single quotes, and {@code false}
     * otherwise.
     */
    static boolean isQuoted(String value)
    {
        return isQuoted(value, '\'');
    }

    /**
     * Quote the given string; single quotes are escaped. If the given string is null, this method
     * returns a quoted empty string ({@code ''}).
     *
     * @param value The value to quote.
     * @return The quoted string.
     */
    public static String quote(String value)
    {
        return quote(value, '\'');
    }

    /**
     * Unquote the given string if it is quoted; single quotes are unescaped. If the given string is
     * not quoted, it is returned without any modification.
     *
     * @param value The string to unquote.
     * @return The unquoted string.
     */
    static String unquote(String value)
    {
        return unquote(value, '\'');
    }

    /**
     * Double quote the given string; double quotes are escaped. If the given string is null, this
     * method returns a quoted empty string ({@code ""}).
     *
     * @param value The value to double quote.
     * @return The double quoted string.
     */
    static String doubleQuote(String value)
    {
        return quote(value, '"');
    }

    /**
     * Unquote the given string if it is double quoted; double quotes are unescaped. If the given
     * string is not double quoted, it is returned without any modification.
     *
     * @param value The string to un-double quote.
     * @return The un-double quoted string.
     */
    public static String unDoubleQuote(String value)
    {
        return unquote(value, '"');
    }

    /**
     * Parse the given string as a date, using one of the accepted ISO-8601 date patterns.
     *
     * <p>This method is adapted from Apache Commons {@code DateUtils.parseStrictly()} method (that is
     * used Cassandra side to parse date strings)..
     *
     * @throws ParseException If the given string is not a valid ISO-8601 date.
     * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with
     * timestamps' section of CQL specification</a>
     */
    static Date parseDate(String str) throws ParseException
    {
        SimpleDateFormat parser = new SimpleDateFormat();
        parser.setLenient(false);
        // set a default timezone for patterns that do not provide one
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        // Java 6 has very limited support for ISO-8601 time zone formats,
        // so we need to transform the string first
        // so that accepted patterns are correctly handled,
        // such as Z for UTC, or "+00:00" instead of "+0000".
        // Note: we cannot use the X letter in the pattern
        // because it has been introduced in Java 7.
        str = str.replaceAll("(\\+|\\-)(\\d\\d):(\\d\\d)$", "$1$2$3");
        str = str.replaceAll("Z$", "+0000");
        ParsePosition pos = new ParsePosition(0);
        for (String parsePattern : iso8601Patterns)
        {
            parser.applyPattern(parsePattern);
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if (date != null && pos.getIndex() == str.length())
            {
                return date;
            }
        }
        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    /**
     * Parse the given string as a date, using the supplied date pattern.
     *
     * <p>This method is adapted from Apache Commons {@code DateUtils.parseStrictly()} method (that is
     * used Cassandra side to parse date strings)..
     *
     * @throws ParseException If the given string cannot be parsed with the given pattern.
     * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps">'Working with
     * timestamps' section of CQL specification</a>
     */
    static Date parseDate(String str, String pattern) throws ParseException
    {
        SimpleDateFormat parser = new SimpleDateFormat();
        parser.setLenient(false);
        // set a default timezone for patterns that do not provide one
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        // Java 6 has very limited support for ISO-8601 time zone formats,
        // so we need to transform the string first
        // so that accepted patterns are correctly handled,
        // such as Z for UTC, or "+00:00" instead of "+0000".
        // Note: we cannot use the X letter in the pattern
        // because it has been introduced in Java 7.
        str = str.replaceAll("(\\+|\\-)(\\d\\d):(\\d\\d)$", "$1$2$3");
        str = str.replaceAll("Z$", "+0000");
        ParsePosition pos = new ParsePosition(0);
        parser.applyPattern(pattern);
        pos.setIndex(0);
        Date date = parser.parse(str, pos);
        if (date != null && pos.getIndex() == str.length())
        {
            return date;
        }
        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    /**
     * Parse the given string as a time, using the following time pattern: {@code
     * hh:mm:ss[.fffffffff]}.
     *
     * <p>This method is loosely based on {@code java.sql.Timestamp}.
     *
     * @param str The string to parse.
     * @return A long value representing the number of nanoseconds since midnight.
     * @throws ParseException if the string cannot be parsed.
     * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtime">'Working with time'
     * section of CQL specification</a>
     */
    static long parseTime(String str) throws ParseException
    {
        String nanos_s;

        long hour;
        long minute;
        long second;
        long a_nanos = 0;

        String formatError = "Timestamp format must be hh:mm:ss[.fffffffff]";
        String zeros = "000000000";

        if (str == null) throw new IllegalArgumentException(formatError);
        str = str.trim();

        // Parse the time
        int firstColon = str.indexOf(':');
        int secondColon = str.indexOf(':', firstColon + 1);

        // Convert the time; default missing nanos
        if (firstColon > 0 && secondColon > 0 && secondColon < str.length() - 1)
        {
            int period = str.indexOf('.', secondColon + 1);
            hour = Integer.parseInt(str.substring(0, firstColon));
            if (hour < 0 || hour >= 24) throw new IllegalArgumentException("Hour out of bounds.");

            minute = Integer.parseInt(str.substring(firstColon + 1, secondColon));
            if (minute < 0 || minute >= 60) throw new IllegalArgumentException("Minute out of bounds.");

            if (period > 0 && period < str.length() - 1)
            {
                second = Integer.parseInt(str.substring(secondColon + 1, period));
                if (second < 0 || second >= 60) throw new IllegalArgumentException("Second out of bounds.");

                nanos_s = str.substring(period + 1);
                if (nanos_s.length() > 9) throw new IllegalArgumentException(formatError);
                if (!Character.isDigit(nanos_s.charAt(0))) throw new IllegalArgumentException(formatError);
                nanos_s = nanos_s + zeros.substring(0, 9 - nanos_s.length());
                a_nanos = Integer.parseInt(nanos_s);
            }
            else if (period > 0) throw new ParseException(formatError, -1);
            else
            {
                second = Integer.parseInt(str.substring(secondColon + 1));
                if (second < 0 || second >= 60) throw new ParseException("Second out of bounds.", -1);
            }
        }
        else throw new ParseException(formatError, -1);

        long rawTime = 0;
        rawTime += TimeUnit.HOURS.toNanos(hour);
        rawTime += TimeUnit.MINUTES.toNanos(minute);
        rawTime += TimeUnit.SECONDS.toNanos(second);
        rawTime += a_nanos;
        return rawTime;
    }

    /**
     * Format the given long value as a CQL time literal, using the following time pattern: {@code
     * hh:mm:ss[.fffffffff]}.
     *
     * @param value A long value representing the number of nanoseconds since midnight.
     * @return The formatted value.
     * @see <a href="https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtime">'Working with time'
     * section of CQL specification</a>
     */
    static String formatTime(long value)
    {
        int nano = (int) (value % 1000000000);
        value -= nano;
        value /= 1000000000;
        int seconds = (int) (value % 60);
        value -= seconds;
        value /= 60;
        int minutes = (int) (value % 60);
        value -= minutes;
        value /= 60;
        int hours = (int) (value % 24);
        value -= hours;
        value /= 24;
        assert (value == 0);
        StringBuilder sb = new StringBuilder();
        leftPadZeros(hours, 2, sb);
        sb.append(':');
        leftPadZeros(minutes, 2, sb);
        sb.append(':');
        leftPadZeros(seconds, 2, sb);
        sb.append('.');
        leftPadZeros(nano, 9, sb);
        return sb.toString();
    }

    /**
     * Return {@code true} if the given string is surrounded by the quote character given, and {@code
     * false} otherwise.
     *
     * @param value The string to inspect.
     * @return {@code true} if the given string is surrounded by the quote character, and {@code
     * false} otherwise.
     */
    public static boolean isQuoted(String value, char quoteChar)
    {
        return value != null
               && value.length() > 1
               && value.charAt(0) == quoteChar
               && value.charAt(value.length() - 1) == quoteChar;
    }

    /**
     * @param quoteChar " or '
     * @return A quoted empty string.
     */
    private static String emptyQuoted(char quoteChar)
    {
        // don't handle non quote characters, this is done so that these are interned and don't create
        // repeated empty quoted strings.
        assert quoteChar == '"' || quoteChar == '\'';
        if (quoteChar == '"') return "\"\"";
        else return "''";
    }

    /**
     * Quotes text and escapes any existing quotes in the text. {@code String.replace()} is a bit too
     * inefficient (see JAVA-67, JAVA-1262).
     *
     * @param text      The text.
     * @param quoteChar The character to use as a quote.
     * @return The text with surrounded in quotes with all existing quotes escaped with (i.e. '
     * becomes '')
     */
    private static String quote(String text, char quoteChar)
    {
        if (text == null || text.isEmpty()) return emptyQuoted(quoteChar);

        int nbMatch = 0;
        int start = -1;
        do
        {
            start = text.indexOf(quoteChar, start + 1);
            if (start != -1) ++nbMatch;
        } while (start != -1);

        // no quotes found that need to be escaped, simply surround in quotes and return.
        if (nbMatch == 0) return quoteChar + text + quoteChar;

        // 2 for beginning and end quotes.
        // length for original text
        // nbMatch for escape characters to add to quotes to be escaped.
        int newLength = 2 + text.length() + nbMatch;
        char[] result = new char[newLength];
        result[0] = quoteChar;
        result[newLength - 1] = quoteChar;
        int newIdx = 1;
        for (int i = 0; i < text.length(); i++)
        {
            char c = text.charAt(i);
            if (c == quoteChar)
            {
                // escape quote with another occurrence.
                result[newIdx++] = c;
                result[newIdx++] = c;
            }
            else
            {
                result[newIdx++] = c;
            }
        }
        return new String(result);
    }

    /**
     * Unquotes text and unescapes non surrounding quotes. {@code String.replace()} is a bit too
     * inefficient (see JAVA-67, JAVA-1262).
     *
     * @param text      The text
     * @param quoteChar The character to use as a quote.
     * @return The text with surrounding quotes removed and non surrounding quotes unescaped (i.e. ''
     * becomes ')
     */
    private static String unquote(String text, char quoteChar)
    {
        if (!isQuoted(text, quoteChar)) return text;

        if (text.length() == 2) return "";

        String search = emptyQuoted(quoteChar);
        int nbMatch = 0;
        int start = -1;
        do
        {
            start = text.indexOf(search, start + 2);
            // ignore the second to last character occurrence, as the last character is a quote.
            if (start != -1 && start != text.length() - 2) ++nbMatch;
        } while (start != -1);

        // no escaped quotes found, simply remove surrounding quotes and return.
        if (nbMatch == 0) return text.substring(1, text.length() - 1);

        // length of the new string will be its current length - the number of occurrences.
        int newLength = text.length() - nbMatch - 2;
        char[] result = new char[newLength];
        int newIdx = 0;
        // track whenever a quoteChar is encountered and the previous character is not a quoteChar.
        boolean firstFound = false;
        for (int i = 1; i < text.length() - 1; i++)
        {
            char c = text.charAt(i);
            if (c == quoteChar)
            {
                if (firstFound)
                {
                    // The previous character was a quoteChar, don't add this to result, this action in
                    // effect removes consecutive quotes.
                    firstFound = false;
                }
                else
                {
                    // found a quoteChar and the previous character was not a quoteChar, include in result.
                    firstFound = true;
                    result[newIdx++] = c;
                }
            }
            else
            {
                // non quoteChar encountered, include in result.
                result[newIdx++] = c;
                firstFound = false;
            }
        }
        return new String(result);
    }

    private static void leftPadZeros(int value, int digits, StringBuilder sb)
    {
        sb.append(String.format("%0" + digits + 'd', value));
    }

    private ParseUtils()
    {
    }
}
