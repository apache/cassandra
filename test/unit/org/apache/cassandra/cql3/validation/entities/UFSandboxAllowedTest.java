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

package org.apache.cassandra.cql3.validation.entities;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

/**
 * Runs allowed Java UDFs with and without UDF threads. Bodies avoid the UDF compiler limitations listed in the
 * functions documentation.
 */
public class UFSandboxAllowedTest extends CQLTester
{
    private static final String[][] TEXT_UDFS =
    {
        // collections
        { "Map<String, List<Integer>> m = new TreeMap<String, List<Integer>>();" +
          "m.put(\"a\", new ArrayList<Integer>(Arrays.asList(3, 1, 2)));" +
          "Collections.sort(m.get(\"a\"));" +
          "return m.toString();", "{a=[1, 2, 3]}" },
        { "Map<String, Integer> m = new LinkedHashMap<String, Integer>(); m.put(\"k\", 1); m.put(\"l\", 2);" +
          "StringBuilder sb = new StringBuilder();" +
          "for (Map.Entry<String, Integer> e : m.entrySet()) sb.append(e.getKey()).append(e.getValue());" +
          "Deque<String> d = new ArrayDeque<String>(); d.push(\"x\"); sb.append(d.peek());" +
          "Set<String> s = new TreeSet<String>(Arrays.asList(\"b\", \"a\")); Iterator<String> it = s.iterator();" +
          "while (it.hasNext()) sb.append(it.next());" +
          "return sb.toString();", "k1l2xab" },
        { "List<String> l = new ArrayList<String>(Arrays.asList(\"b\", \"a\")); l.sort(null);" +
          "return String.join(\",\", l) + Objects.hash(1) + Optional.of(\"o\").get();", "a,b32o" },
        // strings and formatting
        { "return String.format(Locale.ROOT, \"%05d %s %.3f\", 42, \"x\", Math.PI) + \" \" + " +
          "\"abc\".toUpperCase(Locale.ROOT).substring(1) + \" \" + Integer.toHexString(255);", "00042 x 3.142 BC ff" },
        { "double d = 1.5; int i = 2; Object o = Boolean.TRUE; return \"a\" + d + i + o + 'c' + 3L;", "a1.52truec3" },
        // math
        { "return new java.math.BigDecimal(\"1.15\").setScale(1, java.math.RoundingMode.HALF_UP).toPlainString() + \" \" + " +
          "java.math.BigInteger.valueOf(2).pow(70);", "1.2 1180591620717411303424" },
        // time
        { "return java.time.LocalDate.of(2020, 1, 1).plusDays(40).format(java.time.format.DateTimeFormatter.ISO_DATE) + \" \" + " +
          "java.time.ZoneId.of(\"Europe/Paris\").getRules().getOffset(java.time.Instant.EPOCH) + \" \" + " +
          "java.time.Duration.ofMinutes(90).toHours() + \" \" + java.time.DayOfWeek.MONDAY.plus(1);",
          "2020-02-10 +01:00 1 TUESDAY" },
        // regex
        { "java.util.regex.Matcher m = java.util.regex.Pattern.compile(\"(\\\\d+)\").matcher(\"ab12cd\");" +
          "return m.find() ? m.group(1) : \"\";", "12" },
        // exceptions
        { "try { Integer.parseInt(\"x\"); return \"\"; }" +
          "catch (NumberFormatException e) { return e.getClass().getSimpleName() + \":\" + e.getMessage(); }",
          "NumberFormatException:For input string: \"x\"" },
        { "try { throw new IllegalStateException(\"s\"); }" +
          "catch (RuntimeException e) { return e.getMessage() + (e.getStackTrace().length > 0); }", "strue" },
        // misc java.util
        { "return UUID.nameUUIDFromBytes(new byte[]{ 1 }).toString() + \" \" + " +
          "Base64.getEncoder().encodeToString(new byte[]{ 1, 2 });", "55a54008-ad1b-3589-aa21-0d2629c1df41 AQI=" },
        // ByteBuffer and the driver types
        { "ByteBuffer b = ByteBuffer.allocate(4); b.putInt(5); b.flip();" +
          "return b.getInt(0) + \" \" + org.apache.cassandra.cql3.functions.types.utils.Bytes.toHexString(b) + \" \" + " +
          "org.apache.cassandra.cql3.functions.types.TypeCodec.cint().format(7);", "5 0x00000005 7" },
        { "return udfContext.newTupleValue(\"tuple<int, text>\").setInt(0, 7).setString(1, \"z\").toString();",
          "(7,'z')" },
        { "return super.udfContext.newTupleValue(\"tuple<int>\").setInt(0, 1).toString() + " +
          "super.decompose(org.apache.cassandra.transport.ProtocolVersion.CURRENT, \"ab\").remaining();", "(1)2" },
        // stack map frames naming supertypes that no instruction names (AbstractStringBuilder, AbstractMap)
        { "Object o = val > 0 ? new StringBuilder(\"a\") : new StringBuffer(\"b\");" +
          "Object m = val > 0 ? new HashMap<String, String>() : new TreeMap<String, String>();" +
          "return o.toString() + m;", "a{}" },
        // summary statistics, and merging two of them in a stack map frame
        { "IntSummaryStatistics s = new IntSummaryStatistics(); s.accept(3); s.accept(1);" +
          "LongSummaryStatistics l = new LongSummaryStatistics(); l.accept(5L);" +
          "DoubleSummaryStatistics d = new DoubleSummaryStatistics(); d.accept(0.5);" +
          "Object o = val > 0 ? s : l;" +
          "return s.getMax() + \" \" + s.getAverage() + \" \" + l.getSum() + \" \" + d.getMax() + \" \" + (o == s);",
          "3 2.0 5 0.5 true" },
        // codec parsing (the inet codec only accepts address literals)
        { "return String.valueOf(org.apache.cassandra.cql3.functions.types.TypeCodec.cint().parse(\"42\")) + \" \" + " +
          "org.apache.cassandra.cql3.functions.types.TypeCodec.inet().parse(\"'127.0.0.1'\").getHostAddress();", "42 127.0.0.1" },
        // Class.cast and isInstance, with an argument of static type Object
        { "Object o = \"x\"; return String.class.cast(o) + o.getClass().isInstance(o);", "xtrue" },
        // array clone
        { "int[] a = new int[]{ 3, 1 }; int[] b = a.clone(); Arrays.sort(b);" +
          "return Arrays.toString(b) + a.length;", "[1, 3]2" },
        // Class members that do not reflect on members
        { "return String.class.getCanonicalName() + \" \" + String.class.getPackageName() + \" \" + " +
          "int[].class.getComponentType() + \" \" + Integer.class.getSuperclass().getSimpleName() + \" \" + " +
          "(String.class.getInterfaces().length > 0);", "java.lang.String java.lang int Number true" },
        // other calendars and zone rules
        { "java.time.LocalDate d = java.time.LocalDate.of(2020, 1, 1);" +
          "return java.time.chrono.JapaneseDate.from(d) + \", \" + java.time.chrono.ThaiBuddhistDate.from(d) + \", \" + " +
          "java.time.chrono.MinguoDate.from(d) + \", \" + java.time.chrono.HijrahDate.from(d).getChronology().getId();",
          "Japanese Reiwa 2-01-01, ThaiBuddhist BE 2563-01-01, Minguo ROC 109-01-01, Hijrah-umalqura" },
        { "try { java.time.ZoneId.of(\"No/Such\"); return \"\"; }" +
          "catch (java.time.zone.ZoneRulesException e) { return e.getMessage() + \" \" + " +
          "java.time.ZoneId.of(\"Europe/Paris\").getRules().getTransitionRules().get(0).getMonth(); }",
          "Unknown time-zone ID: No/Such MARCH" },
        // text boundaries and collation
        { "java.text.BreakIterator b = java.text.BreakIterator.getWordInstance(Locale.ROOT); b.setText(\"ab cd\");" +
          "java.text.CharacterIterator i = new java.text.StringCharacterIterator(\"xy\");" +
          "java.text.RuleBasedCollator c = (java.text.RuleBasedCollator) java.text.Collator.getInstance(Locale.ROOT);" +
          "return b.next() + \" \" + i.last() + \" \" + c.compare(\"a\", \"b\");", "2 y -1" },
        { "WeakHashMap<String, String> m = new WeakHashMap<String, String>(); m.put(\"a\", \"b\");" +
          "try { throw new SecurityException(\"s\"); } catch (SecurityException e) { return m.get(\"a\") + e.getMessage(); }",
          "bs" },
    };

    @Before
    public void createData()
    {
        createTable("CREATE TABLE %s (key int primary key, dval double)");
        execute("INSERT INTO %s (key, dval) VALUES (1, 1)");
    }

    @Test
    public void testAllowedWithThreads() throws Throwable
    {
        runAll();
        String body = "return String.valueOf(System.currentTimeMillis() > 0 && System.nanoTime() != 0);";
        assertRows(execute(select(body)), row("true"));
    }

    @Test
    public void testAllowedWithoutThreads() throws Throwable
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        boolean threads = conf.user_defined_functions_threads_enabled;
        boolean insecure = conf.allow_insecure_udfs;
        conf.user_defined_functions_threads_enabled = false;
        conf.allow_insecure_udfs = true;
        try
        {
            runAll();
        }
        finally
        {
            conf.user_defined_functions_threads_enabled = threads;
            conf.allow_insecure_udfs = insecure;
        }
    }

    @Test
    public void testNewerJavaClasses() throws Throwable
    {
        int feature = Runtime.version().feature();
        if (feature >= 17)
        {
            String body = "return java.util.HexFormat.of().formatHex(new byte[]{ 1, (byte) 0xab }) + \" \" + " +
                          "java.text.NumberFormat.getCompactNumberInstance(Locale.US, java.text.NumberFormat.Style.SHORT)" +
                          ".format(1200);";
            assertRows(execute(select(body)), row("01ab 1K"));
        }
        if (feature >= 21)
        {
            String body = "SequencedCollection<String> c = new ArrayList<String>(Arrays.asList(\"a\", \"b\"));" +
                          "SequencedMap<String, Integer> m = new LinkedHashMap<String, Integer>(); m.put(\"k\", 1);" +
                          "return c.reversed().toString() + c.getFirst() + m.firstEntry();";
            assertRows(execute(select(body)), row("[b, a]ak=1"));
        }
    }

    @Test
    public void testTupleResult() throws Throwable
    {
        String f = createJavaFunction("double", "tuple<int, text>",
                                      "return udfContext.newReturnTupleValue().setInt(0, 7).setString(1, \"z\");");
        assertRows(execute("SELECT " + f + "(dval) FROM %s WHERE key = 1"), row(tuple(7, "z")));
    }

    @Test
    public void testInetArgument() throws Throwable
    {
        createTable("CREATE TABLE %s (key int primary key, i inet)");
        execute("INSERT INTO %s (key, i) VALUES (1, '127.0.0.1')");
        String f = createJavaFunction("inet", "text", "return val.getHostAddress() + val.isLoopbackAddress();");
        assertRows(execute("SELECT " + f + "(i) FROM %s WHERE key = 1"), row("127.0.0.1true"));
    }

    private void runAll() throws Throwable
    {
        for (String[] udf : TEXT_UDFS)
            assertRows(execute(select(udf[0])), row(udf[1]));
    }

    private String select(String body) throws Throwable
    {
        return "SELECT " + createJavaFunction("double", "text", body) + "(dval) FROM %s WHERE key = 1";
    }
}
