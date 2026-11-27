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

package org.apache.cassandra.cql3.functions;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.text.ChoiceFormat;
import java.text.CollationKey;
import java.text.Collator;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.Format;
import java.text.MessageFormat;
import java.text.Normalizer;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.RuleBasedCollator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.chrono.ChronoZonedDateTime;
import java.time.chrono.Chronology;
import java.time.chrono.Era;
import java.time.chrono.HijrahChronology;
import java.time.chrono.HijrahDate;
import java.time.chrono.HijrahEra;
import java.time.chrono.IsoChronology;
import java.time.chrono.IsoEra;
import java.time.chrono.JapaneseChronology;
import java.time.chrono.JapaneseDate;
import java.time.chrono.JapaneseEra;
import java.time.chrono.MinguoChronology;
import java.time.chrono.MinguoDate;
import java.time.chrono.MinguoEra;
import java.time.chrono.ThaiBuddhistChronology;
import java.time.chrono.ThaiBuddhistDate;
import java.time.chrono.ThaiBuddhistEra;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.DecimalStyle;
import java.time.format.FormatStyle;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.time.temporal.ValueRange;
import java.time.temporal.WeekFields;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesException;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Currency;
import java.util.Date;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.EmptyStackException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.IllegalFormatException;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Stack;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import org.objectweb.asm.Type;

import org.apache.cassandra.cql3.functions.types.AbstractGettableData;
import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.Duration;
import org.apache.cassandra.cql3.functions.types.GettableByIndexData;
import org.apache.cassandra.cql3.functions.types.GettableByNameData;
import org.apache.cassandra.cql3.functions.types.GettableData;
import org.apache.cassandra.cql3.functions.types.LocalDate;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.cql3.functions.types.SettableByNameData;
import org.apache.cassandra.cql3.functions.types.SettableData;
import org.apache.cassandra.cql3.functions.types.TupleType;
import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.cql3.functions.types.TypeCodec;
import org.apache.cassandra.cql3.functions.types.TypeTokens;
import org.apache.cassandra.cql3.functions.types.UDTValue;
import org.apache.cassandra.cql3.functions.types.UserType;
import org.apache.cassandra.cql3.functions.types.VectorType;
import org.apache.cassandra.cql3.functions.types.exceptions.CodecNotFoundException;
import org.apache.cassandra.cql3.functions.types.exceptions.DriverException;
import org.apache.cassandra.cql3.functions.types.exceptions.DriverInternalError;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.cql3.functions.types.utils.Bytes;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.FunctionExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * The types, methods and fields that Java UDFs may use in the bytecode UDF sandbox. A member is only usable if every
 * type in its descriptor is listed, so {@code String.chars()}, which returns an {@code IntStream}, is not.
 */
public final class UDFSandboxPolicy
{
    private static final Set<String> INET = ImmutableSet.of("getAddress", "getHostAddress", "isAnyLocalAddress",
                                                            "isLinkLocalAddress", "isLoopbackAddress", "isMulticastAddress",
                                                            "isSiteLocalAddress", "isMCGlobal", "isMCLinkLocal",
                                                            "isMCNodeLocal", "isMCOrgLocal", "isMCSiteLocal",
                                                            "equals", "hashCode", "toString");

    // after INET, which the constructor uses
    public static final UDFSandboxPolicy STANDARD = new UDFSandboxPolicy(false);
    /** {@link #STANDARD} plus the members of {@link System}; used with {@code allow_extra_insecure_udfs}. */
    public static final UDFSandboxPolicy EXTRA_INSECURE = new UDFSandboxPolicy(true);

    enum Kind
    {
        ALL, MEMBERS, TYPE_ONLY
    }

    static final class Entry
    {
        final Class<?> type;
        final Kind kind;
        // ALL: excluded names. MEMBERS: granted names. TYPE_ONLY: empty.
        final Set<String> names;

        Entry(Class<?> type, Kind kind, Set<String> names)
        {
            this.type = type;
            this.kind = kind;
            this.names = names;
        }

        boolean grants(String name)
        {
            switch (kind)
            {
                case ALL: return !names.contains(name);
                case MEMBERS: return names.contains(name);
                default: return false;
            }
        }

        boolean excludes(String name)
        {
            return kind == Kind.ALL && names.contains(name);
        }
    }

    private final Map<String, Entry> entries;
    private final Map<String, Boolean> methodCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> fieldCache = new ConcurrentHashMap<>();

    private UDFSandboxPolicy(boolean extraInsecure)
    {
        Builder b = new Builder();

        // java.lang: restricted
        b.members(Object.class, "<init>", "equals", "hashCode", "toString", "getClass");
        b.members(Class.class, "getName", "getSimpleName", "getTypeName", "getCanonicalName", "getPackageName",
                  "isInstance", "cast", "isArray", "isEnum", "isPrimitive", "isInterface", "getComponentType",
                  "getSuperclass", "getInterfaces", "getEnumConstants", "equals", "hashCode", "toString");
        if (extraInsecure)
            b.all(System.class);
        else
            b.members(System.class, "currentTimeMillis", "nanoTime", "arraycopy", "identityHashCode", "lineSeparator");
        // no initCause or addSuppressed, which can build cyclic chains, and no printStackTrace, which writes to stderr
        b.members(Throwable.class, "<init>", "getMessage", "getLocalizedMessage", "getCause", "getSuppressed",
                  "getStackTrace", "toString");
        b.members(Enum.class, "name", "ordinal", "compareTo", "equals", "hashCode", "toString", "valueOf",
                  "getDeclaringClass");
        b.members(Iterable.class, "iterator");
        b.all(StackTraceElement.class);

        // java.lang: values and pure functions
        b.all(String.class, CharSequence.class, StringBuilder.class, StringBuffer.class, Character.class,
              Number.class, Byte.class, Short.class, Float.class, Double.class, Math.class, StrictMath.class,
              Comparable.class);
        b.all(Boolean.class).except("getBoolean");
        b.all(Integer.class).except("getInteger");
        b.all(Long.class).except("getLong");

        // java.lang: throwables
        for (Class<?> c : Arrays.asList(Exception.class, RuntimeException.class, IllegalArgumentException.class,
                                        IllegalStateException.class, NullPointerException.class, ArithmeticException.class,
                                        IndexOutOfBoundsException.class, ArrayIndexOutOfBoundsException.class,
                                        StringIndexOutOfBoundsException.class, ClassCastException.class,
                                        NumberFormatException.class, UnsupportedOperationException.class,
                                        NegativeArraySizeException.class, ArrayStoreException.class,
                                        CloneNotSupportedException.class, SecurityException.class,
                                        AssertionError.class))
            b.members(c, "<init>");
        b.typeOnly(Error.class, VirtualMachineError.class, StackOverflowError.class, OutOfMemoryError.class,
                   InterruptedException.class);

        // java.math, java.nio, java.net, java.io
        b.all(BigDecimal.class, MathContext.class, RoundingMode.class, Buffer.class);
        // parallelMultiply (Java 19+) runs on the common fork/join pool, like Arrays.parallelSort
        b.all(BigInteger.class).except("parallelMultiply");
        b.all(ByteBuffer.class).except("allocateDirect");
        b.members(InetAddress.class, INET);
        b.members(Inet4Address.class, INET);
        b.members(Inet6Address.class, INET);
        b.members(IOException.class, "<init>");
        b.typeOnly(Serializable.class);

        // java.util
        b.all(Collection.class, List.class, Set.class, SortedSet.class, NavigableSet.class, Queue.class, Deque.class,
              Map.class, Map.Entry.class, SortedMap.class, NavigableMap.class, Iterator.class, ListIterator.class,
              Enumeration.class, Comparator.class, RandomAccess.class,
              ArrayList.class, LinkedList.class, ArrayDeque.class, PriorityQueue.class, HashMap.class,
              LinkedHashMap.class, TreeMap.class, IdentityHashMap.class, EnumMap.class, HashSet.class,
              LinkedHashSet.class, TreeSet.class, EnumSet.class, BitSet.class, Hashtable.class, Vector.class,
              Stack.class, WeakHashMap.class, AbstractMap.SimpleEntry.class, AbstractMap.SimpleImmutableEntry.class,
              Collections.class, Objects.class, Optional.class, OptionalInt.class, OptionalLong.class,
              OptionalDouble.class, UUID.class,
              Date.class, Calendar.class, GregorianCalendar.class, SimpleTimeZone.class, Currency.class,
              Random.class, StringJoiner.class, StringTokenizer.class, Base64.class, Base64.Encoder.class,
              Base64.Decoder.class, NoSuchElementException.class, ConcurrentModificationException.class,
              EmptyStackException.class, IllegalFormatException.class,
              IntSummaryStatistics.class, LongSummaryStatistics.class, DoubleSummaryStatistics.class,
              Pattern.class, Matcher.class, MatchResult.class, PatternSyntaxException.class);
        b.allIfPresent("java.util.HexFormat", // Java 17+
                       "java.util.SequencedCollection", "java.util.SequencedSet", "java.util.SequencedMap"); // Java 21+
        b.all(Arrays.class).except("parallelSort", "parallelPrefix", "parallelSetAll");
        b.all(TimeZone.class).except("setDefault");
        b.all(Locale.class).except("setDefault");

        // java.text
        b.all(Format.class, NumberFormat.class, DecimalFormat.class, DecimalFormatSymbols.class, DateFormat.class,
              SimpleDateFormat.class, DateFormatSymbols.class, MessageFormat.class, ChoiceFormat.class,
              FieldPosition.class, ParsePosition.class, ParseException.class, Normalizer.class, Normalizer.Form.class,
              Collator.class, RuleBasedCollator.class, CollationKey.class, BreakIterator.class,
              CharacterIterator.class, StringCharacterIterator.class);
        b.allIfPresent("java.text.CompactNumberFormat", "java.text.NumberFormat$Style"); // Java 12+

        // java.time
        b.all(Clock.class, DayOfWeek.class, java.time.Duration.class, Instant.class, java.time.LocalDate.class,
              LocalDateTime.class, LocalTime.class, Month.class, MonthDay.class, OffsetDateTime.class,
              OffsetTime.class, Period.class, Year.class, YearMonth.class, ZoneId.class, ZoneOffset.class,
              ZonedDateTime.class, DateTimeException.class);
        b.all(DateTimeFormatter.class, DateTimeFormatterBuilder.class, DateTimeParseException.class,
              DecimalStyle.class, FormatStyle.class, ResolverStyle.class, SignStyle.class, TextStyle.class);
        b.all(ChronoField.class, ChronoUnit.class, IsoFields.class, JulianFields.class, Temporal.class,
              TemporalAccessor.class, TemporalAdjuster.class, TemporalAdjusters.class, TemporalAmount.class,
              TemporalField.class, TemporalUnit.class, TemporalQuery.class, TemporalQueries.class, ValueRange.class,
              WeekFields.class, UnsupportedTemporalTypeException.class);
        b.all(ChronoLocalDate.class, ChronoLocalDateTime.class, ChronoZonedDateTime.class, Chronology.class,
              IsoChronology.class, Era.class, IsoEra.class, ChronoPeriod.class);
        b.all(HijrahChronology.class, HijrahDate.class, HijrahEra.class, JapaneseChronology.class, JapaneseDate.class,
              JapaneseEra.class, MinguoChronology.class, MinguoDate.class, MinguoEra.class,
              ThaiBuddhistChronology.class, ThaiBuddhistDate.class, ThaiBuddhistEra.class);
        // not ZoneRulesProvider, which can register providers
        b.all(ZoneRules.class, ZoneOffsetTransition.class, ZoneOffsetTransitionRule.class,
              ZoneOffsetTransitionRule.TimeDefinition.class, ZoneRulesException.class);

        // Guava
        b.members(TypeToken.class, "of", "getRawType", "equals", "hashCode", "toString");

        // Cassandra
        b.all(Arguments.class, UDFContext.class);
        // UDFByteCodeVerifier checks access to the protected members of JavaUDF
        b.typeOnly(JavaUDF.class, UDFDataType.class, CodecRegistry.class);
        b.members(ProtocolVersion.class, "CURRENT", "V3", "V4", "V5", "V6", "asInt");
        b.all(UDTValue.class, TupleValue.class, AbstractGettableData.class, GettableData.class,
              GettableByIndexData.class, GettableByNameData.class, SettableData.class, SettableByIndexData.class,
              SettableByNameData.class, DataType.class, UserType.class, UserType.Field.class, TupleType.class,
              VectorType.class, LocalDate.class, Duration.class, TypeTokens.class, Bytes.class,
              CodecNotFoundException.class, DriverException.class, DriverInternalError.class,
              InvalidTypeException.class);
        for (Class<?> c : DataType.class.getDeclaredClasses())
            if (Modifier.isPublic(c.getModifiers()))
                b.all(c);
        // the inet codec's parse() accepts address literals only
        b.all(TypeCodec.class);
        for (Class<?> c : TypeCodec.class.getDeclaredClasses())
            if (Modifier.isPublic(c.getModifiers()))
                b.all(c);
        for (Class<?> c : Arrays.asList(InvalidRequestException.class, SyntaxException.class, ConfigurationException.class,
                                        UnauthorizedException.class, AuthenticationException.class))
            b.members(c, "<init>");
        b.typeOnly(CassandraException.class, RequestValidationException.class, RequestExecutionException.class,
                   FunctionExecutionException.class);

        this.entries = b.build();
    }

    @VisibleForTesting
    Map<String, Entry> entries()
    {
        return entries;
    }

    /**
     * @param internalNameOrDescriptor an internal class name ({@code java/lang/String}) or an array descriptor
     * @return true for a listed class, or an array of primitives or listed classes
     */
    public boolean isTypeAllowed(String internalNameOrDescriptor)
    {
        if (internalNameOrDescriptor.startsWith("["))
            return isTypeAllowed(Type.getType(internalNameOrDescriptor));
        return entries.containsKey(internalNameOrDescriptor);
    }

    private boolean isTypeAllowed(Type type)
    {
        if (type.getSort() == Type.ARRAY)
            type = type.getElementType();
        if (type.getSort() != Type.OBJECT)
            return true;
        return entries.containsKey(type.getInternalName());
    }

    /**
     * @return true if UDF code may call the method or constructor {@code owner.name} with descriptor {@code desc},
     * where {@code owner} is the class named in the instruction
     */
    public boolean isMethodAllowed(String owner, String name, String desc)
    {
        return methodCache.computeIfAbsent(owner + '.' + name + desc, k -> computeMethodAllowed(owner, name, desc));
    }

    /**
     * @return true if UDF code may read the field {@code owner.name} with descriptor {@code desc}
     */
    public boolean isFieldAllowed(String owner, String name, String desc)
    {
        return fieldCache.computeIfAbsent(owner + '.' + name + ':' + desc, k -> computeFieldAllowed(owner, name, desc));
    }

    private boolean computeMethodAllowed(String owner, String name, String desc)
    {
        Entry entry = entries.get(owner);
        if (entry == null || entry.excludes(name))
            return false;
        Type methodType = Type.getMethodType(desc);
        if (!isTypeAllowed(methodType.getReturnType()))
            return false;
        for (Type arg : methodType.getArgumentTypes())
            if (!isTypeAllowed(arg))
                return false;

        Executable member = findMethod(entry.type, name, desc);
        if (member == null)
            return false;
        return isGranted(entry, member.getDeclaringClass(), name);
    }

    /**
     * {@code desc} is a method descriptor or a field descriptor.
     * @return the internal name of the declaring class, or null if the owner is not listed or has no such member
     */
    @VisibleForTesting
    String declaringClass(String owner, String name, String desc)
    {
        Entry entry = entries.get(owner);
        if (entry == null)
            return null;
        if (desc.startsWith("("))
        {
            Executable member = findMethod(entry.type, name, desc);
            return member == null ? null : Type.getInternalName(member.getDeclaringClass());
        }
        try
        {
            Field field = entry.type.getField(name);
            return Type.getDescriptor(field.getType()).equals(desc) ? Type.getInternalName(field.getDeclaringClass()) : null;
        }
        catch (NoSuchFieldException e)
        {
            return null;
        }
    }

    private boolean computeFieldAllowed(String owner, String name, String desc)
    {
        Entry entry = entries.get(owner);
        if (entry == null || entry.excludes(name) || !isTypeAllowed(Type.getType(desc)))
            return false;

        Field field;
        try
        {
            field = entry.type.getField(name);
        }
        catch (NoSuchFieldException e)
        {
            return false;
        }
        if (!Type.getDescriptor(field.getType()).equals(desc))
            return false;
        return isGranted(entry, field.getDeclaringClass(), name);
    }

    // A member declared by the owner or by another listed class is granted by that class's entry; one inherited from
    // an unlisted class is granted by an all() owner.
    private boolean isGranted(Entry owner, Class<?> declaringClass, String name)
    {
        if (declaringClass == owner.type)
            return owner.grants(name);
        Entry declaring = entries.get(Type.getInternalName(declaringClass));
        if (declaring != null)
            return declaring.grants(name);
        return owner.kind == Kind.ALL; // such as AbstractStringBuilder
    }

    private static Executable findMethod(Class<?> owner, String name, String desc)
    {
        if ("<init>".equals(name))
        {
            for (Constructor<?> c : owner.getConstructors())
                if (Type.getConstructorDescriptor(c).equals(desc))
                    return c;
            return null;
        }
        for (Method m : owner.getMethods())
            if (m.getName().equals(name) && Type.getMethodDescriptor(m).equals(desc))
                return m;
        // Object's methods can be invoked on an interface type, but getMethods() of an interface omits them
        if (owner.isInterface())
            for (Method m : Object.class.getMethods())
                if (m.getName().equals(name) && Type.getMethodDescriptor(m).equals(desc))
                    return m;
        return null;
    }

    private static final class Builder
    {
        private final Map<String, Entry> entries = new HashMap<>();

        private void put(Class<?> c, Kind kind, Set<String> names)
        {
            Entry previous = entries.put(Type.getInternalName(c), new Entry(c, kind, names));
            if (previous != null)
                throw new IllegalStateException("Duplicate UDF sandbox entry for " + c.getName());
        }

        /** Every public member, including those inherited from unlisted classes. */
        void all(Class<?>... classes)
        {
            for (Class<?> c : classes)
                put(c, Kind.ALL, Collections.emptySet());
        }

        /** Like {@link #all(Class[])} for JDK classes, skipping those that the running JDK does not have. */
        void allIfPresent(String... classNames)
        {
            for (String name : classNames)
            {
                try
                {
                    all(Class.forName(name, false, ClassLoader.getPlatformClassLoader()));
                }
                catch (ClassNotFoundException e)
                {
                    // added in a later Java version
                }
            }
        }

        Except all(Class<?> c)
        {
            put(c, Kind.ALL, Collections.emptySet());
            return names -> entries.put(Type.getInternalName(c), new Entry(c, Kind.ALL, ImmutableSet.copyOf(names)));
        }

        /** Only the named members, all overloads; {@code "<init>"} names the constructors. */
        void members(Class<?> c, String... names)
        {
            put(c, Kind.MEMBERS, ImmutableSet.copyOf(names));
        }

        void members(Class<?> c, Set<String> names)
        {
            put(c, Kind.MEMBERS, names);
        }

        /** The type may be named, cast to, caught and used in descriptors, but its own members cannot be used. */
        void typeOnly(Class<?>... classes)
        {
            for (Class<?> c : classes)
                put(c, Kind.TYPE_ONLY, Collections.emptySet());
        }

        Map<String, Entry> build()
        {
            return ImmutableMap.copyOf(entries);
        }
    }

    @FunctionalInterface
    private interface Except
    {
        void except(String... names);
    }
}
