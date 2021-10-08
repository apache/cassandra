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

package org.apache.cassandra.guardrails;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;

/**
 * General class defining a given guardrail (that guards against some particular usage/condition).
 *
 * <p>Some guardrails only emit warnings when triggered, while other fail the query that trigger them. Some may do one
 * or the other based on specific threshold.
 *
 * <p>Note that all the defined class support live updates, which is why each guardrail class ctor takes suppliers of
 * the condition the guardrail acts on rather than the condition itself. Which does imply that said suppliers should
 * be fast and non-blocking to avoid surprises. Note that this does not mean live updates are exposed to the user,
 * just that the implementation is up to it if we ever want to expose it.
 */
public abstract class DefaultGuardrail implements Guardrail
{
     private static final NoSpamLogger logger = NoSpamLogger.getLogger(LoggerFactory.getLogger(DefaultGuardrail.class),
                                                                       10, TimeUnit.MINUTES);
     private static final String REDACTED = "<redacted>";

     /** A name identifying the guardrail (mainly for shipping with Insights events). */
     public final String name;

     /** whether to throw {@link InvalidRequestException} on {@link this#fail(String)} */
     private boolean throwOnFailure = true;

     /** minimum logging and triggering interval to avoid spamming downstream*/
     private long minNotifyIntervalInMs = 0;

     /** time of last warning in milliseconds */
     private volatile long lastWarnInMs = 0;

     /** time of last failure in milliseconds */
     private volatile long lastFailInMs = 0;

     DefaultGuardrail(String name)
     {
          this.name = name;
     }

     protected void warn(String fullMessage, String redactedMessage)
     {
          if (skipNotifying(true))
               return;

          logger.warn(fullMessage);
          // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
          // (the internal "state" will be null)
          ClientWarn.instance.warn(fullMessage);
          // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
          Tracing.trace(fullMessage);
          for (Guardrails.Listener listener : Guardrails.listeners)
               listener.onWarningTriggered(name, redactedMessage);
     }

     protected void warn(String fullMessage)
     {
          warn(fullMessage, fullMessage);
     }

     protected void fail(String fullMessage, String redactedMessage)
     {
          if (!skipNotifying(false))
          {
               logger.error(fullMessage);
               // Tracing will ignore the message if we're not running tracing on the current thread.
               Tracing.trace(fullMessage);
               for (Guardrails.Listener listener : Guardrails.listeners)
                    listener.onFailureTriggered(name, redactedMessage);
          }

          if (throwOnFailure)
               throw new InvalidRequestException(fullMessage);
     }

     protected void fail(String message)
     {
          fail(message, message);
     }

     /**
      * do no throw {@link InvalidRequestException} if guardrail failure is triggered.
      *
      * Note: this method is not thread safe and should only be used during guardrail initialization
      *
      * @return current guardrail
      */
     @Override
     public DefaultGuardrail setNoExceptionOnFailure()
     {
          this.throwOnFailure = false;
          return this;
     }

     /**
      * Note: this method is not thread safe and should only be used during guardrail initialization
      *
      * @param minNotifyIntervalInMs frequency of logging and triggering listener to avoid spamming,
      * default 0 means always log and trigger listeners.
      * @return current guardrail
      */
     @Override
     public DefaultGuardrail setMinNotifyIntervalInMs(long minNotifyIntervalInMs)
     {
          assert minNotifyIntervalInMs >= 0;
          this.minNotifyIntervalInMs = minNotifyIntervalInMs;
          return this;
     }

     /**
      * reset last notify time to make sure it will notify downstream when {@link this#warn(String, String)}
      * or {@link this#fail(String)} is called next time.
      */
     @VisibleForTesting
     public void resetLastNotifyTime()
     {
          lastFailInMs = 0;
          lastWarnInMs = 0;
     }

     /**
      * @return true if guardrail should not log message and trigger listeners; otherwise, update lastWarnInMs or
      * lastFailInMs respectively.
      */
     private boolean skipNotifying(boolean isWarn)
     {
          if (minNotifyIntervalInMs == 0)
               return false;

          long nowInMs = System.currentTimeMillis();
          long timeElapsedInMs = nowInMs - (isWarn ? lastWarnInMs : lastFailInMs);

          boolean skip = timeElapsedInMs < minNotifyIntervalInMs;

          if (!skip)
          {
               if (isWarn)
                    lastWarnInMs = nowInMs;
               else
                    lastFailInMs = nowInMs;
          }

          return skip;
     }

     @Override
     public boolean enabled(@Nullable QueryState state)
     {
         return Guardrails.ready() && (state == null || state.isOrdinaryUser());
     }

     /**
      * A guardrail based on numeric threshold(s).
      *
      * <p>A {@link DefaultThreshold} guardrail defines (up to) 2 threshold, one at which a warning is issued, and a higher one
      * at which a failure is triggered. Only one of those threshold can be activated if desired.
      *
      * <p>This guardrail only handles guarding positive values.
      */
     public static class DefaultThreshold extends DefaultGuardrail implements Threshold
     {
          /**
           * A {@link DefaultThreshold} with both failure and warning thresholds disabled, so that cannot ever be triggered.
           */
          public static final DefaultThreshold NEVER_TRIGGERED = new DefaultThreshold("never_triggered", () -> -1L, () -> -1L, null);

          private final LongSupplier warnThreshold;
          private final LongSupplier failThreshold;
          protected final ErrorMessageProvider errorMessageProvider;

          protected DefaultThreshold(String name,
                    LongSupplier warnThreshold,
                    LongSupplier failThreshold,
                    ErrorMessageProvider errorMessageProvider)
          {
               super(name);
               this.warnThreshold = warnThreshold;
               this.failThreshold = failThreshold;
               this.errorMessageProvider = errorMessageProvider;
          }

          protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
          {
               return errorMessageProvider.createMessage(isWarning,
                                                         what,
                                                         value,
                                                         thresholdValue);
          }

          protected String redactedErrMsg(boolean isWarning, long value, long thresholdValue)
          {
               return errorMessageProvider.createMessage(isWarning,
                                                         REDACTED,
                                                         value,
                                                         thresholdValue);
          }

          private long failValue()
          {
               long failValue = failThreshold.getAsLong();
               return failValue < 0 ? Long.MAX_VALUE : failValue;
          }

          private long warnValue()
          {
               long warnValue = warnThreshold.getAsLong();
               return warnValue < 0 ? Long.MAX_VALUE : warnValue;
          }

          @Override
          public boolean enabled(@Nullable QueryState state)
          {
               return super.enabled(state) && (failThreshold.getAsLong() >= 0 || warnThreshold.getAsLong() >= 0);
          }

          @Override
          public boolean triggersOn(long value, @Nullable QueryState state)
          {
               return enabled(state) && (value > Math.min(failValue(), warnValue()));
          }

          @Override
          public void guard(long value, String what, boolean containsUserData, @Nullable QueryState state)
          {
               if (!enabled(state))
                    return;

               long failValue = failValue();
               if (value > failValue)
               {
                    triggerFail(value, failValue, what, containsUserData);
                    return;
               }

               long warnValue = warnValue();
               if (value > warnValue)
                    triggerWarn(value, warnValue, what, containsUserData);
          }

          private void triggerFail(long value, long failValue, String what, boolean containsUserData)
          {
               String fullMsg = errMsg(false, what, value, failValue);
               fail(fullMsg, containsUserData ? redactedErrMsg(false, value, failValue) : fullMsg);
          }

          private void triggerWarn(long value, long warnValue, String what, boolean containsUserData)
          {
               String fullMsg = errMsg(true, what, value, warnValue);
               warn(fullMsg, containsUserData ? redactedErrMsg(true, value, warnValue) : fullMsg);
          }

          @Override
          public DefaultGuardedCounter newCounter(Supplier<String> whatFct, boolean containsUserData, @Nullable QueryState state)
          {
               DefaultThreshold threshold = enabled(state) ? this : NEVER_TRIGGERED;
               return threshold.new DefaultGuardedCounter(whatFct, containsUserData);
          }

          /**
           * A facility for when the value to guard is built incrementally, but we want to trigger failures as soon
           * as the failure threshold is reached, but only trigger the warning on the final value (and so only if the
           * failure threshold hasn't also been reached).
           * <p>
           * Note that instances are neither thread safe nor reusable.
           */
          public class DefaultGuardedCounter implements Threshold.GuardedCounter
          {
               private final long warnValue;
               private final long failValue;
               private final Supplier<String> what;
               private final boolean containsUserData;

               private long accumulated;

               private DefaultGuardedCounter(Supplier<String> what, boolean containsUserData)
               {
                    // We capture the warn and fail value at the time of the counter construction to ensure we use
                    // stable value during the counter lifetime (and reading a final field is possibly at tad faster).
                    this.warnValue = warnValue();
                    this.failValue = failValue();
                    this.what = what;
                    this.containsUserData = containsUserData;
               }

               /**
                * The currently accumulated value of the counter.
                */
               public long get()
               {
                    return accumulated;
               }

               /**
                * Add the provided increment to the counter, triggering a failure if the counter after this addition
                * crosses the failure threshold.
                *
                * @param increment the increment to add.
                */
               public void add(long increment)
               {
                    accumulated += increment;
                    if (accumulated > failValue)
                         triggerFail(accumulated, failValue, what.get(), containsUserData);
               }

               /**
                * Trigger the warn if the currently accumulated counter value crosses warning threshold and the failure
                * has not been triggered yet.
                * <p>
                * This is generally meant to be called when the guarded value is complete.
                *
                * @return {@code true} and trigger a warning if the current counter value is greater than the warning
                * threshold and less than or equal to the failure threshold, {@code false} otherwise.
                */
               public boolean checkAndTriggerWarning()
               {
                    if (accumulated > warnValue && accumulated <= failValue)
                    {
                         triggerWarn(accumulated, warnValue, what.get(), containsUserData);
                         return true;
                    }
                    return false;
               }
          }
     }

     /**
      * A guardrail that completely disables the use of a particular feature.
      *
      * <p>Note that this guardrail only triggers failures (if the feature is disabled) so is only meant for
      * query-based guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail
      * that breaks compaction for instance).
      */
     public static class DefaultDisableFlag extends DefaultGuardrail implements DisableFlag
     {
          private final BooleanSupplier disabled;
          private final String what;

          /**
           * Creates a new {@link DefaultDisableFlag} guardrail.
           * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
           * @param disabled a supplier of boolean indicating whether the feature guarded by this guardrail must be
           * disabled.
           * @param what the feature that is guarded by this guardrail (for reporting in error messages),
           * {@link #ensureEnabled(String, QueryState)} can specify a different {@code what}.
           */
          public DefaultDisableFlag(String name, BooleanSupplier disabled, String what)
          {
               super(name);
               this.disabled = disabled;
               this.what = what;
          }

          /**
           * Triggers a failure if this guardrail is disabled.
           *
           * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
           * allowed.
           *
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * A {@code null} value means that the check should be done regardless of the query.
           */
          public void ensureEnabled(@Nullable QueryState state)
          {
               ensureEnabled(what, state);
          }

          /**
           * Triggers a failure if this guardrail is disabled.
           *
           * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
           * allowed.
           *
           * @param what the feature that is guarded by this guardrail (for reporting in error messages).
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * A {@code null} value means that the check should be done regardless of the query.
           */
          public void ensureEnabled(String what, @Nullable QueryState state)
          {
               if (enabled(state) && disabled.getAsBoolean())
                    fail(what + " is not allowed");
          }
     }

     /**
      * Base class for guardrail that are triggered based on a set of values.
      *
      * @param <T> the type of the values that trigger the guardrail.
      */
     private static abstract class MulitpleValuesBasedGuardrail<T> extends DefaultGuardrail
     {
          /*
           * Implementation note: as mentioned in the class Javadoc and for consistency with the other Guardrail
           * implementation of this class (and to generally avoid surprises), this implementation ensures that live
           * changes to the underlying guardrail setting gets reflected. This is the reason for the relative
           * "complexity" of this class.
           */

          private final Supplier<Set<String>> rawSupplier;
          private final Function<String, T> parser;
          protected final String what;

          private volatile Set<T> cachedValues;
          private volatile Set<String> cachedRaw;

          protected MulitpleValuesBasedGuardrail(
                  String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
          {
               super(name);
               this.rawSupplier = disallowedRaw;
               this.parser = parser;
               this.what = what;

               if (Guardrails.ready())
                    ensureUpToDate();
          }

          protected void ensureUpToDate()
          {
               Set<String> current = rawSupplier.get();
               // Same as below, this shouldn't happen if settings have been properly sanitized, but throw a meaningful
               // error if there is a bug.
               if (current == null)
                    throw new RuntimeException(format("Invalid null setting for guardrail on %s. This should not have"
                                                      + " happened, please contact DataStax support", what));

               // Note that this will fail on first call (as we want), as currentRaw will be null but not current
               if (current == cachedRaw)
                    return;

               try
               {
                    // Setting cachedAllowed first so that on a parse failure we leave everything as it previously
                    // was (not that we'd expect that matter but ...).
                    cachedValues = current.stream()
                                          .map(parser)
                                          .collect(Collectors.toSet());
                    cachedRaw = current;
               }
               catch (Exception e)
               {
                    // This catch parsing errors. Hopefully, this shouldn't happen as guardrails settings should have
                    // been sanitized, but ...
                    // Also, we catch the exception to add a meaningful error message, but rethrow otherwise: if a
                    // guardrail has been configured, it's presumably to avoid bad things to go in, so we don't wan to
                    // take the risk of letting it go if there is a misconfiguration.
                    throw new RuntimeException(format("Error parsing configured setting for guardrail on %s. This "
                                                      + "should not have happened, please contact DataStax support."
                                                      + "The failing setting is %s", what, current), e);
               }
          }

          protected Set<T> matchingValues(Set<T> values) {
               return Sets.intersection(values, cachedValues);
          }

          protected String triggerValuesString()
          {
              return cachedRaw.toString();
          }

          /**
           * Checks whether the provided value would trigger this guardrail.
           *
           * <p>This method is optional (does not have to be called) but can be used in the case some of the arguments
           * to the actual guardrail method is expensive to build to save doing so in the common case (of the
           * guardrail not being triggered).
           *
           * @param value the value to test.
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * @return {@code true} if {@code value} is not allowed by this guardrail,
           * {@code false otherwise}.
           */
          public boolean triggersOn(T value, @Nullable QueryState state)
          {
               if (!enabled(state))
                    return false;

               ensureUpToDate();
               return cachedValues.contains(value);
          }
     }


     /**
      * A guardrail that rejects the use of specific values.
      *
      * <p>Note that like {@link DefaultDisableFlag}, this guardrail only trigger failures and is thus only for query-based
      * guardrails.
      *
      * @param <T> the type of the values of which certain are disallowed.
      */
     public static class DefaultDisallowedValues<T> extends MulitpleValuesBasedGuardrail<T> implements DisallowedValues<T>
     {
          /**
           * Creates a new {@link DefaultDisallowedValues} guardrail.
           *
           * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
           * @param disallowedRaw a supplier of the values that are disallowed in raw (string) form. The set returned by
           *                      this supplier <b>must</b> be immutable (we don't use {@code ImmutableSet} because we
           *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
           *                      there would currently be annoying (because populated automatically by snakeYaml)).
           * @param parser a function to parse the value to disallow from string.
           * @param what what represents the value disallowed (for reporting in error messages).
           */
          public DefaultDisallowedValues(String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
          {
               super(name, disallowedRaw, parser, what);
          }

          /**
           * Triggers a failure if the provided value is disallowed by this guardrail.
           *
           * @param value the value to check.
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * A {@code null} value means that the check should be done regardless of the query.
           */
          public void ensureAllowed(T value, @Nullable QueryState state)
          {
               if (triggersOn(value, state))
                    fail(format("Provided value %s is not allowed for %s (disallowed values are: %s)",
                                value, what, triggerValuesString()));
          }

          /**
           * Triggers a failure if any of the provided values is disallowed by this guardrail.
           *
           * @param values the values to check.
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * A {@code null} value means that the check should be done regardless of the query.
           */
          public void ensureAllowed(Set<T> values, @Nullable QueryState state)
          {
               if (!enabled(state))
                    return;

               ensureUpToDate();

               Set<T> disallowed = matchingValues(values);
               if (!disallowed.isEmpty())
                    fail(format("Provided values %s are not allowed for %s (disallowed values are: %s)",
                                disallowed.stream().sorted().collect(Collectors.toList()), what, triggerValuesString()));
          }
     }

     /**
      *
      * A guardrail based on two predicates.
      *
      * <p>A {@link Predicates} guardrail defines (up to) 2 predicates, one at which a warning is issued, and another one
      * at which a failure is triggered. If failure is triggered, warning is skipped.
      *
      * @param <T> the type of the values to be tested against predicates.
      */
     public static class Predicates<T> extends DefaultGuardrail implements ValueBasedGuardrail<T>
     {
          private final Predicate<T> warnPredicate;
          private final Predicate<T> failurePredicate;
          private final MessageProvider<T> messageProvider;

          /**
           * Creates a new {@link Predicates} guardrail.
           *
           * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
           * @param warnPredicate a predicate that is used to check if given value should trigger a warning.
           * @param failurePredicate a predicate that is used to check if given value should trigger a failure.
           * @param messageProvider a function to generate the warning or error message if the guardrail is triggered
           */
          public Predicates(String name, Predicate<T> warnPredicate, Predicate<T> failurePredicate, MessageProvider<T> messageProvider)
          {
               super(name);
               this.warnPredicate = warnPredicate;
               this.failurePredicate = failurePredicate;
               this.messageProvider = messageProvider;
          }

          /**
           * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
           *
           * @param value the value to check.
           * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
           * A {@code null} value means that the check should be done regardless of the query.
           */
          public void guard(T value, @Nullable QueryState state)
          {
               if (!enabled(state))
                    return;

               if (failurePredicate.test(value))
               {
                    fail(messageProvider.createMessage(false, value));
               }
               else if (warnPredicate.test(value))
               {
                    warn(messageProvider.createMessage(true, value));
               }
          }
     }

    /**
     * A guardrail that warns but ignore some specific values.
     *
     * @param <T> the type of the values of which certain are ignored.
     */
    public static class DefaultIgnoredValues<T> extends MulitpleValuesBasedGuardrail<T> implements IgnoredValues<T>
    {
        /**
         * Creates a new {@link DefaultIgnoredValues} guardrail.
         *
         * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param ignoredRaw a supplier of the values that are ignored in raw (string) form. The set returned by
         *                      this supplier <b>must</b> be immutable (we don't use {@code ImmutableSet} because we
         *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
         *                      there would currently be annoying (because populated automatically by snakeYaml)).
         * @param parser a function to parse the value to ignore from string.
         * @param what what represents the value ignored (for reporting in error messages).
         */
        public DefaultIgnoredValues(String name, Supplier<Set<String>> ignoredRaw, Function<String, T> parser, String what)
        {
            super(name, ignoredRaw, parser, what);
        }

         /**
          * Checks for ignored values by this guardrail and when it found some, log a warning and trigger an action
          * to ignore them.
          *
          * @param values the values to check.
          * @param ignoreAction an action called on the subset of {@code values} that should be ignored. This action
          * should do whatever is necessary to make sure the value is ignored.
          * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
          * A {@code null} value means that the check should be done regardless of the query.
          */
        public void maybeIgnoreAndWarn(Set<T> values, Consumer<T> ignoreAction, @Nullable QueryState state)
        {
             if (!enabled(state))
                  return;

             ensureUpToDate();

             Set<T> toIgnore = matchingValues(values);
             if (toIgnore.isEmpty())
                  return;

             warn(format("Ignoring provided values %s as they are not supported for %s (ignored values are: %s)",
                         toIgnore.stream().sorted().collect(Collectors.toList()), what, triggerValuesString()));
             for (T value : toIgnore)
                  ignoreAction.accept(value);
        }
    }
}
