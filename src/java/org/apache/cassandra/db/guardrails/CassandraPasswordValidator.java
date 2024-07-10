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

package org.apache.cassandra.db.guardrails;

import java.io.IOException;
import java.io.RandomAccessFile; // checkstyle: permit this import
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.passay.CharacterCharacteristicsRule;
import org.passay.CharacterData;
import org.passay.CharacterRule;
import org.passay.CyrillicCharacterData;
import org.passay.CyrillicModernCharacterData;
import org.passay.CyrillicModernSequenceData;
import org.passay.CyrillicSequenceData;
import org.passay.CzechCharacterData;
import org.passay.CzechSequenceData;
import org.passay.DictionaryRule;
import org.passay.EnglishCharacterData;
import org.passay.EnglishSequenceData;
import org.passay.GermanCharacterData;
import org.passay.GermanSequenceData;
import org.passay.IllegalSequenceRule;
import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.PasswordValidator;
import org.passay.PolishCharacterData;
import org.passay.PolishSequenceData;
import org.passay.Rule;
import org.passay.RuleResult;
import org.passay.RuleResultDetail;
import org.passay.RuleResultMetadata;
import org.passay.SequenceData;
import org.passay.WhitespaceRule;
import org.passay.dictionary.FileWordList;
import org.passay.dictionary.WordListDictionary;

import static java.util.Optional.empty;
import static org.passay.EnglishCharacterData.Digit;

/**
 * The figures for each respective password characteristic and rule holds for the default configuration.
 * <p>
 * This password validator acts around 4 password characteristics which are:
 * <ul>
 *     <li>minimum number of lower-case characters (at least 2 to not emit warning, at least 1 to not emit failure)</li>
 *     <li>minimum number of upper-case characters (at least 2 to not emit warning, at least 1 to not emit failure)</li>
 *     <li>minimum number of special characters (at least 2 to not emit warning, at least 1 to not emit failure)</li>
 *     <li>minimum number of digits  (at least 2 to not emit warning, at least 1 to not emit failure)</li>
 * </ul>
 * To not emit warning, all characteristics have to be satisfied. A warning is emitted when 3 out of 4 are satistifed,
 * and it emits a failure when 2 (or less) out of 4 are satisfied.
 * <p>
 * Additionally, this validator uses these password rules which can not be violated in order to have a chance
 * to consider a password to be valid (in connection with above characteristics)
 * <ul>
 *     <li>a password has to be at least 12 characters long to not emit a warning and at least 8 characters
 *     long to not emit a failure</li>
 *     <li>a password can not contain whitespace characters</li>
 *     <li>a password can not contain illegal sequences</li>
 * </ul>
 * <p>
 * The default illegal-sequence length is 5 and its allowed minimum is 3.
 * <p>
 * Only some characters are contributing to the password characteristics above when it comes to
 * upper-cased and lower-cased ones. All other (unicode) characters do not contribute to these characteristics,
 * but they do contribute to the length rule.
 * <p>
 * For example, let's have this password: {@code 山13g.^Tg$棚Sd}. This password satisfies all 4 password characteristics
 * above:
 * <ul>
 *     <li>2 specials (contains 3)</li>
 *     <li>2 upper chars (contains 2)</li>
 *     <li>2 lower chars (contains 3)</li>
 *     <li>2 digits (contains 2)</li>
 * </ul>
 * Together 10 characters. That would not be enough but there are two more: {@code 山} and {@code 棚}
 * which is together 12. Not all (unicode) characters are possible to be categorized to upper-case and lower-case
 * categories so these characters do not contribute to these password characteristics but only to the length rule.
 * <p>
 * The characters which do contribute to lower-case and upper-case characteristics are:
 * <ul>
 *     <li>{@link EnglishCharacterData}</li>
 *     <li>{@link CyrillicCharacterData}</li>
 *     <li>{@link CyrillicModernCharacterData}</li>
 *     <li>{@link GermanCharacterData}</li>
 *     <li>{@link PolishCharacterData}</li>
 *     <li>{@link CzechCharacterData}</li>
 * </ul>
 * <p>
 * The characters which do contribute to {@link IllegalSequenceRule} rule are:
 * <ul>
 *     <li>{@link EnglishSequenceData#Alphabetical}</li>
 *     <li>{@link EnglishSequenceData#Numerical}</li>
 *     <li>{@link EnglishSequenceData#USQwerty}</li>
 *     <li>{@link CyrillicSequenceData#Alphabetical}</li>
 *     <li>{@link CyrillicModernSequenceData#Alphabetical}</li>
 *     <li>{@link CzechSequenceData#Alphabetical}</li>
 *     <li>{@link GermanSequenceData#Alphabetical}</li>
 *     <li>{@link PolishSequenceData#Alphabetical}</li>
 * </ul>
 * </p>
 * Passwords consisting of all characters belonging to unsupported character sets will be rejected and such
 * passwords will not be validated. For example, this password will be rejected: {@code 卡桑德拉-卡桑德拉山山羊棚}.
 * <p>
 * The password validator acts on two levels when used in a guardrail, executed in this order:
 * <ol>
 *    <li>tests if a password does not violate a failure threshold, if it does, failure is emitted and query failed</li>
 *    <li>tests if a password does not violate a warning threshold, if it does, warning is emitted</li>
 * </ol>
 * <p>
 * Passwords will be searched against a dictionary if "dictionary" configuration property is specified pointing to
 * a dictionary to read passwords from. This dictionary is all cached in a memory. Dictionary lookup is done
 * only upon {@link #shouldFail(String, boolean)} method call.
 * <p>
 *
 * @see CharacterCharacteristicsRule
 * @see CassandraPasswordConfiguration
 * @see WhitespaceRule
 * @see LengthRule
 * @see IllegalSequenceRule
 * @see ValueValidator#shouldWarn(Object, boolean)
 * @see ValueValidator#shouldFail(Object, boolean)
 */
public class CassandraPasswordValidator extends ValueValidator<String> implements PasswordDictionaryAware<CassandraPasswordConfiguration>
{
    private static final RuleResult VALID = new RuleResult(true);

    protected final PasswordValidator warnValidator;
    protected final PasswordValidator failValidator;

    protected final CassandraPasswordConfiguration configuration;
    private final UnsupportedCharsetRule unsupportedCharsetRule = new UnsupportedCharsetRule();
    private final DictionaryRule dictionaryRule;
    private final boolean provideDetailedMessages;

    public CassandraPasswordValidator(CustomGuardrailConfig config)
    {
        super(config);
        configuration = new CassandraPasswordConfiguration(config);
        provideDetailedMessages = configuration.detailedMessages;

        dictionaryRule = initializeDictionaryRule(configuration);

        warnValidator = new PasswordValidator(getRules(configuration.lengthWarn,
                                                       configuration.maxLength,
                                                       configuration.characteristicsWarn,
                                                       configuration.illegalSequenceLength,
                                                       getCharacterValidationRules(configuration.upperCaseWarn,
                                                                                   configuration.lowerCaseWarn,
                                                                                   configuration.digitsWarn,
                                                                                   configuration.specialsWarn)));
        failValidator = new PasswordValidator(getRules(configuration.lengthFail,
                                                       configuration.maxLength,
                                                       configuration.characteristicsFail,
                                                       configuration.illegalSequenceLength,
                                                       getCharacterValidationRules(configuration.upperCaseFail,
                                                                                   configuration.lowerCaseFail,
                                                                                   configuration.digitsFail,
                                                                                   configuration.specialsFail)));
    }

    @Nonnull
    @Override
    public CustomGuardrailConfig getParameters()
    {
        return configuration.asCustomGuardrailConfig();
    }

    @Override
    public Optional<ValidationViolation> shouldWarn(String password, boolean calledBySuperuser)
    {
        return executeValidation(warnValidator, password, calledBySuperuser, true);
    }

    @Override
    public Optional<ValidationViolation> shouldFail(String password, boolean calledBySuperUser)
    {
        return executeValidation(failValidator, password, calledBySuperUser, false);
    }

    private Optional<ValidationViolation> executeValidation(PasswordValidator validator,
                                                            String passwordToValidate,
                                                            boolean calledBySuperUser,
                                                            boolean toWarn)
    {
        PasswordData passwordData = new PasswordData(passwordToValidate);
        if (!unsupportedCharsetRule.validate(passwordData).isValid())
        {
            String message = (calledBySuperUser || provideDetailedMessages) ? "Unsupported language / character set for password validator"
                                                                            : "Password complexity policy not met.";
            return Optional.of(new ValidationViolation(message, UnsupportedCharsetRule.ERROR_CODE));
        }
        else
        {
            if (!toWarn && configuration.dictionary != null) // for shouldFail
            {
                RuleResult result = foundInDictionary(passwordData);
                if (!result.isValid())
                    return Optional.of(getValidationMessage(calledBySuperUser, validator, false, result));
            }
            RuleResult result = validator.validate(passwordData);
            return result.isValid() ? empty() : Optional.of(getValidationMessage(calledBySuperUser, validator, toWarn, result));
        }
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        configuration.validateParameters();
    }

    protected List<CharacterRule> getCharacterValidationRules(int upper, int lower, int digits, int special)
    {
        return Arrays.asList(new CharacterRule(new CustomUpperCaseCharacterData(), upper),
                             new CharacterRule(new CustomLowerCaseCharacterData(), lower),
                             new CharacterRule(Digit, digits),
                             new CharacterRule(specialCharacters, special));
    }

    protected List<Rule> getRules(int minLength,
                                  int maxLength,
                                  int characteristics,
                                  int illegalSequenceLength,
                                  List<CharacterRule> characterRules)
    {
        List<Rule> rules = new ArrayList<>();

        rules.add(new LengthRule(minLength, maxLength));

        CharacterCharacteristicsRule characteristicsRule = new CharacterCharacteristicsRule();
        characteristicsRule.setNumberOfCharacteristics(characteristics + 1);
        characteristicsRule.getRules().addAll(characterRules);
        rules.add(characteristicsRule);

        rules.add(new WhitespaceRule());

        for (SequenceData sequenceData : getSequenceData())
            rules.add(new IllegalSequenceRule(sequenceData, illegalSequenceLength, false));

        return rules;
    }

    private ValidationViolation getValidationMessage(boolean calledBySuperuser,
                                                     PasswordValidator validator,
                                                     boolean toWarn,
                                                     RuleResult result)
    {
        Set<String> errorCodes = new HashSet<>();
        for (RuleResultDetail ruleResultDetail : result.getDetails())
            errorCodes.add(ruleResultDetail.getErrorCode());

        String redactedMessage = errorCodes.toString();

        if (calledBySuperuser || provideDetailedMessages)
        {
            String type = toWarn ? "warning" : "error";
            StringBuilder sb = new StringBuilder();
            sb.append("Password was")
              .append(toWarn ? " set, however it might not be strong enough according to the " +
                               "configured password strength policy. "
                             : " not set as it violated configured password strength policy. ")
              .append("To fix this ")
              .append(type)
              .append(", the following has to be resolved: ");

            for (String message : validator.getMessages(result))
                sb.append(message).append(' ');

            sb.append("You may also use 'GENERATED PASSWORD' upon role creation or alteration.");

            String message = sb.toString();

            return new ValidationViolation(message, redactedMessage);
        }
        else
        {
            if (toWarn)
            {
                return new ValidationViolation("Password was set, however it might not be strong enough " +
                                               "according to the configured password strength policy.",
                                               redactedMessage);
            }
            else
            {
                return new ValidationViolation("Password was not set as it violated configured password " +
                                               "strength policy. You may also use 'GENERATED PASSWORD' upon role " +
                                               "creation or alteration.",
                                               redactedMessage);
            }
        }
    }

    @Override
    public RuleResult foundInDictionary(String password)
    {
        if (dictionaryRule == null)
            return VALID;

        return dictionaryRule.validate(new PasswordData(password));
    }

    @Override
    public RuleResult foundInDictionary(PasswordData passwordData)
    {
        if (dictionaryRule == null)
            return VALID;

        return dictionaryRule.validate(passwordData);
    }

    protected static class CustomLowerCaseCharacterData implements CharacterData
    {
        @Override
        public String getErrorCode()
        {
            return "INSUFFICIENT_LOWERCASE";
        }

        @Override
        public String getCharacters()
        {
            return EnglishCharacterData.LowerCase.getCharacters() +
                   CyrillicCharacterData.LowerCase.getCharacters() +
                   CyrillicModernCharacterData.LowerCase.getCharacters() +
                   CzechCharacterData.LowerCase.getCharacters() +
                   GermanCharacterData.LowerCase.getCharacters() +
                   PolishCharacterData.LowerCase.getCharacters();
        }
    }

    protected static class CustomUpperCaseCharacterData implements CharacterData
    {
        @Override
        public String getErrorCode()
        {
            return "INSUFFICIENT_UPPERCASE";
        }

        @Override
        public String getCharacters()
        {
            return EnglishCharacterData.UpperCase.getCharacters() +
                   CyrillicCharacterData.UpperCase.getCharacters() +
                   CyrillicModernCharacterData.UpperCase.getCharacters() +
                   CzechCharacterData.UpperCase.getCharacters() +
                   GermanCharacterData.UpperCase.getCharacters() +
                   PolishCharacterData.UpperCase.getCharacters();
        }
    }

    protected static final CharacterData specialCharacters = new CharacterData()
    {
        @Override
        public String getErrorCode()
        {
            return "INSUFFICIENT_SPECIAL";
        }

        @Override
        public String getCharacters()
        {
            return "!\"#$%&()*+,-./:;<=>?@[\\]^_`{|}~";
        }
    };

    protected List<SequenceData> getSequenceData()
    {
        return List.of(EnglishSequenceData.Alphabetical,
                       EnglishSequenceData.Numerical,
                       EnglishSequenceData.USQwerty,
                       CyrillicSequenceData.Alphabetical,
                       CyrillicModernSequenceData.Alphabetical,
                       CzechSequenceData.Alphabetical,
                       GermanSequenceData.Alphabetical,
                       PolishSequenceData.Alphabetical);
    }

    public static class UnsupportedCharsetRule implements Rule
    {
        private static final char[] supportedAlphabeticChars = UnsupportedCharsetRule.supportedChars(true);
        private static final char[] allSupportedCharsChars = UnsupportedCharsetRule.supportedChars(false);

        public static final String ERROR_CODE = "UNSUPPORTED_CHARSET";

        @Override
        public RuleResult validate(final PasswordData passwordData)
        {
            final String text = passwordData.getPassword();

            final RuleResult result = new RuleResult();
            if (text.isEmpty())
                return result;

            int unsupportedChars = 0;
            int supportedNonAlphabeticChars = 0;
            for (char c : text.toCharArray())
            {
                if (Arrays.binarySearch(allSupportedCharsChars, c) < 0)
                    unsupportedChars++;
                else if (Arrays.binarySearch(supportedAlphabeticChars, c) < 0)
                    supportedNonAlphabeticChars++;
            }

            if (unsupportedChars > 0)
            {
                if (unsupportedChars + supportedNonAlphabeticChars == text.length())
                {
                    result.setValid(false);
                    result.addError(ERROR_CODE, Map.of());
                }

                result.setMetadata(new RuleResultMetadata(RuleResultMetadata.CountCategory.Illegal, unsupportedChars));
            }

            return result;
        }

        private static char[] supportedChars(boolean onlyAlphabetic)
        {
            char[] charArray = getChars(onlyAlphabetic);

            Set<Character> charactersSet = new HashSet<>();

            // filter out the duplicates
            for (int i = 0; i < charArray.length; i++)
                charactersSet.add(charArray[i]);

            char[] result = new char[charactersSet.size()];

            Iterator<Character> characterIterator = charactersSet.iterator();
            int i = 0;
            while (characterIterator.hasNext())
                result[i++] = characterIterator.next();

            Arrays.sort(result); // important for binary search
            return result;
        }

        private static char[] getChars(boolean onlyAlphabetic)
        {
            if (onlyAlphabetic)
                return (new CassandraPasswordValidator.CustomUpperCaseCharacterData().getCharacters() +
                        new CassandraPasswordValidator.CustomLowerCaseCharacterData().getCharacters()).toCharArray();
            else
                return (new CassandraPasswordValidator.CustomUpperCaseCharacterData().getCharacters() +
                        new CassandraPasswordValidator.CustomLowerCaseCharacterData().getCharacters() +
                        CassandraPasswordValidator.specialCharacters.getCharacters() +
                        "0123456789").toCharArray();
        }
    }

    @Override
    public DictionaryRule initializeDictionaryRule(CassandraPasswordConfiguration configuration)
    {
        if (configuration.dictionary == null)
            return null;

        try
        {
            RandomAccessFile raf = new RandomAccessFile(configuration.dictionary, "r");
            FileWordList fileWordList = new FileWordList(raf, true, 100);
            WordListDictionary wordListDictionary = new WordListDictionary(fileWordList);
            return new DictionaryRule(wordListDictionary);
        }
        catch (IllegalArgumentException ex)
        {
            // improve message a little bit
            if ("File is not sorted correctly for this comparator".equals(ex.getMessage()))
                throw new ConfigurationException("Dictionary file " + configuration.dictionary + " is not correctly " +
                                                 "sorted for case-sensitive comparator according to String's " +
                                                 "compareTo contract.");
            else
                throw new ConfigurationException(ex.getMessage());
        }
        catch (IOException ex)
        {
            throw new ConfigurationException(ex.getMessage());
        }
    }
}
