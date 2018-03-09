package org.apache.cassandra.index.sasi.analyzer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;

/**
 * This class implements Word Break rules from the Unicode Text Segmentation 
 * algorithm, as specified in 
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a>. 
 * <p/>
 * Tokens produced are of the following types:
 * <ul>
 *   <li>&lt;ALPHANUM&gt;: A sequence of alphabetic and numeric characters</li>
 *   <li>&lt;NUM&gt;: A number</li>
 *   <li>&lt;SOUTHEAST_ASIAN&gt;: A sequence of characters from South and Southeast
 *       Asian languages, including Thai, Lao, Myanmar, and Khmer</li>
 *   <li>&lt;IDEOGRAPHIC&gt;: A single CJKV ideographic character</li>
 *   <li>&lt;HIRAGANA&gt;: A single hiragana character</li>
 *   <li>&lt;KATAKANA&gt;: A sequence of katakana characters</li>
 *   <li>&lt;HANGUL&gt;: A sequence of Hangul characters</li>
 * </ul>
 */
%%

%unicode 6.3
%integer
%final
%public
%class StandardTokenizerImpl
%implements StandardTokenizerInterface
%function getNextToken
%char
%buffer 4096

%include SUPPLEMENTARY.jflex-macro
ALetter           = (\p{WB:ALetter}                                     | {ALetterSupp})
Format            = (\p{WB:Format}                                      | {FormatSupp})
Numeric           = ([\p{WB:Numeric}[\p{Blk:HalfAndFullForms}&&\p{Nd}]] | {NumericSupp})
Extend            = (\p{WB:Extend}                                      | {ExtendSupp})
Katakana          = (\p{WB:Katakana}                                    | {KatakanaSupp})
MidLetter         = (\p{WB:MidLetter}                                   | {MidLetterSupp})
MidNum            = (\p{WB:MidNum}                                      | {MidNumSupp})
MidNumLet         = (\p{WB:MidNumLet}                                   | {MidNumLetSupp})
ExtendNumLet      = (\p{WB:ExtendNumLet}                                | {ExtendNumLetSupp})
ComplexContext    = (\p{LB:Complex_Context}                             | {ComplexContextSupp})
Han               = (\p{Script:Han}                                     | {HanSupp})
Hiragana          = (\p{Script:Hiragana}                                | {HiraganaSupp})
SingleQuote       = (\p{WB:Single_Quote}                                | {SingleQuoteSupp})
DoubleQuote       = (\p{WB:Double_Quote}                                | {DoubleQuoteSupp})
HebrewLetter      = (\p{WB:Hebrew_Letter}                               | {HebrewLetterSupp})
RegionalIndicator = (\p{WB:Regional_Indicator}                          | {RegionalIndicatorSupp})
HebrewOrALetter   = ({HebrewLetter} | {ALetter})

// UAX#29 WB4. X (Extend | Format)* --> X
//
HangulEx            = [\p{Script:Hangul}&&[\p{WB:ALetter}\p{WB:Hebrew_Letter}]] ({Format} | {Extend})*
HebrewOrALetterEx   = {HebrewOrALetter}                                         ({Format} | {Extend})*
NumericEx           = {Numeric}                                                 ({Format} | {Extend})*
KatakanaEx          = {Katakana}                                                ({Format} | {Extend})* 
MidLetterEx         = ({MidLetter} | {MidNumLet} | {SingleQuote})               ({Format} | {Extend})* 
MidNumericEx        = ({MidNum} | {MidNumLet} | {SingleQuote})                  ({Format} | {Extend})*
ExtendNumLetEx      = {ExtendNumLet}                                            ({Format} | {Extend})*
HanEx               = {Han}                                                     ({Format} | {Extend})*
HiraganaEx          = {Hiragana}                                                ({Format} | {Extend})*
SingleQuoteEx       = {SingleQuote}                                             ({Format} | {Extend})*                                            
DoubleQuoteEx       = {DoubleQuote}                                             ({Format} | {Extend})*
HebrewLetterEx      = {HebrewLetter}                                            ({Format} | {Extend})*
RegionalIndicatorEx = {RegionalIndicator}                                       ({Format} | {Extend})*


%{
  /** Alphanumeric sequences */
  public static final int WORD_TYPE = StandardAnalyzer.TokenType.ALPHANUM.value;
  
  /** Numbers */
  public static final int NUMERIC_TYPE = StandardAnalyzer.TokenType.NUM.value;
  
  /**
   * Chars in class \p{Line_Break = Complex_Context} are from South East Asian
   * scripts (Thai, Lao, Myanmar, Khmer, etc.).  Sequences of these are kept 
   * together as as a single token rather than broken up, because the logic
   * required to break them at word boundaries is too complex for UAX#29.
   * <p>
   * See Unicode Line Breaking Algorithm: http://www.unicode.org/reports/tr14/#SA
   */
  public static final int SOUTH_EAST_ASIAN_TYPE = StandardAnalyzer.TokenType.SOUTHEAST_ASIAN.value;
  
  public static final int IDEOGRAPHIC_TYPE = StandardAnalyzer.TokenType.IDEOGRAPHIC.value;
  
  public static final int HIRAGANA_TYPE = StandardAnalyzer.TokenType.HIRAGANA.value;
  
  public static final int KATAKANA_TYPE = StandardAnalyzer.TokenType.KATAKANA.value;
  
  public static final int HANGUL_TYPE = StandardAnalyzer.TokenType.HANGUL.value;

  public final int yychar()
  {
    return yychar;
  }

  public String getText()
  {
    return String.valueOf(zzBuffer, zzStartRead, zzMarkedPos-zzStartRead);
  }

  public char[] getArray()
  {
    return Arrays.copyOfRange(zzBuffer, zzStartRead, zzMarkedPos);
  }

  public byte[] getBytes()
  {
    return getText().getBytes();
  }

%}

%%

// UAX#29 WB1.   sot   ÷
//        WB2.     ÷   eot
//
<<EOF>> { return StandardAnalyzer.TokenType.EOF.value; }

// UAX#29 WB8.   Numeric × Numeric
//        WB11.  Numeric (MidNum | MidNumLet | Single_Quote) × Numeric
//        WB12.  Numeric × (MidNum | MidNumLet | Single_Quote) Numeric
//        WB13a. (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana) 
//
{ExtendNumLetEx}* {NumericEx} ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx} )* {ExtendNumLetEx}* 
  { return NUMERIC_TYPE; }

// subset of the below for typing purposes only!
{HangulEx}+
  { return HANGUL_TYPE; }
  
{KatakanaEx}+
  { return KATAKANA_TYPE; }

// UAX#29 WB5.   (ALetter | Hebrew_Letter) × (ALetter | Hebrew_Letter)
//        WB6.   (ALetter | Hebrew_Letter) × (MidLetter | MidNumLet | Single_Quote) (ALetter | Hebrew_Letter)
//        WB7.   (ALetter | Hebrew_Letter) (MidLetter | MidNumLet | Single_Quote) × (ALetter | Hebrew_Letter)
//        WB7a.  Hebrew_Letter × Single_Quote
//        WB7b.  Hebrew_Letter × Double_Quote Hebrew_Letter
//        WB7c.  Hebrew_Letter Double_Quote × Hebrew_Letter
//        WB9.   (ALetter | Hebrew_Letter) × Numeric
//        WB10.  Numeric × (ALetter | Hebrew_Letter)
//        WB13.  Katakana × Katakana
//        WB13a. (ALetter | Hebrew_Letter | Numeric | Katakana | ExtendNumLet) × ExtendNumLet
//        WB13b. ExtendNumLet × (ALetter | Hebrew_Letter | Numeric | Katakana) 
//
{ExtendNumLetEx}*  ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                           )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx}    )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}         )*
                     | {HebrewOrALetterEx} ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {HebrewOrALetterEx} )*
                     )+
                   )
({ExtendNumLetEx}+ ( {KatakanaEx}          ( {ExtendNumLetEx}*   {KatakanaEx}                           )*
                   | ( {HebrewLetterEx}    ( {SingleQuoteEx}     | {DoubleQuoteEx}  {HebrewLetterEx}    )
                     | {NumericEx}         ( ( {ExtendNumLetEx}* | {MidNumericEx} ) {NumericEx}         )*
                     | {HebrewOrALetterEx} ( ( {ExtendNumLetEx}* | {MidLetterEx}  ) {HebrewOrALetterEx} )*
                     )+
                   )
)*
{ExtendNumLetEx}* 
  { return WORD_TYPE; }


// From UAX #29:
//
//    [C]haracters with the Line_Break property values of Contingent_Break (CB), 
//    Complex_Context (SA/South East Asian), and XX (Unknown) are assigned word 
//    boundary property values based on criteria outside of the scope of this
//    annex.  That means that satisfactory treatment of languages like Chinese
//    or Thai requires special handling.
// 
// In Unicode 6.3, only one character has the \p{Line_Break = Contingent_Break}
// property: U+FFFC ( ￼ ) OBJECT REPLACEMENT CHARACTER.
//
// In the ICU implementation of UAX#29, \p{Line_Break = Complex_Context}
// character sequences (from South East Asian scripts like Thai, Myanmar, Khmer,
// Lao, etc.) are kept together.  This grammar does the same below.
//
// See also the Unicode Line Breaking Algorithm:
//
//    http://www.unicode.org/reports/tr14/#SA
//
{ComplexContext}+ { return SOUTH_EAST_ASIAN_TYPE; }

// UAX#29 WB14.  Any ÷ Any
//
{HanEx} { return IDEOGRAPHIC_TYPE; }
{HiraganaEx} { return HIRAGANA_TYPE; }


// UAX#29 WB3.   CR × LF
//        WB3a.  (Newline | CR | LF) ÷
//        WB3b.  ÷ (Newline | CR | LF)
//        WB13c. Regional_Indicator × Regional_Indicator
//        WB14.  Any ÷ Any
//
{RegionalIndicatorEx} {RegionalIndicatorEx}+ | [^]
  { /* Break so we don't hit fall-through warning: */ break; /* Not numeric, word, ideographic, hiragana, or SE Asian -- ignore it. */ }
