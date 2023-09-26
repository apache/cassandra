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

package org.apache.cassandra.index.sai.analyzer.filter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

/**
 * Built-in {@link Analyzer} implementations. These are provided to allow users to easily configure analyzers with
 * a single word.
 */
public enum BuiltInAnalyzers
{
    STANDARD
    {
        public Analyzer getNewAnalyzer()
        {
            return new StandardAnalyzer();
        }
    },
    SIMPLE
    {
        public Analyzer getNewAnalyzer()
        {
            return new SimpleAnalyzer();
        }
    },
    WHITESPACE
    {
        public Analyzer getNewAnalyzer()
        {
            return new WhitespaceAnalyzer();
        }
    },
    STOP
    {
        public Analyzer getNewAnalyzer()
        {
            return new StopAnalyzer(EnglishAnalyzer.getDefaultStopSet());
        }
    },
    LOWERCASE
    {
        public Analyzer getNewAnalyzer()
        {
            try
            {
                CustomAnalyzer.Builder builder = CustomAnalyzer.builder();
                builder.withTokenizer("keyword");
                builder.addTokenFilter("lowercase");
                return builder.build();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    },
    KEYWORD
    {
        public Analyzer getNewAnalyzer()
        {
            try
            {
                return new KeywordAnalyzer();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    },
    ARABIC
    {
        public Analyzer getNewAnalyzer()
        {
            return new ArabicAnalyzer();
        }
    },
    ARMENIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new ArmenianAnalyzer();
        }
    },
    BASQUE
    {
        public Analyzer getNewAnalyzer()
        {
            return new BasqueAnalyzer();
        }
    },
     BENGALI
     {
         public Analyzer getNewAnalyzer()
         {
             return new BengaliAnalyzer();
         }
     },
     BRAZILIAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new BrazilianAnalyzer();
         }
     },
     BULGARIAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new BulgarianAnalyzer();
         }
     },
     CATALAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new CatalanAnalyzer();
         }
     },
     CJK
     {
         public Analyzer getNewAnalyzer()
         {
             return new CJKAnalyzer();
         }
     },
     CZECH
     {
         public Analyzer getNewAnalyzer()
         {
             return new CzechAnalyzer();
         }
     },
     DANISH
     {
         public Analyzer getNewAnalyzer()
         {
             return new DanishAnalyzer();
         }
     },
     DUTCH
     {
         public Analyzer getNewAnalyzer()
         {
             return new DutchAnalyzer();
         }
     },
     ENGLISH
     {
         public Analyzer getNewAnalyzer()
         {
             return new EnglishAnalyzer();
         }
     },
     ESTONIAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new EstonianAnalyzer();
         }
     },
     FINNISH
     {
         public Analyzer getNewAnalyzer()
         {
             return new FinnishAnalyzer();
         }
     },
     FRENCH
     {
         public Analyzer getNewAnalyzer()
         {
             return new FrenchAnalyzer();
         }
     },
     GALICIAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new GalicianAnalyzer();
         }
     },
     GERMAN
     {
         public Analyzer getNewAnalyzer()
         {
             return new GermanAnalyzer();
         }
     },
    GREEK
    {
        public Analyzer getNewAnalyzer()
        {
            return new GreekAnalyzer();
        }
    },
    HINDI
    {
        public Analyzer getNewAnalyzer()
        {
            return new HindiAnalyzer();
        }
    },
    HUNGARIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new HungarianAnalyzer();
        }
    },
    INDONESIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new IndonesianAnalyzer();
        }
    },
    IRISH
    {
        public Analyzer getNewAnalyzer()
        {
            return new IrishAnalyzer();
        }
    },
    ITALIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new ItalianAnalyzer();
        }
    },
    LATVIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new LatvianAnalyzer();
        }
    },
    LITHUANIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new LithuanianAnalyzer();
        }
    },
    NORWEGIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new NorwegianAnalyzer();
        }
    },
    PERSIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new PersianAnalyzer();
        }
    },
    PORTUGUESE
    {
        public Analyzer getNewAnalyzer()
        {
            return new PortugueseAnalyzer();
        }
    },
    ROMANIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new RomanianAnalyzer();
        }
    },
    RUSSIAN
    {
        public Analyzer getNewAnalyzer()
        {
            return new RussianAnalyzer();
        }
    },
    SORANI
    {
        public Analyzer getNewAnalyzer()
        {
            return new SoraniAnalyzer();
        }
    },
    SPANISH
    {
        public Analyzer getNewAnalyzer()
        {
            return new SpanishAnalyzer();
        }
    },
    SWEDISH
    {
        public Analyzer getNewAnalyzer()
        {
            return new SwedishAnalyzer();
        }
    },
    TURKISH
    {
        public Analyzer getNewAnalyzer()
        {
            return new TurkishAnalyzer();
        }
    },
    THAI
    {
        public Analyzer getNewAnalyzer()
        {
            return new ThaiAnalyzer();
        }
    },
    ;

    public abstract Analyzer getNewAnalyzer();
}
