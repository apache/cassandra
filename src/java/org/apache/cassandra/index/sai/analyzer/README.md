<!---
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Configuring an SAI with an Analyzer

Analyzers are built on the Lucene Java Analyzer API. The SAI uses the Lucene Java Analyzer API to transform text columns into tokens for indexing and querying. The SAI supports the use of built-in analyzers and custom analyzers.

## Defining an Analyzer

Analyzers have one `tokenizer`, a list of `filters`, and a list of `charFilters`. The `tokenizer` splits the input text into tokens. The `filters` and `charFilters` transform the tokens into a form that is suitable for indexing and querying. The `filters` and `charFilters` are applied in the order they are defined in the configuration. The `filters` and the `charFilters` are optional.

## Configuration Formatting

The `OPTIONS` configuration argument is formatted as a JSON object:

```
OPTIONS = { 'index_analyzer' : '<BUILT_IN_ANALYZER>' }
```

OR

```
OPTIONS = {
  'index_analyzer':
  {
    "tokenizer" : {
      "name" : "",
      "args" : {}
    },
    "filters" : [
      {
        "name" : "", 
        "args": {}
      }
    ], 
    "charFilters" : [
      {
        "name" : "",
        "args": {}
      }
    ]
  }
}
```

## Built-in Analyzers

The following built-in analyzers are available:

| Analyzer Name | Description from Lucene Java Docs                                                                        |
|---------------|----------------------------------------------------------------------------------------------------------|
| `standard`    | Filters `StandardTokenizer` output with `LowerCaseFilter`                                                |
| `simple`      | Filters `LetterTokenizer` output with `LowerCaseFilter`                                                  |
| `whitespace`  | Analyzer that uses `WhitespaceTokenizer`.                                                                |
| `stop`        | Filters `LetterTokenizer` output with `LowerCaseFilter` and removes Lucene's default English stop words. |
| `lowercase`   | Normalizes input by applying `LowerCaseFilter` (no additional tokenization is performed).                |
| `<language>`  | Analyzers for specific languages. For example, `english` and `french`.                                   |

### Standard Analyzer

Here is the custom analyzer configuration for the standard analyzer:

```
OPTIONS = {
  'index_analyzer':
  {
    "tokenizer" : {
      "name" : "standard",
      "args" : {}
    },
    "filters" : [
      {
        "name" : "lowercase", 
        "args": {}
      }
    ], 
    "charFilters" : []
  }
}
```

### Simple Analyzer

Here is the custom analyzer configuration for the simple analyzer:

```
OPTIONS = {
  'index_analyzer':
  {
    "tokenizer" : {
      "name" : "letter",
      "args" : {}
    },
    "filters" : [
      {
        "name" : "lowercase", 
        "args": {}
      }
    ], 
    "charFilters" : []
  }
}
```

### Whitespace Analyzer

Here is the custom analyzer configuration for the whitespace analyzer:

```
OPTIONS = {
  'index_analyzer':
  {
    "tokenizer" : {
      "name" : "whitespace",
      "args" : {}
    },
    "filters" : [], 
    "charFilters" : []
  }
}
```

### Lowercase Analyzer

Here is the custom analyzer configuration for the lowercase analyzer:

```
OPTIONS = {
 'index_analyzer':
  {
    "tokenizer" : {
      "name" : "keyword",
      "args" : {}
    },
    "filters" : [
      {
        "name" : "lowercase", 
        "args": {}
      }
    ], 
    "charFilters" : []
  }
}
```