/*
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/

CREATE TABLE has_all_types (
    num int PRIMARY KEY,
    intcol int,
    asciicol ascii,
    bigintcol bigint,
    blobcol blob,
    booleancol boolean,
    decimalcol decimal,
    doublecol double,
    floatcol float,
    smallintcol smallint,
    textcol text,
    timestampcol timestamp,
    tinyintcol tinyint,
    uuidcol uuid,
    varcharcol varchar,
    varintcol varint
) WITH compression = {'sstable_compression':'LZ4Compressor'};

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (0, -12, 'abcdefg', 1234567890123456789, 0x000102030405fffefd, true,
        19952.11882, 1.0, -2.1, 32767, 'Voilá!',
        '2012-05-14 12:53:20+0000', 127, bd1924e1-6af8-44ae-b5e1-f24131dbd460, '"', 10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (1, 2147483647, '__!''$#@!~"', 9223372036854775807, 0xffffffffffffffffff, true,
        0.00000000000001, 9999999.999, 99999.999, 32767, '∭Ƕ⑮ฑ➳❏''',
        '1950-01-01+0000', 127, ffffffff-ffff-ffff-ffff-ffffffffffff, 'newline->
<-', 9);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (2, 0, '', 0, 0x, false,
        0.0, 0.0, 0.0, 0, '',
        0, 0, 00000000-0000-0000-0000-000000000000, '', 0);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (3, -2147483648, '''''''', -9223372036854775808, 0x80, false,
        10.0000000000000, -1004.10, 100000000.9, 32767, '龍馭鬱',
        '2038-01-19T03:14-1200', 127, ffffffff-ffff-1fff-8fff-ffffffffffff,
	    '''', -10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, smallintcol, textcol,
                           timestampcol, tinyintcol, uuidcol, varcharcol, varintcol)
VALUES (4, blobAsInt(0x), '', blobAsBigint(0x), 0x, blobAsBoolean(0x),
	blobAsDecimal(0x), blobAsDouble(0x), blobAsFloat(0x), blobAsSmallInt(0x0000), '',
	blobAsTimestamp(0x), blobAsTinyInt(0x00), blobAsUuid(0x), '', blobAsVarint(0x));



CREATE TABLE empty_table (
    lonelykey float primary key,
    lonelycol text
);



CREATE TABLE dynamic_columns (
    somekey int,
    column1 float,
    value text,
    PRIMARY KEY(somekey, column1)
);

INSERT INTO dynamic_columns (somekey, column1, value) VALUES (1, 1.2, 'one point two');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (2, 2.3, 'two point three');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, 3.46, 'three point four six');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, 99.0, 'ninety-nine point oh');
INSERT INTO dynamic_columns (somekey, column1, value) VALUES (3, -0.0001, 'negative ten thousandth');



CREATE TABLE twenty_rows_table (
    a text primary key,
    b text
);

INSERT INTO twenty_rows_table (a, b) VALUES ('1', '1');
INSERT INTO twenty_rows_table (a, b) VALUES ('2', '2');
INSERT INTO twenty_rows_table (a, b) VALUES ('3', '3');
INSERT INTO twenty_rows_table (a, b) VALUES ('4', '4');
INSERT INTO twenty_rows_table (a, b) VALUES ('5', '5');
INSERT INTO twenty_rows_table (a, b) VALUES ('6', '6');
INSERT INTO twenty_rows_table (a, b) VALUES ('7', '7');
INSERT INTO twenty_rows_table (a, b) VALUES ('8', '8');
INSERT INTO twenty_rows_table (a, b) VALUES ('9', '9');
INSERT INTO twenty_rows_table (a, b) VALUES ('10', '10');
INSERT INTO twenty_rows_table (a, b) VALUES ('11', '11');
INSERT INTO twenty_rows_table (a, b) VALUES ('12', '12');
INSERT INTO twenty_rows_table (a, b) VALUES ('13', '13');
INSERT INTO twenty_rows_table (a, b) VALUES ('14', '14');
INSERT INTO twenty_rows_table (a, b) VALUES ('15', '15');
INSERT INTO twenty_rows_table (a, b) VALUES ('16', '16');
INSERT INTO twenty_rows_table (a, b) VALUES ('17', '17');
INSERT INTO twenty_rows_table (a, b) VALUES ('18', '18');
INSERT INTO twenty_rows_table (a, b) VALUES ('19', '19');
INSERT INTO twenty_rows_table (a, b) VALUES ('20', '20');


CREATE TABLE undefined_values_table (
    k text PRIMARY KEY,
    c text,
    notthere text
);

INSERT INTO undefined_values_table (k, c) VALUES ('k1', 'c1');
INSERT INTO undefined_values_table (k, c) VALUES ('k2', 'c2');



CREATE TABLE ascii_with_special_chars (
    k int PRIMARY KEY,
    val ascii
);

-- "newline:\n"
INSERT INTO ascii_with_special_chars (k, val) VALUES (0, blobAsAscii(0x6e65776c696e653a0a));
-- "return\rand null\0!"
INSERT INTO ascii_with_special_chars (k, val) VALUES (1, blobAsAscii(0x72657475726e0d616e64206e756c6c0021));
-- "\x00\x01\x02\x03\x04\x05control chars\x06\x07"
INSERT INTO ascii_with_special_chars (k, val) VALUES (2, blobAsAscii(0x000102030405636f6e74726f6c2063686172730607));
-- "fake special chars\\x00\\n"
INSERT INTO ascii_with_special_chars (k, val) VALUES (3, blobAsAscii(0x66616b65207370656369616c2063686172735c7830305c6e));



CREATE TABLE utf8_with_special_chars (
    k int PRIMARY KEY,
    val text
);

INSERT INTO utf8_with_special_chars (k, val) VALUES (0, 'Normal string');
INSERT INTO utf8_with_special_chars (k, val) VALUES (1, 'Text with
newlines
');
INSERT INTO utf8_with_special_chars (k, val) VALUES (2, 'Text with embedded  char');
INSERT INTO utf8_with_special_chars (k, val) VALUES (3, 'ⓈⓅⒺⒸⒾⒶⓁ ⒞⒣⒜⒭⒮ and normal ones');
INSERT INTO utf8_with_special_chars (k, val) VALUES (4, 'double wides: ⾑⾤⾚');
INSERT INTO utf8_with_special_chars (k, val) VALUES (5, 'zero width​space');
INSERT INTO utf8_with_special_chars (k, val) VALUES (6, 'fake special chars\x00\n');


CREATE TABLE empty_composite_table (
    lonelykey float,
    lonelycol text,
    lonelyval int,
    primary key (lonelykey, lonelycol)
);

CREATE TABLE twenty_rows_composite_table (
    a text,
    b text,
    c text,
    primary key (a, b)
);

-- all in the same storage engine row:

INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '1', '1');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '2', '2');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '3', '3');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '4', '4');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '5', '5');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '6', '6');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '7', '7');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '8', '8');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '9', '9');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '10', '10');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '11', '11');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '12', '12');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '13', '13');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '14', '14');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '15', '15');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '16', '16');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '17', '17');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '18', '18');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '19', '19');
INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('A', '20', '20');

CREATE TYPE address (
    city text,
    address text,
    zip text
);

CREATE TYPE phone_number (
    country text,
    number text
);

CREATE TABLE users (
    login text PRIMARY KEY,
    name text,
    addresses set<frozen<address>>,
    phone_numbers set<frozen<phone_number>>
);

insert into users (login, name, addresses, phone_numbers)
values ('jbellis',
        'jonathan ellis',
        {{city: 'Austin', address: '902 East 5th St. #202', zip: '78702'},
         {city: 'Sunnyvale', address: '292 Gibraltar Drive #107', zip: '94089'}},
        {{country: '+44', number: '208 622 3021'},
         {country: '+1', number: '512-537-7809'}});

insert into users (login, name, addresses, phone_numbers)
values ('vpupkin',
        'vasya pupkin',
        {{city: 'Chelyabinsk', address: '3rd street', zip: null},
         {city: 'Chigirinsk', address: null, zip: '676722'}},
        {{country: '+7', number: null},
         {country: null, number: '03'}});

CREATE TYPE band_info_type (
  founded varint,
  members set<text>,
  description text
);

CREATE TYPE tags (
  tags map<text, text>
);

CREATE TABLE songs (
    title text PRIMARY KEY,
    band text,
    info frozen<band_info_type>,
    tags frozen<tags>
);

insert into songs (title, band, info, tags)
values (
    'The trooper',
    'Iron Maiden',
    {
        founded:188694000,
        members: {
            'Bruce Dickinson',
            'Dave Murray',
            'Adrian Smith',
            'Janick Gers',
            'Steve Harris',
            'Nicko McBrain'
        },
        description: 'Pure evil metal'
     },
    {
        tags: {
            'genre':'metal',
            'origin':'england'
        }
    });

CREATE FUNCTION fBestband ( input double )
    RETURNS NULL ON NULL INPUT
    RETURNS text 
    LANGUAGE java
    AS 'return "Iron Maiden";';

CREATE FUNCTION fBestsong ( input double )
    RETURNS NULL ON NULL INPUT
    RETURNS text 
    LANGUAGE java
    AS 'return "Revelations";';

CREATE FUNCTION fMax(current int, candidate int)
    CALLED ON NULL INPUT
    RETURNS int 
    LANGUAGE java 
    AS 'if (current == null) return candidate; else return Math.max(current, candidate);' ;

CREATE FUNCTION fMin(current int, candidate int)
    CALLED ON NULL INPUT
    RETURNS int
    LANGUAGE java 
    AS 'if (current == null) return candidate; else return Math.min(current, candidate);' ;

CREATE AGGREGATE aggMax(int)
    SFUNC fMax
    STYPE int
    INITCOND null;

CREATE AGGREGATE aggMin(int)
    SFUNC fMin
    STYPE int
    INITCOND null;

