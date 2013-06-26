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
    textcol text,
    timestampcol timestamp,
    uuidcol uuid,
    varcharcol varchar,
    varintcol varint
) WITH compression = {'sstable_compression':'LZ4Compressor'};

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (0, -12, 'abcdefg', 1234567890123456789, 0x000102030405fffefd, true,
        19952.11882, 1.0, -2.1, 'Voilá!', '2012-05-14 12:53:20+0000',
        bd1924e1-6af8-44ae-b5e1-f24131dbd460, '"', 10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (1, 2147483647, '__!''$#@!~"', 9223372036854775807, 0xffffffffffffffffff, true,
        0.00000000000001, 9999999.999, 99999.99, '∭Ƕ⑮ฑ➳❏''', '1900-01-01+0000',
        ffffffff-ffff-ffff-ffff-ffffffffffff, 'newline->
<-', 9);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (2, 0, '', 0, 0x, false,
        0.0, 0.0, 0.0, '', 0,
        00000000-0000-0000-0000-000000000000, '', 0);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (3, -2147483648, '''''''', -9223372036854775808, 0x80, false,
        10.0000000000000, -1004.10, 100000000.9, '龍馭鬱', '2038-01-19T03:14-1200',
        ffffffff-ffff-1fff-8fff-ffffffffffff, '''', -10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (4, blobAsInt(0x), '', blobAsBigint(0x), 0x, blobAsBoolean(0x), blobAsDecimal(0x),
        blobAsDouble(0x), blobAsFloat(0x), '', blobAsTimestamp(0x), blobAsUuid(0x), '',
        blobAsVarint(0x));


CREATE TABLE has_value_encoding_errors (
    pkey ascii PRIMARY KEY,
    utf8col blob
);

INSERT INTO has_value_encoding_errors (pkey, utf8col) VALUES ('A', 0x00ff00ff);
ALTER TABLE has_value_encoding_errors ALTER utf8col TYPE text;

CREATE TABLE has_key_encoding_errors (
    pkey blob PRIMARY KEY,
    col text
);

INSERT INTO has_key_encoding_errors (pkey, col) VALUES (0x00ff028f, 'whatever');
ALTER TABLE has_key_encoding_errors ALTER pkey TYPE text;



CREATE TABLE empty_table (
    lonelykey float primary key,
    lonelycol text
);



CREATE COLUMNFAMILY dynamic_columns (
    somekey int,
    column1 float,
    value text,
    PRIMARY KEY(somekey, column1)
) WITH COMPACT STORAGE;

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



CREATE TABLE ascii_with_invalid_and_special_chars (
    k int PRIMARY KEY,
    val blob
);

-- "newline:\n"
INSERT INTO ascii_with_invalid_and_special_chars (k, val) VALUES (0, 0x6e65776c696e653a0a);
-- "return\rand null\0!"
INSERT INTO ascii_with_invalid_and_special_chars (k, val) VALUES (1, 0x72657475726e0d616e64206e756c6c0021);
-- "\x00\x01\x02\x03\x04\x05control chars\x06\x07"
INSERT INTO ascii_with_invalid_and_special_chars (k, val) VALUES (2, 0x000102030405636f6e74726f6c2063686172730607);
-- "\xfe\xffbyte order mark"
INSERT INTO ascii_with_invalid_and_special_chars (k, val) VALUES (3, 0xfeff62797465206f72646572206d61726b);
-- "fake special chars\\x00\\n"
INSERT INTO ascii_with_invalid_and_special_chars (k, val) VALUES (4, 0x66616b65207370656369616c2063686172735c7830305c6e);

ALTER TABLE ascii_with_invalid_and_special_chars ALTER val TYPE ascii;



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
