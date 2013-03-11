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
