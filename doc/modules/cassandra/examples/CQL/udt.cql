CREATE TYPE phone (
    country_code int,
    number text,
);

CREATE TYPE address (
    street text,
    city text,
    zip text,
    phones map<text, phone>
);

CREATE TABLE user (
    name text PRIMARY KEY,
    addresses map<text, frozen<address>>
);
