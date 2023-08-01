CREATE TABLE patients (
   id timeuuid PRIMARY KEY,
   name text,
   birth date
);

INSERT INTO patients(id, name, birth) VALUES (now(), 'alice', '1982-01-02');
INSERT INTO patients(id, name, birth) VALUES (now(), 'bob', '1982-01-02');

SELECT mask_inner(name, 1, null), mask_default(birth) FROM patients;

//   system.mask_inner(name, 1, NULL) | system.mask_default(birth)
// -----------------------------------+----------------------------
//                                b** |                 1970-01-01
//                              a**** |                 1970-01-01
