CREATE ROLE new_role;
CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true;
CREATE ROLE alice WITH HASHED PASSWORD = '$2a$10$JSJEMFm6GeaW9XxT5JIheuEtPvat6i7uKbnTcxX3c1wshIIsGyUtG' AND LOGIN = true;
CREATE ROLE bob WITH PASSWORD = 'password_b' AND LOGIN = true AND SUPERUSER = true;
CREATE ROLE carlos WITH OPTIONS = { 'custom_option1' : 'option1_value', 'custom_option2' : 99 };
CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND ACCESS TO DATACENTERS {'DC1', 'DC3'};
CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND ACCESS TO ALL DATACENTERS;
CREATE ROLE bob WITH LOGIN = true and PASSWORD = 'password_d' AND ACCESS FROM CIDRS { 'region1', 'region2' };
CREATE ROLE hob WITH LOGIN = true and PASSWORD = 'password_c' AND ACCESS FROM ALL CIDRS;
