# Insert some data
INSERT INTO store.shopping_cart
(userid, item_count, last_update_timestamp)
VALUES ('9876', 2, toTimeStamp(toDate(now))));
INSERT INTO store.shopping_cart
(userid, item_count, last_update_timestamp)
VALUES (1234, 5, toTimeStamp(toDate(now))));
