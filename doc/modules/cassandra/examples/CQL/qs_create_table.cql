# Create a table
CREATE TABLE IF NOT EXISTS store.shopping_cart  (
	userid text PRIMARY KEY,
	item_count int,
	last_update_timestamp timestamp
);
