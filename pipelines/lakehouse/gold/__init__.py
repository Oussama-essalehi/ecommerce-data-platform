"""Gold layer: the business view, one table per business concept.

Silver is still organised by source (a marketplace table, a web shop
table). Gold is organised by what the business talks about:

    orders        one row per order, whatever the sales channel
    order_lines   one row per product sold in an order
    customers     one row per customer, as known today
    products      one row per product, as known today

These four tables are what the data warehouse loads.
"""

TABLES = ["orders", "order_lines", "customers", "products"]
