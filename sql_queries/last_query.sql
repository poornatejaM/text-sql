-- User Query: which day recorded the most sales

SELECT Sale_Date FROM sales_data GROUP BY Sale_Date ORDER BY sum(Sales_Amount) DESC LIMIT 1