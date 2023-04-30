--All raw data from table
SELECT * 
FROM "mydb"."public"."bitcoin_prices";

--Number of records collected on the most recent data load
SELECT 
    COUNT(id) 
FROM "mydb"."public"."bitcoin_prices" 
WHERE period_date = current_date - 1;

--Highest volume trading period
SELECT 
    time_period_start, 
    time_period_end, 
    volume_traded 
FROM "mydb"."public"."bitcoin_prices" 
ORDER BY volume_traded DESC
LIMIT 1;

--High and Low Price
SELECT 
    max(price_high), 
    min(price_low) 
FROM "mydb"."public"."bitcoin_prices";