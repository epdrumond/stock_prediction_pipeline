#Create table for storaging actual stock daily prices
create table daily_stock_prices (
	Date datetime,
	Open double,
	High double,
	Low double,
	Close double,
	AdjClose double,
	Volume int,
	ExtractionDate datetime
);