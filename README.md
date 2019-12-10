# SparkBatchWithUnitTest
Spark batch job template with UnitTest.

This projects serves to provide a template for Spark batch applications where you have a main class which executes a business logic (Moving average in this case) and a test class which extends **QueryTest** with **SharedSQLContext**. We use **checkAnswer** method to check the output with the desired output.

Sample task info: 
1) **StockPrices:**(src/test/resources/input/stock_prices) 
_Columns_: stockId,timeStamp,stockPrice
2) **StockInfo:** (src/test/resources/input/stock_info) 
_Columns_: stockId,stockName,stockCategory

**Task 1:**
Compute the moving average for every stockID with a predefined moving window size.
Window Size : 3
_Columns_: stockId,timeStamp,stockPrice,moving_average 
_Ordered by_: stockId,timeStamp

**Task 2:**
Join with Projection - Join moving average calculated in Task1 with the stockInfo.
Window Size : 3 

Output Data
_Columns_: stockId,timeStamp,stockPrice,moving_average,stockName,stockCategory

**Task 3:**
Get the projected moving average data for a given stockId.

stockId :103 
_Columns_:stockId,timeStamp,stockPrice,moving_average,stockName,stockCategory 

Output Data
_Columns_:stockId,timeStamp,stockPrice,moving_average,stockName,stockCategory 

**Extensions:**

**Task 4:**
Handle Nulls 
File : src/test/resources/input/stock_prices_dirty/0.csv 
contains null values in stockPrice column.

Filter all the non null rows 
Columns: stockId,timeStamp,stockPrice

**Other Extensions**

 _Task 1 :_ Count of each files
 Load all the above files as spark dataframes and print the count of each of files.

 _Task 2 :_ Handle Nulls
File : test/resources/input/stock_prices_dirty/0.csv
 contains null values in amount column. Replace those null values with mean of column in spark dataframe.

_Task 3 :_ Add a Sequential Row ID
For a joined dataframe, add an id column which contains sequence numbers from 1 to number of rows

_Task 4 :_ Sales with 5% Discount
Add a column ​discount_amount​ to joined dataframe, which holds 5% discounted amount. 

**WHAT IS MOVING AVERAGE** 
Moving Average is a lagging indicator, which means that it do not predict new trends but confirm trends once they have been established. 
A moving average is commonly used with time series data to smooth out short-term fluctuations and highlight longer-term trends or cycles. 
To compute the moving average you would need to input a set of values and a window size,  and compute the moving average of the input. 
If the input values are x1, x2, x3, ..., xn and the window value is k the moving average is computed using the following formula:

yk = [x(k-k+1) + x(k-k+2) + .... + xk]/k

(All the windows with less than k elements gave output as 0)
