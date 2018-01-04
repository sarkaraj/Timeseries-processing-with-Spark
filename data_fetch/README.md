# Data-Fetch Module

#### Usage
Retrieve data from Hive tables.

#### Module Structure
1. ``__init__`` : Initiate module
2. ``data_query`` : Contains functions to get weekly and monthly data with appropriate transformations and filters
3. ``properties`` : Parameter file for ``data_query``. Primary data-fetch query for the program should be changed here
4. ``support_func`` : Utility functions used in ``data_query``

#### Function Documentation

  1. ##### data_query

     1.``get_data_weekly`` : Fetch data for weekly categories. Filters applied to dataset(s):
       1. 'quantity' greater than 0
       2.  Ignore all groups (cust-pdt combo) which has no invoice for the past _latest_product_criteria_days (92 days)
       
       
        :param sqlContext: Spark SQLContext
        
        :param kwargs: 'week_cutoff_date' : String:: (yyyy-MM-dd)
        
        :return: Spark DataFrame of all filtered groups. Each row is 1 group. Full time-series for each group is zipped in each row.
     
     2. ``get_data_monthly`` : Fetch data for monthly categories. Filters applied to dataset(s):
        1. 'quantity' greater than 0
        2.  Ignore all groups (cust-pdt combo) which has no invoice for the past _latest_product_criteria_days (92 days)
    
        :param sqlContext: Spark SQLContext
        
        :param kwargs: 'month_cutoff_date' : String:: (yyyy-MM-dd)
        
        :return: Spark DataFrame of all filtered groups. Each row is 1 group. Full time-series for each group is zipped in each row.
        
