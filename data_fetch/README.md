# Data-Fetch Module

#### Usage
Retrieve data from Hive tables.

#### Module Structure
1. ``__init__`` : Initiate module
2. ``data_query`` : Contains functions to get weekly and monthly data with appropriate transformations and filters
3. ``properties`` : Parameter file for ``data_query``. Primary data-fetch query for the program should be changed here
4. ``support_func`` : Utility functions used in ``data_query``

------
#### Function Documentation

  1. ##### data_query

        1.``get_data_weekly`` : Fetch data for weekly categories. Filters applied to dataset(s):
                1. 'quantity' greater than 0
                2.  Ignore all groups (cust-pdt combo) which has no invoice for the past _latest_product_criteria_days (92 days)
       
            :param sqlContext: Spark SQLContext
        
            :param kwargs: 'week_cutoff_date' : String:: (yyyy-MM-dd)
        
            :return: Spark DataFrame of all filtered groups. Each row is 1 group. Full time-series for each group is zipped in each row.
     
     -----
     
        2.``get_data_monthly`` : Fetch data for monthly categories. Filters applied to dataset(s):
                1. 'quantity' greater than 0
                2.  Ignore all groups (cust-pdt combo) which has no invoice for the past _latest_product_criteria_days (92 days)
    
            :param sqlContext: Spark SQLContext
        
            :param kwargs: 'month_cutoff_date' : String:: (yyyy-MM-dd)
        
            :return: Spark DataFrame of all filtered groups. Each row is 1 group. Full time-series for each group is zipped in each row.
     
  ------
     
  2. ##### properties
  
        1.``_query`` : Primary query for data extraction from Hive tables
        
        2.``CUSTOMER_LIST`` : Optional parameter to specify a set of customers into the query. There are existing two sets of query. This works only with a specific one. Use it appropriately 
  ------
  
  3. ##### support_func
        1.``_get_weekly_mdl_bld_cutoff_date`` : Input should ONLY be Sundays when model building is performed. Used in the initial data query from Hive
        
            :param _date: Date of SUNDAY in datetime.date type
            
            :return: String of Date used for query --> last Saturday to the corresponding Sunday
            
        ------
    
        2.``_get_monthly_mdl_bld_cutoff_date`` : Input should ONLY be Sundays when model building is performed. Used in the initial data query from Hive
        
            :param _date: Date of SUNDAY in datetime.date type
            
            :return: String of Date used for query --> date of the last day of previous month is string format
        
        ------
    
        3.``generate_weekly_query`` : Generate data extraction query for weekly run
        
            :param date: String:: Date in ('yyyy-MM-dd') format. The date when the model is executed
            
            :return: String:: Complete hive query
        
        ------
    
        4.``generate_monthly_query`` : Generate data extraction query for monthly run
        
            :param date:  String:: Date in ('yyyy-MM-dd') format. The date when the model is executed
            
            :return:  String:: Complete hive query
            
  ------

        
