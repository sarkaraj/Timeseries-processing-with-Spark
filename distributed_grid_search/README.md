# Distributed Grid Search Module

#### Usage
Deploy the __*time series*__ models with different parameter sets in a distributed fashion.

#### Module Structure
1. ``__init__`` : 
2. ``_fbprophet`` : 
3. ``_fbprophet_monthly`` : 
4. ``_model_params_set`` : 
5. ``_pydlm_monthly`` : 
6. ``_sarimax`` : 
7. ``properties`` : 
------

#### Function Documentation
    
   1. _fbprophet
    
        1. ``_get_pred_dict_prophet_w``: Get a dict {(weekNum,year):pred_val} from a pandas dataframe. weekNum --> Week Number of the year
        
            :param prediction: Pandas DataFrame:: DataFrame of the structure --> |date |prediction |
            
            :return: Dictionary:: {(weekNum,year): pred_val}
            
        ------
            
        2. ``run_prophet``: Execute Prophet given a specific set of parameters.
        
               :param cus_no: String:: Customer Number
                
               :param mat_no: String:: Material Number
                
               :param prod: Pandas DataFrame:: DataFrame containing the time series data
                
               :param param: Dictionary:: a dictionary containing the complete parameter set for a single Prophet instance
                
               :param kwargs:
                
                               1. 'min_train_days': Float/Int:: Minimum Training Days
                                
                               2. 'test_points': Float/Int:: Number of test points for each successive cross-validation loop
                                
                               3. 'pred_points': Float/Int:: Number of prediction points
                                
                               4. 'pdt_cat': Dictionary:: All category specific information
                                
               :return: Tuple:: Tuple of the structure
                
                               ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))
                                
            
                               1. _criteria: Float/Double:: Selection criteria used to select the best parameter set for a given cust-pdt group
                                
                               2. output_error_dict: Dictionary{String : Float}:: dictionary containing all the captured errors
                                
                               3. _prediction: Dictionary{(String, String) : Float}:: dictionary containing the prediction values.
                                
                                               Structure -->  {(weekNum,year): pred_val}
                                                