from transform_data.data_transform import string_to_gregorian


def _get_weekly_mdl_bld_cutoff_date(_date):
    """
    Input should ONLY be Sundays when model building is performed. Used in the initial data query from Hive
    :param _date: Date of SUNDAY in datetime.date type
    :return: String of Date used for query --> last Saturday to the corresponding Sunday
    """
    _result = _date.strftime('\'%Y%m%d\'')
    return _result


def _get_monthly_mdl_bld_cutoff_date(_date):
    """
    Input should ONLY be Sundays when model building is performed. Used in the initial data query from Hive
    :param _date: Date of SUNDAY in datetime.date type
    :return: String of Date used for query --> date of the last day of previous month is string format
    """
    _result = _date.strftime('\'%Y%m%d\'')
    return _result


def generate_weekly_query(date):
    """
    Generate data extraction query for weekly run
    :param date: String:: Date in ('yyyy-MM-dd') format. The date when the model is executed
    :return: String:: Complete hive query
    """
    from properties import _query

    MODEL_BLD_CURRENT_DATE = string_to_gregorian(date)

    _result = _query + _get_weekly_mdl_bld_cutoff_date(MODEL_BLD_CURRENT_DATE)
    return _result


def generate_monthly_query(date):
    """
    Generate data extraction query for monthly run
    :param date:  String:: Date in ('yyyy-MM-dd') format. The date when the model is executed
    :return:  String:: Complete hive query
    """
    from properties import _query

    MODEL_BLD_CURRENT_DATE = string_to_gregorian(date)

    _result = _query + _get_monthly_mdl_bld_cutoff_date(MODEL_BLD_CURRENT_DATE)
    return _result


if __name__ == "__main__":
    print (_get_weekly_mdl_bld_cutoff_date.__name__)
    print (_get_weekly_mdl_bld_cutoff_date.__doc__)
    print (_get_monthly_mdl_bld_cutoff_date.__name__)
    print (_get_monthly_mdl_bld_cutoff_date.__doc__)
    print (generate_weekly_query.__name__)
    print (generate_weekly_query.__doc__)
    print (generate_monthly_query.__name__)
    print (generate_monthly_query.__doc__)
    # import datetime
    #
    # # today = string_to_gregorian('2017-10-11')
    # # first = today.replace(day=1)
    # # print first
    # # lastMonth = first - datetime.timedelta(days=1)
    # # print lastMonth
    # # print lastMonth.strftime('\'%Y%m%d\'')
    # input = '2017-09-01'
    # print generate_monthly_query(input)
    # print generate_weekly_query(input)
