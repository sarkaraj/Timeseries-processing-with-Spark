# # CATEGORY 1
class product_cat_1(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 60
        self.pdt_freq_annual_upper = float("inf")
        self.time_gap_days_lower = (731 + 123)
        self.time_gap_days_upper = float("inf")
        self.time_gap_years = 2.0
        self.min_train_days = 731
        self.category = 'I'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 2
class product_cat_2(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 60
        self.pdt_freq_annual_upper = float("inf")
        self.time_gap_days_lower = (365 + 310)
        self.time_gap_days_upper = (731 + 123)
        self.time_gap_years = 1.5
        self.min_train_days = int(365 * 1.5)
        self.category = 'II'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 3
class product_cat_3(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 60
        self.pdt_freq_annual_upper = float("inf")
        self.time_gap_days_lower = (365 + 123)
        self.time_gap_days_upper = (365 + 310)
        self.time_gap_years = 1.0
        self.min_train_days = 365
        self.category = 'III'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 4
class product_cat_4(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 20
        self.pdt_freq_annual_upper = 60
        self.time_gap_days_lower = (731 + 123)
        self.time_gap_days_upper = float("inf")
        self.time_gap_years = 2.0
        self.min_train_days = 731
        self.category = 'IV'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 5
class product_cat_5(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 20
        self.pdt_freq_annual_upper = 60
        self.time_gap_days_lower = (365 + 310)
        self.time_gap_days_upper = (731 + 123)
        self.time_gap_years = 1.5
        self.min_train_days = int(365 * 1.5)
        self.category = 'V'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 6
class product_cat_6(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 20
        self.pdt_freq_annual_upper = 60
        self.time_gap_days_lower = (365 + 123)
        self.time_gap_days_upper = (365 + 310)
        self.time_gap_years = 1.0
        self.min_train_days = 365
        self.category = 'VI'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 7
class product_cat_7(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 60
        self.pdt_freq_annual_upper = float("inf")
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = (365 + 123)
        self.category = 'VII'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


# # CATEGORY 8
class product_cat_8(object):
    def __init__(self):
        self.pdt_freq_annual_lower = 20
        self.pdt_freq_annual_upper = 60
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = (365 + 123)
        self.category = 'VIII'

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}


cat_1 = product_cat_1()
cat_2 = product_cat_2()
cat_3 = product_cat_3()
cat_4 = product_cat_4()
cat_5 = product_cat_5()
cat_6 = product_cat_6()
cat_7 = product_cat_7()
cat_8 = product_cat_8()

# print cat_1.get_product_prop()
