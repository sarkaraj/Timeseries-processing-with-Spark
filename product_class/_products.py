from product_class.properties import *

# # CATEGORY 1


class product_cat_1(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_1
        self.pdt_freq_annual_upper = float(annual_freq_cut_MAX)
        self.time_gap_days_lower = (731 + 123)
        self.time_gap_days_upper = float("inf")
        self.time_gap_years = 2.0
        self.min_train_days = 731
        self.category = 'I'
        self.baseline_ma_window = 6

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 2
class product_cat_2(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_1
        self.pdt_freq_annual_upper = float(annual_freq_cut_MAX)
        self.time_gap_days_lower = (365 + 310)
        self.time_gap_days_upper = (731 + 123)
        self.time_gap_years = 1.5
        self.min_train_days = int(365 * 1.5)
        self.category = 'II'
        self.baseline_ma_window = 6

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 3
class product_cat_3(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_1
        self.pdt_freq_annual_upper = float(annual_freq_cut_MAX)
        self.time_gap_days_lower = (365 + 123)
        self.time_gap_days_upper = (365 + 310)
        self.time_gap_years = 1.0
        self.min_train_days = 365
        self.category = 'III'
        self.baseline_ma_window = 6

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 4
class product_cat_4(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_2
        self.pdt_freq_annual_upper = annual_freq_cut_1
        self.time_gap_days_lower = (731 + 123)
        self.time_gap_days_upper = float("inf")
        self.time_gap_years = 2.0
        self.min_train_days = 731
        self.category = 'IV'
        self.baseline_ma_window = 3

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 5
class product_cat_5(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_2
        self.pdt_freq_annual_upper = annual_freq_cut_1
        self.time_gap_days_lower = (365 + 310)
        self.time_gap_days_upper = (731 + 123)
        self.time_gap_years = 1.5
        self.min_train_days = int(365 * 1.5)
        self.category = 'V'
        self.baseline_ma_window = 3

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 6
class product_cat_6(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_2
        self.pdt_freq_annual_upper = annual_freq_cut_1
        self.time_gap_days_lower = (365 + 123)
        self.time_gap_days_upper = (365 + 310)
        self.time_gap_years = 1.0
        self.min_train_days = 365
        self.category = 'VI'
        self.baseline_ma_window = 3

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self.baseline_ma_window


# # CATEGORY 7
class product_cat_7(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_1
        self.pdt_freq_annual_upper = float(annual_freq_cut_MAX)
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = (365 + 123)
        self.category = 'VII'
        self._window = 6

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self._window


# # CATEGORY 8
class product_cat_8(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_2
        self.pdt_freq_annual_upper = annual_freq_cut_1
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = (365 + 123)
        self.category = 'VIII'
        self._window = 3

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self._window


# # CATEGORY 9
class product_cat_9(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_3
        self.pdt_freq_annual_upper = annual_freq_cut_2
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = float("inf")
        self.category = 'IX'
        self._window = 6

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self._window


class product_cat_10(object):
    def __init__(self):
        self.pdt_freq_annual_lower = annual_freq_cut_MIN
        self.pdt_freq_annual_upper = annual_freq_cut_3
        self.time_gap_days_lower = 0
        self.time_gap_days_upper = float("inf")
        self.category = 'X'
        self._window = 12

    def get_product_prop(self):
        return {key: str(self.__dict__.get(key)) for key in self.__dict__.keys()}

    def get_window(self):
        return self._window


cat_1 = product_cat_1()
cat_2 = product_cat_2()
cat_3 = product_cat_3()
cat_4 = product_cat_4()
cat_5 = product_cat_5()
cat_6 = product_cat_6()
cat_7 = product_cat_7()
cat_8 = product_cat_8()
cat_9 = product_cat_9()
cat_10 = product_cat_10()

if __name__ == "__main__":
    print cat_1.get_product_prop()

    print cat_10.get_product_prop()
