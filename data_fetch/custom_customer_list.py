_customer_list = """
500083544
500244939
500097312
500068481
500067084
500076413
500070167
500096598
500075776
500075236
500078638
500075874
500073843
500096584
500096591
500062352
500068490
500071751
500510900
500070166
500074960
500058564
500057580
500244933
500075185
500076032
500057578
500269279
500078134
500072231
500080046
500068482
500074135
500096578
500096582
"""

remaining = """



"""


def generate_customer_list_fomatted(customer_list=_customer_list, string_to_append="0"):
    result_list = [["".join([string_to_append + str(elem)])] for elem in customer_list.split("\n") if len(elem) > 0]
    return result_list


if __name__ == "__main__":
    result = generate_customer_list_fomatted()
    print([elem[0] for elem in result])
