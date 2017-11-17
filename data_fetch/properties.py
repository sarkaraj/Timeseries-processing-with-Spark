# CUSTOMER_LIST = "('0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889', '0500137825','0500149923','0500138015','0500137010','0500133350','0500236214','0500244712','0500137485','0500146185','0500139554','0500137155','0500150393','0500133156','0500245118','0500129375','0500286185','0500145182','0500134652','0500152491','0500131809')"
#
# # Query for all 25 customers in Cleveland,TN where sales office is C001
# _query = """
# select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, ((quantity * quantity) / d.price) q_indep_prc
# from
# (
# select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, b.price price
# from
# (
# select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
# from skuopt.invoices a
# where a.kunag in """ + CUSTOMER_LIST + """
# ) b
# ) d
# join
# (
# select e.matnr matnr, e.meinh meinh, e.umren umren, e.umrez umrez
# from mdm.dim_marm e
# ) f
# on
# d.matnr=f.matnr and d.units=f.meinh
# where d.bill_date <= """

_query = """
select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, ((quantity * quantity) / d.price) q_indep_prc
from
(
select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, b.price price
from
(
select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
from skuopt.invoices a
) b
join
(
select kunnr customernumber
from mdm.customer
where katr6='3' and regio='FL'
limit 3000
) c
on
b.customernumber = c.customernumber
) d
join
(
select e.matnr matnr, e.meinh meinh, e.umren umren, e.umrez umrez
from mdm.dim_marm e
) f
on
d.matnr=f.matnr and d.units=f.meinh
where d.bill_date <= """

_latest_product_criteria_days = 92

if __name__ == "__main__":
    print _query
