
query="""
select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, ((quantity * quantity) / d.price) q_indep_prc
from
(
select b.customernumber customernumber, b.matnr matnr, b.bill_date bill_date ,b.quantity quantity, b.units units, b.price price
from
(
select a.kunag customernumber, a.matnr matnr, a.fkdat bill_date ,a.fklmg quantity, a.meins units, a.netwr price
from skuopt.invoices a
where a.kunag in ('0500066337','0500070166','0500070167','0500075749','0500083147','0500061438','0500067084','0500058324','0500080723','0500060033','0500068825','0500060917','0500078551','0500076115','0500071747','0500078478','0500078038','0500073982','0500064458','0500268924','0500070702','0500070336','0500076032','0500095883','0500284889')
) b
join
(
select kunnr customernumber
from mdm.customer
where vkbur='C005'
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
"""

_latest_product_criteria_days=92
_product_criteria_days=820
_product_criteria_years=2.0
_product_criteria_annual_frequency=60
