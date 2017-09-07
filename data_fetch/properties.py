# appname="test_cona"
# mode="yarn-client"

#########################_____DATA_INGESTION_PARAMETERS_____#########################

query="""select d.customernumber customernumber, d.matnr matnr, d.bill_date bill_date, IF(d.units != 'CS', d.quantity * (f.umrez / f.umren), d.quantity) quantity, ((quantity * quantity) / d.price) q_indep_prc
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
        where vkbur='C001'
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
        d.matnr=f.matnr and d.units=f.meinh"""

_latest_product_criteria_days=92
_product_criteria_days=1030
_product_criteria_years=2.0
_product_criteria_annual_frequency=60

###############################################################################

#########################_____MODELING_PARAMETERS_____#########################



###############################################################################


#########################_____EXPERIMENTAL_____#########################
# Experimental
# include_neg_quantity=False

###############################################################################