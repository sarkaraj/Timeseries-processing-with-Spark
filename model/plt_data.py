from model.save_images import *

def plot_raw_data(data, dir_name, cus_no, mat_no):
    one_dim_save_plot(x = data.date, y = data.quantity,dir_name= dir_name, xlable= "Date", ylable= "Quantity",
                      title= "Raw_data", cus_no= cus_no, mat_no= mat_no)

def plot_weekly_data(data, dir_name, cus_no, mat_no):
    one_dim_save_plot(x=data.ds, y=data.y, dir_name=dir_name, xlable="Date", ylable="Quantity",
                      title="Weekly_data", cus_no=cus_no, mat_no=mat_no)

def plot_monthly_data(data, dir_name, cus_no, mat_no):
    one_dim_save_plot(x=data.ds, y=data.y, dir_name=dir_name, xlable="Date", ylable="Quantity",
                      title="Monthly_data", cus_no=cus_no, mat_no=mat_no)

def plot_raw_data_pred(invoice_data, pred_data, dir_name, cus_no, mat_no):
    two_dim_save_plot(x1= invoice_data.date, y1= invoice_data.quantity,y1_label= "invoice_data",
                      x2= pred_data.delivery_date, y2=pred_data.cso_quantity, y2_label= "Prediction",
                      xlable= "Date", ylable= "Quantity", dir_name= dir_name,
                      title= "Raw Data vs Prediction", cus_no=cus_no, mat_no= mat_no)