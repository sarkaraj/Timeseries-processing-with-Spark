import os
import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15, 6

# 2d Image saver function
def one_dim_save_plot(x, y, xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x, y)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

def two_dim_save_plot(x1, y1, y1_label,
                      x2, y2, y2_label,
                      xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x1, y1, label = y1_label)
    plt.plot(x2, y2, label = y2_label)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)


def three_dim_save_plot(x1, y1, y1_label,
                        x2, y2, y2_label,
                        x3, y3, y3_label,
                        xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x1, y1, label = y1_label)
    plt.plot(x2, y2, label = y2_label)
    plt.plot(x3, y3, label = y3_label)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

def four_dim_save_plot(x1, y1, y1_label,
                       x2, y2, y2_label,
                       x3, y3, y3_label,
                       x4, y4, y4_label,
                       xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x1, y1, label = y1_label)
    plt.plot(x2, y2, label = y2_label)
    plt.plot(x3, y3, label= y3_label)
    plt.plot(x4, y4, label= y4_label)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

def weekly_ensm_model_error_plots(output_result, dir_name, cus_no, mat_no):

    fig = plt.figure()
    plt.plot(output_result.ds[2:], output_result.Error_Cumsum_arima[2:], label='ARIMA')
    plt.plot(output_result.ds[2:], output_result.Error_Cumsum_prophet[2:], label='Prophet')
    plt.plot(output_result.ds[2:], output_result.Error_Cumsum[2:], label='Ensembled')
    plt.title("% Cumulative Error")

    plt.xlabel('Date')
    plt.ylabel('% Cumulative Error')
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_cum_error.png")

    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

    fig = plt.figure()
    plt.plot(output_result.ds, output_result.rolling_6week_percent_error_arima, label='ARIMA')
    plt.plot(output_result.ds, output_result.rolling_6week_percent_error_prophet, label='Prophet')
    plt.plot(output_result.ds, output_result.rolling_6week_percent_error, label='Ensembled')
    plt.title("Six Week Rolling % Error")

    plt.xlabel('Date')
    plt.ylabel('% Rolling Error')
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_sixweek_rolling_error.png")

    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

    fig = plt.figure()
    plt.plot(output_result.ds, output_result.rolling_12week_percent_error_arima, label='ARIMA')
    plt.plot(output_result.ds, output_result.rolling_12week_percent_error_prophet, label='Prophet')
    plt.plot(output_result.ds, output_result.rolling_12week_percent_error, label='Ensembled')
    plt.title("Twelve Week Rolling % Error")

    plt.xlabel('Date')
    plt.ylabel('% Rolling Error')
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_twelveweek_rolling_error.png")

    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)