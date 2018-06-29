import os
import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams
import matplotlib.patches as mpatches
rcParams['figure.figsize'] = 15, 6

# 2d Image saver function
def one_dim_save_plot(x, y, xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x, y, marker = "*", markerfacecolor = "red", markeredgecolor = "red", markersize=3.0)
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
    plt.plot(x1, y1, label = y1_label, marker = "*", markerfacecolor = "blue", markeredgecolor = "blue", markersize=3.0)
    plt.plot(x2, y2, label = y2_label, marker = "*", markerfacecolor = "red", markeredgecolor = "red", markersize=3.0)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    from dateutil import parser
    start = [parser.parse('20180101'), parser.parse('20180201')]
    end = [parser.parse('20180228'), parser.parse('20180328')]
    print(start)
    ax = plt.subplot()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)


def three_dim_save_plot(x1, y1, y1_label,
                        x2, y2, y2_label,
                        x3, y3, y3_label,
                        xlable, ylable, title, dir_name, cus_no, mat_no, **kwargs):

    # if 'y1_color' in kwargs.keys():
    #     y1_color = kwargs.get('y1_color')
    # else:
    #     y1_color = 'cyan'
    # if 'y2_color' in kwargs.keys():
    #     y2_color = kwargs.get('y2_color')
    # else:
    #     y2_color = 'orange'
    # if 'y3_color' in kwargs.keys():
    #     y3_color = kwargs.get('y3_color')
    # else:
    #     y3_color = 'green'

    fig = plt.figure()
    plt.plot(x1, y1, label = y1_label, marker = "*", markerfacecolor = "blue", markeredgecolor = "blue", markersize=3.0)
    plt.plot(x2, y2, label = y2_label, marker = "*", markerfacecolor = "red", markeredgecolor = "red", markersize=3.0)
    plt.plot(x3, y3, label = y3_label, marker = "*", markerfacecolor = "green", markeredgecolor = "green", markersize=3.0)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    if 'text' in kwargs.keys():
        text = kwargs.get('text')
        ax = plt.subplot()
        plt.text(0.5,0.95,text, horizontalalignment='center', verticalalignment='center', transform=ax.transAxes,
                 bbox=dict(facecolor='green', alpha=0.5))

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

def seven_dim_save_plot(x1, y1, y1_label,
                       x2, y2, y2_label,
                       x3, y3, y3_label,
                       x4, y4, y4_label,
                       x5, y5, y5_label,
                       x6, y6, y6_label,
                       x7, y7, y7_label,
                       xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x1, y1, label = y1_label)
    plt.plot(x2, y2, label = y2_label)
    plt.plot(x3, y3, label= y3_label)
    plt.plot(x4, y4, label= y4_label)
    plt.plot(x5, y5, label=y5_label)
    plt.plot(x6, y6, label=y6_label)
    plt.plot(x7, y7, label=y7_label)
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

def promotions_and_holidays_save_plots(x, y,
                                       xlable, ylable,
                                       promo_count, start_day_list, end_day_list, spnd_type,
                                       holidays_count, holiday_start_day_list, holidays_end_day_list, holiday_name,
                                       title, dir_name, cus_no, mat_no,
                                       plot_type = "holidays"):
    fig = plt.figure()
    plt.plot(x, y, marker="*", markerfacecolor="red", markeredgecolor="red", markersize=3.0)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)

    spnd_type_col = {'ZCMG' : '#7B241C', 'ZCPI' : '#C0392B', 'ZEDV': '#DAF7A6', 'ZEPI': '#FFC300',
                     'ZTLS': '#FF5733', 'ZVOL': '#C70039'}

    holiday_col = {'New Year Day' : '#E6194B', 'Memorial Day' : '#3CB44B', 'Independence Day' : '#FFE119',
                   'Labor Day' : '#0082C8', 'Thanksgiving Day' : '#F58231', 'Christmas Day' : '#911EB4'}

    ax = plt.subplot()
    all_patch = []
    if (plot_type == "promotions"):
        promo_type = []
        for i in range(promo_count):
            ax.axvspan(start_day_list[i], end_day_list[i], alpha=0.1, color= spnd_type_col.get(spnd_type[i]))

            if spnd_type[i] not in promo_type:
                promo_type = promo_type + [spnd_type[i]]
                patch = mpatches.Patch(color= spnd_type_col.get(spnd_type[i]), label= spnd_type[i])
                all_patch = all_patch + [patch]

    elif (plot_type == "holidays"):
        holiday_type = []
        for j in range(holidays_count):
            ax.axvspan(holiday_start_day_list.iloc[j], holidays_end_day_list.iloc[j], alpha=0.1, color=holiday_col.get(holiday_name[j]))

            if holiday_name[j] not in holiday_type:
                holiday_type = holiday_type + [holiday_name[j]]
                patch = mpatches.Patch(color=holiday_col.get(holiday_name[j]), label=holiday_name[j])
                all_patch = all_patch + [patch]

    plt.legend(handles= all_patch)

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)
