import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import os
import time
rcParams['figure.figsize'] = 15, 6

def plot_count_hist(data, field, title, num_bar, image_dir):
    """
    plot the histogram of count
    :param data: df
    :param field: feature name(string)
    :param num_bar: bar count
    :param image_dir: dir name
    :return: none
    """
    fig = plt.figure()
    ax =data[field].value_counts().sort_index().plot(kind = 'bar',grid=True, color='#607c8e')
    plt.title(title)
    plt.xlabel('Absolute Error Quantity(cs)')
    plt.ylabel('Count')
    plt.grid(axis='y', alpha=0.75)
    # create a list to collect the plt.patches data
    totals = []

    # find the values and append to list
    for i in ax.patches:
        totals.append(i.get_height())

    # set individual bar lables using above list
    total = sum(totals)

    for i in ax.patches[:(num_bar + 1)]:
        # get_x pulls left or right; get_height pushes up or down
        ax.text(i.get_x()-.03, i.get_height()+ 5.0, \
                str(round((i.get_height()/total)*100, 2))+'%', fontsize=13,
                    color='dimgrey')
    ax.set_xlim(left=None, right= num_bar + 0.5)
    save_file = os.path.join(image_dir, title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)