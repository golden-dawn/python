import numpy as np
import matplotlib
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
from mpl_finance import candlestick_ochl as candlestick
from mpl_finance import volume_overlay3
from matplotlib.dates import num2date
from matplotlib.dates import date2num
import matplotlib.mlab as mlab
import datetime
from matplotlib.ticker import Formatter

class MyFormatter(Formatter):
    def __init__(self, dates, fmt='%Y-%m-%d'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(np.round(x))
        if ind >= len(self.dates) or ind < 0:
            return ''

        return num2date(self.dates[ind]).strftime(self.fmt)

datafile = 'data.csv'
r = mlab.csv2rec(datafile, delimiter=' ')

candlesticks = zip(date2num(r['date']), r['open'], r['close'], 
                   r['high'], r['low'], r['volume'])

formatter = MyFormatter(r['date'])

# candlesticks = zip(date2num(r['date']), r['open'], r['high'], r['low'],
#                    r['close'], r['volume'])

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.xaxis.set_major_formatter(formatter)
ax.set_ylabel('Quote ($)', size=20)
fig.autofmt_xdate()
candlestick(ax, candlesticks, width=1, colorup='g', colordown='r')
fig.autofmt_xdate()
fig.savefig('aapl1.png')


# shift y-limits of the candlestick plot so that there is space at the
# bottom for the volume bar chart
pad = 0.25
yl = ax.get_ylim()
print('yl = {}'.format(str(yl)))
ax.set_ylim(yl[0] - (yl[1] - yl[0]) * pad, yl[1])

# create the second axis for the volume bar-plot
ax2 = ax.twinx()

# set the position of ax2 so that it is short (y2=0.32) but otherwise
# the same size as ax
ax2.set_position(matplotlib.transforms.Bbox([[0.125, 0.1], [0.9, 0.32]]))

# get data from candlesticks for a bar plot
dates = [x[0] for x in candlesticks]
dates = np.asarray(dates)
volume = [x[5] for x in candlesticks]
volume = np.asarray(volume)

# make bar plots and color differently depending on up/down for the day
pos = r['open'] - r['close'] < 0
neg = r['open'] - r['close'] > 0
ax2.bar(dates[pos], volume[pos], color='green', width=1, align='center')
ax2.bar(dates[neg], volume[neg], color='red', width=1, align='center')

#scale the x-axis tight
ax2.set_xlim(min(dates), max(dates))
# the y-ticks for the bar were too dense, keep only every third one
yticks = ax2.get_yticks()
ax2.set_yticks(yticks[::3])

ax2.yaxis.set_label_position("right")
ax2.set_ylabel('Volume', size=20)

# format the x-ticks with a human-readable date. 
xt = ax.get_xticks()
new_xticks = [datetime.date.isoformat(num2date(d)) for d in xt]
ax.set_xticklabels(new_xticks,rotation=45, horizontalalignment='right')

fig.savefig('aapl.png')

plt.ion()
plt.show()
