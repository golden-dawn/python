from datetime import datetime


# This function converts the NYSE holidays (retrieved from
# http://nyseholidays.blogspot.com/) to a more readable format.  Also,
# I don't care about Saturday data, I will always count Saturdays as
# holidays because they are part of the weekend
def format_nyse_holidays(fname='nyse_holidays.txt'):
    with open(fname, 'r') as f:
        lines = f.readlines()
    lst = []
    for line in lines:
        line = line.strip()
        if line not in ['', 'Date', 'WeekDay', 'Holiday']:
            lst.append(line)
    dates, weekdays, hol_names = lst[::3], lst[1::3], lst[2::3]
    new_lst = []
    for (dt, wd, hn) in zip(dates, weekdays, hol_names):
        if wd == 'Saturday':
            continue
        new_lst.append('{0:s},{1:s},{2:s}\n'.
                       format(str(datetime.strptime(dt, '%d %b %Y').date()),
                              wd, hn))
    with open(fname.replace('.txt', '.csv'), 'w') as ofile:
        for line in new_lst:
            ofile.write(line)
