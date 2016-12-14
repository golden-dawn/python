from datetime import datetime
from io import BytesIO
import pycurl

class MypivotsSplits :

    fname      = 'C:/goldendawn/mypivots.html'
    out_fname  = 'C:/goldendawn/mypivots_splits.txt'
    prefix     = '<a style="padding-right:10px" href="/stock-split-calendar/' \
                          'ticker/'
    url_base   = 'http://www.mypivots.com/stock-split-calendar/ticker'
    start_row  = '<tr style="text-align:center">'
    end_row    = '</tr>'
      
    def get_all(self) :
        with open(self.fname, 'r') as ifile :
            lines = ifile.readlines()
        # found     = False
        for line in lines:
            if line.startswith(self.prefix) :
                ixx        = line.find('">')
                if ixx == -1 :
                    print('WRONG LINE: {0:s}'.format(line))
                    continue
                url_suffix = line[len(self.prefix): ixx]
                ticker     = line[ixx + 2 : line.find('</a>')]
                # if ticker == 'NRGY' :
                #     found  = True
                # if found :
                #     self.store_splits(ticker, url_suffix)
                self.store_splits(ticker, url_suffix)
                print('Got splits for {0:s}'.format(ticker))

    def store_splits(self, ticker, url_suffix) :
        with open(self.out_fname, 'a') as ofile :
            res_buffer          = BytesIO()
            c                   = pycurl.Curl()
            c.setopt(c.URL, '{0:s}/{1:s}'.format(self.url_base, url_suffix))
            c.setopt(c.WRITEDATA, res_buffer)
            c.perform()
            c.close()
            res                 = res_buffer.getvalue().decode('iso-8859-1')
            lines               = res.split('\n')
            started             = False
            str_buffer          = ''
            for line in lines:
                line            = line.strip()
                if not (started or line == self.start_row) :
                    continue
                if line == self.start_row :
                    str_buffer  = ''
                    started     = True
                elif line == self.end_row :
                    tokens      = str_buffer[4:-5].split('</td><td>')
                    split_date  = str(datetime.strptime(tokens[1],
                                                        '%m/%d/%Y').date())
                    denom, num  = tokens[3].split('-')
                    ratio       = round(float(num) / float(denom), 2)
                    ofile.write('{0:s}\t{1:s}\t{2:.2f}\n'.\
                                format(ticker, split_date, ratio))
                    started     = False
                else :
                    str_buffer += line.strip()
            

    def parse_file(self) :
        fname      = 'C:/goldendawn/AAON.html'
        with open(fname, 'r') as ifile :
            lines  = ifile.readlines()
        started    = False
        str_buffer = ''
        ticker     = 'AAON'
        with open(self.out_fname, 'a') as ofile :
            for line in lines:
                line   = line.strip()
                if not (started or line == self.start_row) :
                    continue
                if line == self.start_row :
                    str_buffer = ''
                    started    = True
                elif line == self.end_row :
                    tokens     = str_buffer[4:-5].split('</td><td>')
                    split_date = str(datetime.strptime(tokens[1],
                                                       '%m/%d/%Y').date())
                    denom, num = tokens[3].split('-')
                    ratio      = round(float(num) / float(denom), 2)
                    print('date = {0:s}, ratio = {1:f}'.format(split_date, ratio))
                    ofile.write('{0:s}\t{1:s}\t{2:.2f}\n'.\
                                format(ticker, split_date, ratio))
                    started    = False
                else :
                    str_buffer += line.strip()
    