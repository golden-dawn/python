import math
import numpy as np
import stxdb


class StxMlLoader:

    def __init__(
            self, num_days=3,
            train_q="select * from ml where stk like 'R%' and dt<'2002-02-08'",
            valid_q="select * from ml where stk like 'Q%' and dt<'2002-02-08'",
            tst_q="select * from ml where stk like 'R%' and dt>='2002-02-08'"):
        self.train_q = train_q
        self.valid_q = valid_q
        self.test_q = tst_q
        self.num_days = num_days

    def get_data(self, n=None):
        train_data = self.get_dataset(self.train_q, n, True)
        valid_data = self.get_dataset(self.valid_q, n, False)
        test_data = self.get_dataset(self.test_q, n, False)
        return train_data, valid_data, test_data

    def get_dataset(self, query, n, is_training):
        db_data = stxdb.db_read_cmd(query)
        ml_data = []
        total = 0
        dropped = 0
        ub = n if n is None else (n + 4)
        for x in db_data:
            total += 1
            arr = np.array(x[4: ub])
            if not np.isfinite(arr).all():
                dropped += 1
                continue
            data = np.array([np.array([y], dtype=np.float32)
                             for y in x[4: ub]])
            result = self.get_result_new(x, is_training)
            ml_data.append(tuple([data, result]))
        print('Found {0:d} records, dropped {1:d}'.format(total, dropped))
        return ml_data

    def get_result_new(self, x, is_training):
        res = [0.0] * 3
        x_ix = 2 if self.num_days == 3 else 3
        cat = x[x_ix]
        if cat < 0:
            ixx = 0
        elif cat == 0:
            ixx = 1
        else:
            ixx = 2
        ixx = int(ixx)
        res[ixx] = 1.0
        return np.array([np.array([y]) for y in res]) if is_training else ixx
    
    def get_result(self, x, is_training):
        res = [0.0] * 13
        x_ix = 2 if self.num_days == 3 else 3
        cat = x[x_ix]
        ixx = int(2 * float(cat) + 6)
        res[ixx] = 1.0
        return np.array([np.array([y]) for y in res]) if is_training else ixx
