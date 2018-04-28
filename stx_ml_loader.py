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

    def get_data(self):
        train_data = self.get_dataset(self.train_q)
        valid_data = self.get_dataset(self.valid_q)
        test_data = self.get_dataset(self.test_q)
        return train_data, valid_data, test_data

    def get_dataset(self, query):
        db_data = stxdb.db_read_cmd(query)
        ml_data = []
        for x in db_data:
            data = np.array(x[4:])
            result = self.get_result(x)
            ml_data.append(tuple(data, result))
        return ml_data

    def get_result(self, x):
        res = [0] * 13
        x_ix = 2 if self.num_days == 3 else 3
        cat = x[x_ix]
        ixx = int(2 * float(cat) + 6)
        res[ixx] = 1
        return np.array(res)
