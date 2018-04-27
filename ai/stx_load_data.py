import stxdb


class StxMlLoader:

    def __init__(
            self,
            train_q="select * from ml where stk like 'R%' and dt<'2002-02-08'",
            valid_q="select * from ml where stk like 'Q%' and dt<'2002-02-08'",
            tst_q="select * from ml where stk like 'R%' and dt>='2002-02-08'"):
        self.train_q = train_q
        self.valid_q = valid_q
        self.test_q = tst_q

    def get_data(self):
        train_data = stxdb.db_read_cmd(self.train_q)
        valid_data = stxdb.db_read_cmd(self.valid_q)
        test_data = stxdb.db_read_cmd(self.test_q)
        return train_data, valid_data, test_data
