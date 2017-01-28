import stxdb
import sys

this = sys.modules[__name__]


def apply_adjustments(adj_fname):
    with open(adj_fname, 'r+') as f:
        lines = f.readlines()
        write_lines = []
        for line in lines:
            if line.startswith('#'):
                write_lines.append(line.strip())
                continue
            sql = line.strip()
            try:
                stxdb.db_write_cmd(sql)
                sql = '# ' + sql
            except:
                print('Failed to execute {0:s}\n  Error: {1:s}'.
                      format(sql, str(sys.exc_info()[1])))
            write_lines.append(sql)
        f.seek(0)
        f.write('\n'.join(write_lines))
        f.truncate()


# Call this function with an argument that is the name of the adjustment file.
# This file should be in the adjustments directory
if __name__ == '__main__':
    args = None
    if len(sys.argv) > 1:
        args = sys.argv[1:]
    else:
        print('Usage: {0:s} <name of the adjustment file'.format(sys.argv[0]))
        exit(1)
    adj_fname = 'adjustments/{0:s}'.format(sys.argv[1])
    apply_adjustments(adj_fname)
