import datetime
import os
import sqlite3
from bdrocfl import ocfl


def get_env_variable(var):
    try:
        return os.environ[var]
    except KeyError:
        raise Exception(f'please set the {var} environment variable')


def check_objects(storage_root, db_conn, top_ntuple_segment='000'):
    for pid in ocfl.walk_repo(storage_root, top_ntuple_segment=top_ntuple_segment):
        now = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
        try:
            obj = ocfl.Object(storage_root, pid, deleted_ok=True)
            ocfl.check_fixity(obj)
            db_conn.execute('INSERT INTO checks (timestamp, pid, result) VALUES (?, ?, ?)',
                    (now, pid, 'pass')
                )
        except Exception as e:
            db_conn.execute('INSERT INTO checks (timestamp, pid, result) VALUES (?, ?, ?)',
                    (now, pid, f'ERR: {e}')
                )
        db_conn.commit()


if __name__ == '__main__':
    OCFL_ROOT = get_env_variable('OCFL_ROOT')
    DB_NAME = get_env_variable('DB_NAME')
    db_conn = sqlite3.connect(DB_NAME)
    check_objects(OCFL_ROOT, db_conn)
