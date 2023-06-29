import datetime
import logging
import os
import sqlite3
import time
from bdrocfl import ocfl


def get_env_variable(var):
    try:
        return os.environ[var]
    except KeyError:
        raise Exception(f'please set the {var} environment variable')


def setup_logger(file_name):
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def send_error_email(msg, server, mail_server, notification_email_address):
    import smtplib
    from email.mime.text import MIMEText
    s = smtplib.SMTP(mail_server)
    email_msg = MIMEText(msg)
    email_msg['Subject'] = f'Validation error on {server}'
    email_msg['From'] = f'validation@{server}'
    email_msg['To'] = notification_email_address
    s.sendmail(f'validation@{server}', [notification_email_address], email_msg.as_string())


def populate_dir_names(db_conn):
    chars = '0123456789abcdef'
    for i in chars:
        for j in chars:
            for k in chars:
                db_conn.execute('INSERT INTO history (dir_name) VALUES (?)', (f'{i}{j}{k}',))
    db_conn.commit()


def init_db(db_conn):
    db_conn.execute('CREATE TABLE checks (timestamp TEXT NOT NULL, pid TEXT NOT NULL, result TEXT NOT NULL)')
    db_conn.execute('CREATE TABLE history (dir_name TEXT NOT NULL UNIQUE, timestamp TEXT NULL)')
    db_conn.commit()
    populate_dir_names(db_conn)


def get_dir_names(db_conn, num):
    return [r[0] for r in db_conn.execute(f'select dir_name from history order by timestamp,dir_name limit {num}').fetchall()]


def set_dir_name_timestamp(db_conn, dir_name, ts=None):
    if not ts:
        ts = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
    db_conn.execute('UPDATE history SET timestamp = ? WHERE dir_name = ?', (ts, dir_name))
    db_conn.commit()


def check_objects(storage_root, db_conn, top_ntuple_segment='000', sleep_seconds=1):
    # logger = setup_logger(os.path.join(LOG_DIR, 'validation.log'))
    invalid_objects = set()
    for pid in ocfl.walk_repo(storage_root, top_ntuple_segment=top_ntuple_segment):
        now = datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
        try:
            obj = ocfl.Object(storage_root, pid, deleted_ok=True)
            ocfl.check_fixity(obj)
            db_conn.execute('INSERT INTO checks (timestamp, pid, result) VALUES (?, ?, ?)',
                    (now, pid, 'pass')
                )
            # test_val = 1/0
        except Exception as e:
            logger.debug( f'problem found with pid, ``{pid}``')
            invalid_objects.add(pid)
            db_conn.execute('INSERT INTO checks (timestamp, pid, result) VALUES (?, ?, ?)',
                    (now, pid, f'ERR: {e}')
                )
        db_conn.commit()
        time.sleep(sleep_seconds)
    return invalid_objects


if __name__ == '__main__':
    '''
    The top-level of the BDR repo has 4096 data directories:
        000, 001, 002, ..., 100, 101, ... ffe, fff
    These directories divide the objects into buckets, and we can specify which top-level data directory we want to process in ocfl.walk_repo(). So, by choosing 1, 10, 100, ... top-level buckets, we can control what percentage of the BDR we want to check.

    If we put the 4096 directory names into a table, and then record the timestamp when it gets processed, we can select the x oldest directories each time we run this script, so we continually loop through the whole BDR.
    '''
    OCFL_ROOT = get_env_variable('OCFL_ROOT')
    DB_NAME = get_env_variable('DB_NAME')
    LOG_DIR = get_env_variable('LOG_DIR')
    SERVER = get_env_variable('SERVER')
    MAIL_SERVER = get_env_variable('MAIL_SERVER')
    NOTIFICATION_EMAIL_ADDRESS = get_env_variable('NOTIFICATION_EMAIL_ADDRESS')
    NUM_DIRECTORIES = 1 #24 #out of 4096 => this would go through the whole BDR in 171 days

    logger = setup_logger(os.path.join(LOG_DIR, 'validation.log'))
    db_conn = sqlite3.connect(DB_NAME)
    directories = get_dir_names(db_conn, NUM_DIRECTORIES)
    all_invalid_objects = set()
    for d in directories:
        logger.info(f'processing {d}')
        invalid_objects = check_objects(OCFL_ROOT, db_conn, top_ntuple_segment=d)
        all_invalid_objects.update(invalid_objects)
        set_dir_name_timestamp(db_conn, d)
    if all_invalid_objects:
        msg = f'invalid objects: {all_invalid_objects}'
        logger.error(msg)
        send_error_email(msg, SERVER, MAIL_SERVER, NOTIFICATION_EMAIL_ADDRESS)
