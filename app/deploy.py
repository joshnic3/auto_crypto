import os
import sys

from library.data import *
from library.database import DatabaseInitiator

WORKING_DIR = '/Users/joshnicholls/Desktop/auto_crypto/'
DAO_LIST = [JobDAO, SignalDAO, StoredValuesDAO]


def main():
    db_path = os.path.join(WORKING_DIR, 'data.db')
    db_initiator = DatabaseInitiator(db_path, DAO_LIST)
    db_initiator.create_tables()


if __name__ == '__main__':
    sys.exit(main())
