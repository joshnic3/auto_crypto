from library.database import DAO


class JobDAO(DAO):

    TABLE = 'Jobs'
    SCHEMA = [DAO.STANDARD_ID, 'datetime', 'name', 'status', 'seconds_elapsed']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)
        self.date_time_columns = ['datetime']


class SignalDAO(DAO):

    TABLE = 'Signals'
    SCHEMA = [DAO.STANDARD_ID, 'job_id', 'datetime', 'name', 'action', 'confidence']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)
        self.date_time_columns = ['datetime']


class StoredValuesDAO(DAO):

    TABLE = 'StoredValues'
    SCHEMA = [DAO.STANDARD_ID, 'job_id',  'datetime', 'key', 'value']

    def __init__(self, database_file_path):
        super().__init__(database_file_path)
        self.date_time_columns = ['datetime']