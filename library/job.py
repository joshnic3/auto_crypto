import argparse
import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from library.configuation import read_script_configs, read_global_configs
from library.constants import DT_FORMAT, PP_DT_FORMAT
from library.data import JobDAO, SignalDAO, StoredValuesDAO


def read_job_configs_from_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--global_configs', required=True, help='YAML global config file path')
    parser.add_argument('-s', '--script_configs', required=True, help='YAML script config file path')
    args = parser.parse_args()
    return read_global_configs(args.global_configs), read_script_configs(args.script_configs)


class Job:

    def __init__(self, global_configs, script_configs, force_datetime=None):
        self._stored_values = {}
        self.status = None
        self.error = None

        # Script configurations
        self.name = script_configs.get('name')
        self.email = script_configs.get('email')
        self.run_datetime = datetime.strptime(force_datetime, DT_FORMAT) if force_datetime else datetime.utcnow()
        self.variables = script_configs.get('variables')

        # Global configurations
        self._dao = JobDAO(global_configs.get('db_path'))
        self._emailer = Emailer(*[global_configs.get('emailer').get(c) for c in ['host', 'from_address', 'password']])
        self._log = self._initialise_log(global_configs.get('log_directory'), show_in_console=True)
        self.data_sources = global_configs.get('data_sources')

        # Create job and signal in database
        self.id = self._dao.new({
            'datetime': self.run_datetime.strftime(DT_FORMAT),
            'name': self.name
        })
        self.signal = Signal(self._dao.database_file_path, self.id, self.name)

        # Inform user
        self._log.info('Configs: {}'.format(self.variables))
        if force_datetime:
            self._log.info('Forcing run datetime as {}'.format(self.run_datetime.strftime(PP_DT_FORMAT)))
        self.set_status(JobStatus.INITIATED)

    def _initialise_log(self, log_directory, debug=False, show_in_console=False):
        # Generate log name
        log_file_name = '{}_{}.log'.format(self.run_datetime.strftime('%Y%m%d%H%M%S'), self.name)

        # Setup logging to file.
        if not os.path.isdir(log_directory):
            os.mkdir(log_directory)
        log_file_path = os.path.join(log_directory, log_file_name)  # will create log obj

        logging.root.handlers = []
        log_format = '%(asctime)s|%(levelname)s : %(message)s'
        if debug:
            logging.basicConfig(level='DEBUG', format=log_format, filename=log_file_path)
        else:
            logging.basicConfig(level='INFO', format=log_format, filename=log_file_path)

        # Setup logging to console.
        if show_in_console:
            console = logging.StreamHandler()
            formatter = logging.Formatter(log_format)
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)

        return logging.getLogger('')

    def set_status(self, status):
        self.status = status
        if JobStatus.is_valid(status):
            seconds_elapsed = (datetime.utcnow() - self.run_datetime).total_seconds()
            if status == JobStatus.FAILED:
                self._log.warning('Job status: "{}"'.format(status))
            else:
                self._log.info('Job status: "{}"'.format(status))
            self._dao.update(self.id, {'status': status, 'seconds_elapsed': seconds_elapsed})

    def store_value(self, key, value):
        if key not in self._stored_values.keys():
            self._stored_values[key] = StoredValue(self._dao.database_file_path, self.id, key, self.variables.get('round_dp'))
        stored_value = self._stored_values.get(key)
        stored_value.set(value)
        self._log.info('Stored value: {}'.format(stored_value))

    def read_value(self, key):
        stored_value = self._stored_values.get(key)
        if stored_value is None:
            raise Exception('No stored value with the key "{}" exists!'.format(key))
        return stored_value.value

    def get_all_stored_values(self):
        return list(self._stored_values.values())

    def set_signal(self, action, confidence=0):
        self.signal.set(action, confidence)
        self._log.info('Signal: {}'.format(self.signal))

    def do_work(self, calculation_function):
        self.error = None
        try:
            calculation_function(self)
        except Exception as error:
            self.error = str(error)

    def finish_and_return_code(self, force_email=False):
        if self.error:
            self.set_status(JobStatus.FAILED)
            self._log.warning('Error: {}'.format(self.error))
            self._emailer.job_failure(self.email, self)
            self._log.info('Sent error to {}'.format(self.email))
            return JobStatus.FAIL_CODE
        else:
            self.set_status(JobStatus.SUCCESS)
            if self._emailer:
                if self.signal.has_changed() or force_email:
                    report = Report(self)
                    self._emailer.signal_change(self.email, self.signal, report)
                    self._log.info('Sent signal and report to {}'.format(self.email))
                else:
                    self._log.info('Signal action hasn\'t changed, so won\'t send email')
            return JobStatus.SUCCESS_CODE

    def as_dict(self):
        return {'id': self.id, 'name': self.name, 'run_datetime': self.run_datetime, 'status': self.status}


class JobStatus:

    SUCCESS_CODE = 0
    FAIL_CODE = 1
    INITIATED = 'INITIATED'
    REQUESTING = 'REQUESTING_DATA'
    CALCULATING = 'CALCULATING'
    FAILED = 'FAILED'
    SUCCESS = 'SUCCESS'
    VALID = [INITIATED, REQUESTING, CALCULATING, FAILED, SUCCESS]

    @staticmethod
    def is_valid(status_string):
        if status_string.upper() not in JobStatus.VALID:
            raise Exception('{} is an invalid status!')
        return True


class StoredValue:

    def __init__(self, database_file_path, job_id, key, round_dp=None):
        self._dao = StoredValuesDAO(database_file_path)
        self._round_dp = round_dp
        self.job_id = job_id
        self.key = key
        self.value = None
        self._load()

    def __str__(self):
        return '{}: {}'.format(self.key, self.value)

    def _load(self):
        value_rows = self._dao.read(None, where_condition={'key': self.key}, sortby='datetime')
        if value_rows:
            latest_value = value_rows[0]
            self.id = latest_value[0]
            self.action = latest_value[self._dao.get_column_index('key')]
            self.confidence = latest_value[self._dao.get_column_index('value')]

    def _round(self, number):
        if self._round_dp:
            return round(number, self._round_dp)
        return number

    def set(self, value):
        self.value = self._round(value) if isinstance(value, float) else value
        self._dao.new({
            'job_id': self.job_id,
            'datetime': datetime.utcnow().strftime('%Y%m%d%H%M%S'),
            'key': self.key,
            'value': value
        })

    def as_dict(self):
        return {'key': self.key, 'value': self.value}


class Signal:

    HOLD = 'HOLD'
    SELL = 'SELL'
    BUY = 'BUY'

    def __init__(self, database_file_path, job_id, name):
        self._dao = SignalDAO(database_file_path)
        self._job_id = job_id
        self._previous_action = None

        self.name = name
        self.action = None
        self.confidence = None

        self._load()

    def __str__(self):
        return '{}: {}, {}'.format(self.name, self.action, self.confidence)

    def _load(self):
        signal_rows = self._dao.read(None, where_condition={'name': self.name}, sortby='datetime')
        if signal_rows:
            latest_signal = signal_rows[0]
            self.action = latest_signal[self._dao.get_column_index('action')]
            self.confidence = latest_signal[self._dao.get_column_index('confidence')]

    def set(self, action, confidence):
        if action not in [Signal.HOLD, Signal.SELL, Signal.BUY]:
            raise Exception('Signal status must be either "hold", "sell" or "buy"!')
        if not (isinstance(confidence, float) or isinstance(confidence, int)) or 0 >= confidence >= 1:
            raise Exception('Confidence must be a float (0-1)')
        self._previous_action = action
        self.action = action
        self.confidence = round(confidence, 2)
        self._dao.new({
            'job_id': self._job_id,
            'name': self.name,
            'datetime': datetime.utcnow().strftime('%Y%m%d%H%M%S'),
            'action': self.action,
            'confidence': self.confidence
        })

    def has_changed(self):
        return self._previous_action != self.action

    def as_dict(self):
        return {'action': self.action, 'confidence': self.confidence}


class Emailer:

    PORT = 465

    def __init__(self, host, from_address, password):
        self._host = host
        self._password = password
        self.from_address = from_address

    def _send_email(self, to_address, subject, text=None, html=None):
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = self.from_address
        message['To'] = to_address
        if text:
            message.attach(MIMEText(text, 'plain'))
        if html:
            message.attach(MIMEText(html, 'html'))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self._host, Emailer.PORT, context=context) as server:
            server.login(self.from_address, self._password)
            server.sendmail(self.from_address, to_address, message.as_string())

    def signal_change(self, to, signal, report):
        subject = 'NEW SIGNAL: {}'.format(signal.name)
        self._send_email(to, subject, html=report.html())

    def job_failure(self, to, job):
        subject = 'JOB FAILED: {}'.format(job.name)
        content_parts = [
            'Id: {}'.format(job.id),
            'Name: {}'.format(job.name),
            'Run Time: {}'.format(job.run_datetime.strftime('%d-%m-%Y %H:%M:%S')),
            'Status: {}<br>'.format(job.status),
            'Configs: {}<br>'.format(job.variables),
            'Error Message: {}'.format(job.error)
        ]
        content = '\n'.join(content_parts)
        self._send_email(to, subject, content)


class Report:

    STORED_VALUES = 'stored_values'
    SIGNAL = 'signal'
    JOB = 'job'
    CONFIGS = 'configs'

    def __init__(self, job):
        self._report_dict = {
            Report.STORED_VALUES: [sv.as_dict() for sv in job.get_all_stored_values()],
            Report.SIGNAL: [job.signal.as_dict()],
            Report.JOB: [job.as_dict()]
        }

    @staticmethod
    def _dict_to_html_row(row_dict, columns):
        html_parts = ['<tr>']
        for column in columns:
            style_str = ''
            if isinstance(row_dict.get(column), float) or isinstance(row_dict.get(column), int):
                style_str = 'font-family:\'Courier New\''
            html_parts.append('<td style="{}">{}</td>'.format(style_str, row_dict.get(column)))
        html_parts.append('</tr>')
        return ''.join(html_parts)

    def _list_to_html_table(self, rows, columns):
        html_parts = ['<table style="border:1px solid black;">']
        for column in columns:
            html_parts.append('<th>{}</th>'.format(column))
        html_parts.append('</tr>')
        for row in rows:
            html_parts.append(self._dict_to_html_row(row, columns))
        html_parts.append('</table>')
        return ''.join(html_parts)

    @staticmethod
    def _html_title(text, tag='h5'):
        return '<{}>{}</{}>'.format(tag, text, tag)

    def _report_html(self, report_type, title, columns):
        signal_dict = self._report_dict.get(report_type)
        if signal_dict:
            return ''.join([
                self._html_title(title),
                self._list_to_html_table(signal_dict, columns)
            ])
        return ''

    def html(self):
        return ''.join([
            self._report_html(Report.SIGNAL, 'New Signal', ['action', 'confidence']),
            self._report_html(Report.STORED_VALUES, 'Stored Values', ['key', 'value']),
            self._report_html(Report.JOB, 'Job', ['id', 'name', 'run_datetime', 'status'])
        ])
