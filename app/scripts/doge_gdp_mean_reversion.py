import sys
from statistics import mean, stdev

from library.job import *
from library.source import BinanceData

CURRENT = 'current_price'
MEAN = 'mean_price'
SELL_LIMIT = 'sell_limit'
BUY_LIMIT = 'buy_limit'


def mean_reversion(job):
    # Request data
    job.set_status(JobStatus.REQUESTING)
    binance_configs = job.data_sources.get('binance')
    binance = BinanceData(binance_configs.get('api_key'), binance_configs.get('secret_key'))
    data = binance.request(
        job.variables.get('ticker'),
        job.variables.get('interval'),
        job.variables.get('look_back_seconds'),
        force_datetime=job.run_datetime
    )

    # Process data
    # TODO Check and handle bad data, e.g. not enough data points?
    current_level = float(data['close'].tail(1))
    level_series = data['close'][:-1]

    # Calculate
    job.set_status(JobStatus.CALCULATING)
    mean_level = mean(level_series)
    std_dev = stdev(level_series)
    sell_limit = mean_level + (std_dev * job.variables.get('sell_limit_modifier'))
    buy_limit = mean_level - (std_dev * job.variables.get('buy_limit_modifier'))

    # Store values
    job.store_value(CURRENT, current_level)
    job.store_value(MEAN, mean_level)
    job.store_value(SELL_LIMIT, sell_limit)
    job.store_value(BUY_LIMIT, buy_limit)

    # Generate signal
    if current_level > sell_limit:
        job.set_signal(Signal.SELL, (current_level - sell_limit) / current_level)
    elif current_level < buy_limit:
        job.set_signal(Signal.BUY, (buy_limit - current_level) / buy_limit)
    else:
        job.set_signal(Signal.HOLD)


def main():
    job = Job(*read_job_configs_from_command_line())
    job.do_work(mean_reversion)
    return job.finish_and_return_code(force_email=True)


if __name__ == '__main__':
    sys.exit(main())
