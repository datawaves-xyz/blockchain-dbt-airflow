import copy
import csv
from dataclasses import dataclass
from datetime import datetime
from typing import List

import pendulum as pdl
from coinpaprika import client as Coinpaprika
from ethereumetl.progress_logger import ProgressLogger

from ethereum_airflow.token import TokenProvider, Token

iso_format = '%Y-%m-%dT%H:%M:%SZ'
minutes_format = '%Y-%m-%d %H:%M'
day_format = '%Y-%m-%d'

price_attrs = ['minute', 'price', 'decimals', 'contract_address', 'symbol', 'dt']


@dataclass
class PriceRecord:
    minute: str
    price: float
    decimals: int
    contract_address: str
    symbol: str
    dt: str

    def copy_it_with_datetime(self, time: pdl.datetime) -> 'PriceRecord':
        other = copy.copy(self)
        other.minute = time.strftime(minutes_format)
        other.dt = time.strftime(day_format)
        return other


class PriceProvider:
    def __init__(self, token_provider: TokenProvider):
        self.token_provider = token_provider
        self.progress_logger = ProgressLogger()

    def get_single_token_daily_price(
            self, token: Token, start: int, end: int
    ) -> List[PriceRecord]:
        raise NotImplementedError()

    def create_temp_csv(
            self, output_path: str, start: int, end: int
    ) -> None:
        tokens = self.token_provider.get_tokens()
        self.progress_logger.start(total_items=len(tokens))

        with open(output_path, 'w') as csvfile:
            spam_writer = csv.DictWriter(csvfile, fieldnames=price_attrs)
            spam_writer.writeheader()

            for token in tokens:
                if token.end is not None:
                    end_at = int(token.end.timestamp())
                    if end_at < end:
                        continue

                spam_writer.writerows([i.__dict__ for i in self.get_single_token_daily_price(token, start, end)])
                self.progress_logger.track()

        self.progress_logger.finish()


class CoinpaprikaPriceProvider(PriceProvider):
    @staticmethod
    def _copy_record_across_interval(
            record: PriceRecord, interval: int
    ) -> List[PriceRecord]:
        start = pdl.from_format(record.minute, minutes_format)
        end = start.add(minutes=interval)
        records = []

        while start < end:
            records.append(record.copy_it_with_datetime(start))
            start = start.add(minutes=1)

        return records

    def get_single_token_daily_price(
            self, token: Token, start: int, end: int
    ) -> List[PriceRecord]:
        client = Coinpaprika.Client()
        records = []
        res = client.historical(coin_id=token.id, start=start, end=end, limit=5000)

        for item in res:
            time = pdl.instance(datetime.strptime(item['timestamp'], iso_format))
            records += self._copy_record_across_interval(
                record=PriceRecord(
                    minute=time.strftime(minutes_format),
                    price=item['price'],
                    decimals=token.decimals,
                    contract_address=token.address,
                    symbol=token.symbol,
                    dt=time.strftime(day_format)
                ),
                interval=5
            )

        return records
