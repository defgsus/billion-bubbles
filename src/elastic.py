from elastipy import Exporter


class StockChartExporter(Exporter):

    INDEX_NAME = "nasdaq-charts"

    MAPPINGS = {
        "properties": {
            "timestamp": {"type": "date"},
            "first_timestamp": {"type": "date"},
            "last_timestamp": {"type": "date"},

            "symbol": {"type": "keyword"},
            "name": {"type": "keyword"},
            "region": {"type": "keyword"},
            "sector": {"type": "keyword"},
            "industry": {"type": "keyword"},

            "value": {"type": "float"},
            "volume": {"type": "long"},
            "open": {"type": "float"},
            "close": {"type": "float"},
            "high": {"type": "float"},
            "low": {"type": "float"},
            "day_change": {"type": "float"},
            "abs_day_change": {"type": "float"},
            "hub": {"type": "float"},
        }
    }

    def get_document_id(self, es_data: dict) -> str:
        return f'{es_data["symbol"]}-{es_data["timestamp"]}'

    def transform_document(self, data: dict):
        data["day_change"] = data["close"] - data["open"]
        data["abs_day_change"] = abs(data["day_change"])
        data["hub"] = data["high"] - data["low"]
        return data
