import sys
import time
import json
import datetime
from pathlib import Path
from typing import Union, Generator, Tuple, Optional, Type, Iterable, List

from tqdm import tqdm
from sqlalchemy import (
    create_engine, select,
    Column, String, Integer, ForeignKey, JSON, Date, DateTime
)
from sqlalchemy.orm import relationship, backref, sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError

from .nasdaq_api import NasdaqApi
from .util import get_path

NasdaqDBBase = declarative_base()


class CompanyProfile(NasdaqDBBase):
    __tablename__ = 'company_profile'
    symbol = Column(String(length=10), primary_key=True)
    timestamp = Column(DateTime)
    data = Column(JSON)


class StockChart(NasdaqDBBase):
    __tablename__ = 'stock_chart'
    key = Column(String(length=48), primary_key=True)
    symbol = Column(String(length=10))
    asset_class = Column(String(length=16))
    date_from = Column(Date)
    date_to = Column(Date)
    timestamp = Column(DateTime)
    data = Column(JSON)


class CompanyHolders(NasdaqDBBase):
    __tablename__ = 'company_holders'
    symbol = Column(String(length=10), primary_key=True)
    type = Column(String(length=16))
    timestamp = Column(DateTime)
    data = Column(JSON)


class CompanyInsiders(NasdaqDBBase):
    __tablename__ = 'company_insiders'
    symbol = Column(String(length=10), primary_key=True)
    timestamp = Column(DateTime)
    data = Column(JSON)


class InstitutionPositions(NasdaqDBBase):
    __tablename__ = 'institutional_positions'
    id = Column(Integer, primary_key=True)
    type = Column(String(length=16))
    timestamp = Column(DateTime)
    data = Column(JSON)


class InsiderPositions(NasdaqDBBase):
    __tablename__ = 'insider_positions'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    data = Column(JSON)


class NasdaqDatabase:
    """
    Opinionated wrapper around the NasdaqApi and a sqlite database.

    This is basically thought for scraping along a graph through
    the live API at a certain point in time (in between a few days).
    """
    def __init__(
            self,
            database_filename: Union[str, Path],
            verbose: bool = True,
    ):
        self.verbose = verbose
        self.api = NasdaqApi(verbose=verbose)
        self.db_engine = create_engine(f"sqlite:///{database_filename}")
        self.db_session: Session = sessionmaker(bind=self.db_engine)()
        NasdaqDBBase.metadata.create_all(self.db_engine)

    def company_profile(self, symbol: str, _unittest_override_db_check: bool = False) -> Optional[dict]:
        symbol = symbol.upper()

        if not _unittest_override_db_check:
            profile = (
                self.db_session
                .query(CompanyProfile)
                .filter(CompanyProfile.symbol == symbol)
            ).first()
            if profile:
                return profile.data

        timestamp = datetime.datetime.utcnow()
        data = self.api.company_profile(symbol)
        if data is None:
            return

        self.db_session.add(
            CompanyProfile(symbol=symbol, timestamp=timestamp, data=data)
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return data

    def stock_chart(
            self,
            symbol: str,
            asset_class: str = "stocks",
            date_from: Union[str, datetime.date, datetime.datetime] = "2000-01-01",
            date_to: Optional[Union[str, datetime.date, datetime.datetime]] = None,
    ) -> dict:
        symbol = symbol.upper()
        if isinstance(date_from, (datetime.date, datetime.datetime)):
            date_from = date_from.strftime("%Y-%m-%d")
        if date_to is None:
            date_to = datetime.date.today()
        if isinstance(date_to, (datetime.date, datetime.datetime)):
            date_to = date_to.strftime("%Y-%m-%d")

        key = f"{symbol}-{asset_class}-{date_from}"

        entry = self.db_session.query(StockChart).filter(StockChart.key == key).first()
        if entry:
            return entry.data

        timestamp = datetime.datetime.utcnow()
        data = self.api.stock_chart(
            symbol=symbol,
            asset_class=asset_class,
            date_from=date_from,
            date_to=date_to,
        )

        self.db_session.add(
            StockChart(
                key=key, symbol=symbol, timestamp=timestamp,
                asset_class=asset_class,
                date_from=datetime.datetime.strptime(date_from, "%Y-%m-%d").date(),
                date_to=datetime.datetime.strptime(date_to, "%Y-%m-%d").date(),
                data=data,
            )
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return data

    def company_holders(
            self,
            symbol,
            type: str = "TOTAL",
            page_size: int = 100,
    ) -> dict:
        symbol = symbol.upper()

        entry = (
            self.db_session
                .query(CompanyHolders)
                # TODO: ignores type for now
                .filter(CompanyHolders.symbol == symbol)
        ).first()
        if entry:
            return self._fix_date(entry.data, "data.holdingsTransactions.table.rows", "date", False)

        timestamp = datetime.datetime.utcnow()

        data = self.api.institutional_holdings(
            id=symbol,
            type=type,
            is_company=True,
            limit=page_size,
        )
        if get_path(data, "data.holdingsTransactions.table.rows"):
            num_total = int(data["data"]["holdingsTransactions"]["totalRecords"])
            while num_total > len(data["data"]["holdingsTransactions"]["table"]["rows"]):
                next_page = self.api.institutional_holdings(
                    id=symbol,
                    type=type,
                    is_company=True,
                    limit=page_size,
                    offset=len(data["data"]["holdingsTransactions"]["table"]["rows"]),
                )
                if not get_path(next_page, "data.holdingsTransactions.table.rows"):
                    break
                data["data"]["holdingsTransactions"]["table"]["rows"] += \
                    next_page["data"]["holdingsTransactions"]["table"]["rows"]

        self.db_session.add(
            CompanyHolders(
                symbol=symbol, type=type, timestamp=timestamp, data=data,
            )
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return self._fix_date(data, "data.holdingsTransactions.table.rows", "date", False)

    def institution_positions(
            self,
            id: Union[int, str],
            type: str = "TOTAL",
            page_size: int = 100,
    ) -> dict:
        id = int(id)

        entry = (
            self.db_session
                .query(InstitutionPositions)
                # TODO: ignores type for now
                .filter(InstitutionPositions.id == id)
        ).first()
        if entry:
            return entry.data

        timestamp = datetime.datetime.utcnow()

        data = self.api.institutional_holdings(
            id=id,
            type=type,
            is_company=False,
            limit=page_size,
        )
        if get_path(data, "data.institutionPositions.table.rows"):
            try:
                num_total = int(data["data"]["institutionPositions"]["totalRecords"])
            except:
                print(json.dumps(data, indent=2))
                raise

            while num_total > len(data["data"]["institutionPositions"]["table"]["rows"]):
                next_page = self.api.institutional_holdings(
                    id=id,
                    type=type,
                    is_company=False,
                    limit=page_size,
                    offset=len(data["data"]["institutionPositions"]["table"]["rows"]),
                )
                if not get_path(next_page, "data.institutionPositions.table.rows"):
                    break
                data["data"]["institutionPositions"]["table"]["rows"] += \
                    next_page["data"]["institutionPositions"]["table"]["rows"]

        self.db_session.add(
            InstitutionPositions(
                id=id, type=type, timestamp=timestamp, data=data,
            )
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return data

    def company_insiders(
            self,
            symbol,
            page_size: int = 300,
    ) -> dict:
        symbol = symbol.upper()

        entry = (
            self.db_session
                .query(CompanyInsiders)
                .filter(CompanyInsiders.symbol == symbol)
        ).first()
        if entry:
            return self._fix_date(entry.data, "data.transactionTable.table.rows", "lastDate", False)

        timestamp = datetime.datetime.utcnow()

        data = self.api.insider_trades(
            id=symbol,
            limit=page_size,
            sort_date=True,
        )
        if get_path(data, "data.transactionTable.table.rows"):
            num_total = int(data["data"]["transactionTable"]["totalRecords"])
            while num_total > len(data["data"]["transactionTable"]["table"]["rows"]):
                next_page = self.api.insider_trades(
                    id=symbol,
                    sort_date=True,
                    limit=page_size,
                    offset=len(data["data"]["transactionTable"]["table"]["rows"]),
                )
                if not get_path(next_page, "data.transactionTable.table.rows"):
                    break
                data["data"]["transactionTable"]["table"]["rows"] += \
                    next_page["data"]["transactionTable"]["table"]["rows"]

        self.db_session.add(
            CompanyInsiders(
                symbol=symbol, timestamp=timestamp, data=data,
            )
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return self._fix_date(data, "data.transactionTable.table.rows", "lastDate", False)

    def insider_positions(
            self,
            id: Union[int, str],
            page_size: int = 300,
    ) -> dict:
        id = int(id)

        entry = (
            self.db_session
                .query(InsiderPositions)
                .filter(InsiderPositions.id == id)
        ).first()
        if entry:
            return self._fix_date(entry.data, "data.filterTransactionTable.rows", "lastDate", False)

        timestamp = datetime.datetime.utcnow()

        data = self.api.insider_trades(
            id=id,
            limit=page_size,
        )

        page = 1
        # we don't have a total count here, so try to load the next page if we
        #   got exactly the number of max rows
        if get_path(data, "data.filerTransactionTable.rows"):
            while len(data["data"]["filerTransactionTable"]["rows"]) == page_size * page:
                next_page = self.api.insider_trades(
                    id=id,
                    limit=page_size,
                    offset=len(data["data"]["filerTransactionTable"]["rows"]),
                )
                if not get_path(next_page, "data.filerTransactionTable.rows"):
                    break
                data["data"]["filerTransactionTable"]["rows"] += \
                    next_page["data"]["filerTransactionTable"]["rows"]
                page += 1

        self.db_session.add(
            InsiderPositions(
                id=id, timestamp=timestamp, data=data,
            )
        )
        try:
            self.db_session.commit()
        except IntegrityError as e:
            # catch this in case multiple scraper run on the same database
            if "unique constraint failed" not in str(e).lower():
                raise
            self.db_session.rollback()

        return self._fix_date(data, "data.filterTransactionTable.rows", "lastDate", False)

    def _fix_date(self, data: dict, rows_path: str, field: str, sort: bool):
        """
        For whatever reason the date is month/day/year,
        change it to year/month/day to make it sortable
        """
        rows: list = get_path(data, rows_path)
        if rows:
            for row in rows:
                date = row[field].split("/")
                row[field] = "/".join(date[-1:] + date[:2])

            if sort:
                rows.sort(key=lambda row: row[field], reverse=True)

        return data

    def iter_objects(
            self,
            company_profile: bool = True,
            company_holders: bool = True,
            company_insiders: bool = True,
            stock_chart: bool = True,
            institution_positions: bool = True,
            insider_positions: bool = True,
            batch_size: int = 1000,
            verbose: Optional[bool] = None,
    ) -> Generator[dict, None, None]:
        """
        Yield all objects from the database, each as dict
        """
        if company_profile:
            yield from self._iter_objects(CompanyProfile, batch_size=batch_size, verbose=verbose)
        if stock_chart:
            yield from self._iter_objects(StockChart, batch_size=max(10, batch_size // 4), verbose=verbose)
        if company_holders:
            yield from self._iter_objects(CompanyHolders, batch_size=batch_size, verbose=verbose)
        if company_insiders:
            yield from self._iter_objects(CompanyInsiders, batch_size=batch_size, verbose=verbose)
        if institution_positions:
            yield from self._iter_objects(InstitutionPositions, batch_size=batch_size, verbose=verbose)
        if insider_positions:
            yield from self._iter_objects(InsiderPositions, batch_size=batch_size, verbose=verbose)

    def _iter_objects(
            self,
            model: Type[NasdaqDBBase],
            batch_size: int = 1000,
            verbose: Optional[bool] = None,
    ) -> Generator[dict, None, None]:
        if verbose is None:
            verbose = self.verbose

        field_names = [c.name for c in model.__table__.columns]
        order_field = field_names[0]

        query = (
            self.db_session
            .query(*(getattr(model, fn) for fn in field_names))
            .order_by(getattr(model, order_field))
        )

        count = query.count()
        iterable = range(0, count, batch_size)

        if verbose:
            iterable = tqdm(iterable, desc=model.__table__.name, unit_scale=batch_size)

        for i in iterable:
            for row in query.slice(i, i+batch_size).all():
                yield {
                    "type": model.__table__.name,
                    "data": {fn: value for fn, value in zip(field_names, row)},
                }

    def import_objects(self, iterable: Iterable[dict], batch_size: int = 100) -> dict:
        report = {}
        object_bulk = []
        if self.verbose:
            iterable = tqdm(iterable, desc="importing")
        last_time = time.time()
        for obj in iterable:
            if object_bulk:
                if object_bulk[-1]["type"] != obj["type"] or len(object_bulk) >= batch_size:
                    self._import_bulk(object_bulk, report)
                    object_bulk.clear()

                    if self.verbose:
                        cur_time = time.time()
                        if cur_time - last_time > 10:
                            last_time = cur_time
                            print()
                            for key, value in report.items():
                                print(f"{key:30}: {value:,}")

            object_bulk.append(obj)

        if object_bulk:
            self._import_bulk(object_bulk, report)

        if self.verbose:
            print("\n")
            for key, value in report.items():
                print(f"{key:30}: {value:,}")

        return report

    def _import_bulk(self, objects: List[dict], report: dict):
        # TODO: below is super ugly
        #   but i really don't understand sqlalchemy enough
        table = NasdaqDBBase.metadata.tables[objects[0]["type"]]
        field_names = [c.name for c in table.columns]
        id_field = field_names[0]

        Base = declarative_base()
        Model = type(table.name, (Base,), {"__table__": table})

        obj_ids = [o["data"][id_field] for o in objects]
        existing_ids = set(
            v[0] for v in
            self.db_session.query(getattr(Model, id_field))
            .filter(getattr(Model, id_field).in_(obj_ids))
            .all()
        )

        report[f"{table.name}"] = report.get(f"{table.name}", 0) + len(objects)
        report[f"{table.name} added"] = report.get(f"{table.name} added", 0) + len(objects) - len(existing_ids)

        models = []
        for obj in objects:
            if obj["data"][id_field] not in existing_ids:
                obj = obj["data"]
                if "timestamp" in obj:
                    obj["timestamp"] = datetime.datetime.strptime(obj["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
                if "date_from" in obj:
                    obj["date_from"] = datetime.datetime.strptime(obj["date_from"], "%Y-%m-%d").date()
                if "date_to" in obj:
                    obj["date_to"] = datetime.datetime.strptime(obj["date_to"], "%Y-%m-%d").date()
                models.append(Model(**obj))
                existing_ids.add(obj[id_field])

        if models:
            self.db_session.bulk_save_objects(models)
            self.db_session.commit()
