import json
from pathlib import Path
from typing import Optional, Union

import igraph

from .nasdaq_walker import NasdaqWalkerInterface
from .util import get_path, to_id, to_int, to_float


class NasdaqGraphBuilder(NasdaqWalkerInterface):

    DEFAULT_VERTEX = {
        "type": None,
        "name": None,
        "symbol": None,
        "label": None,
        "industry": None,
        "sector": None,
        "region": None,
        "is_director": 0,
        "is_officer": 0,
        "is_10percent": 0,
        "total_shares": 0.,
        "total_holdings_dollar": 0,
        "sale_price": 0.,
    }
    
    DEFAULT_EDGE = {
        "type": None,
        "relation": None,
        "own_type": None,
        "weight": 0.,
        "date": None,
        "shares": 0.,
        "shares_percent": 0.,
        "shares_dollar": 0.,
    }

    RELATION_MAP = {
        "Director": "director",
        "Officer": "officer",
        "Beneficial Owner (10%)": "ten_percent",
    }

    def __init__(self):
        self.vertex_map = dict()
        self.edge_map = dict()

    def vertex(self, symbol_or_id: Union[int, str]) -> dict:
        symbol_or_id = str(symbol_or_id)
        if symbol_or_id not in self.vertex_map:
            self.vertex_map[symbol_or_id] = {
                **self.DEFAULT_VERTEX,
                # "name" is the alternative to the vertex_id in igraph
                "name": symbol_or_id,
                "symbol": symbol_or_id,
                "label": symbol_or_id,
            }
        return self.vertex_map[symbol_or_id]

    def edge(self, source: Union[int, str], target: Union[int, str], type: str) -> dict:
        source = str(source)
        target = str(target)
        key = (source, target, type)
        if key not in self.edge_map:
            self.edge_map[key] = {
                **self.DEFAULT_EDGE,
                "type": type,
            }
        return self.edge_map[key]

    def on_company_profile(self, symbol: str, data: dict):
        self.vertex(symbol).update({
            "type": "company",
            "label": get_path(data, "CompanyName.value"),
            "industry": get_path(data, "Industry.value"),
            "sector": get_path(data, "Sector.value"),
            "region": get_path(data, "Region.value"),
        })
        
    def on_company_holders(self, symbol: str, data: Optional[dict]):
        company_total_shares = 0

        info_rows = get_path(data, "activePositions.rows")
        if info_rows:
            assert info_rows[3]["positions"] == "Total Institutional Shares", info_rows["3"]
            company_total_shares = to_int(info_rows[3]["shares"])
            self.vertex(symbol)["total_shares"] = company_total_shares

        value = get_path(data, "ownershipSummary.TotalHoldingsValue")
        if value:
            assert value["label"] == "Total Value of Holdings (millions)", value
            self.vertex(symbol)["total_holdings_dollar"] = to_int(value["value"][1:]) * 1_000_000

        rows = get_path(data, "holdingsTransactions.table.rows")
        if rows:
            for row in rows:
                if row["url"]:
                    institution_id = int(row["url"].split("-")[-1])
                else:
                    institution_id = to_id(row["ownerName"])

                self.vertex(institution_id).update({
                    "type": "institution",
                    "label": row["ownerName"]
                })

                edge = self.edge(institution_id, symbol, "holder")
                edge.update({
                    "shares": float(to_int(row["sharesHeld"])),
                    "relation": "holder",
                    "own_type": "holder",
                    "date": row["date"],
                    "shares_dollar": float(to_int(row["marketValue"][1:])),
                })
                if company_total_shares:
                    edge["shares_percent"] = edge["shares"] / company_total_shares * 100
                    edge["weight"] = edge["shares_percent"] / 100.

    def on_company_insiders(self, symbol: str, data: Optional[dict]):
        rows = get_path(data, "transactionTable.table.rows")
        if rows:
            for row in rows:
                if row["url"]:
                    insider_id = int(row["url"].split("-")[-1])
                else:
                    insider_id = to_id(row["insider"])

                relation = f'insider-{row["ownType"]}-{row["relation"]}'

                company_total_shares = self.vertex(symbol)["total_shares"]
                sale_price = self.vertex(symbol)["sale_price"]

                shares = float(to_int(row["sharesHeld"]))
                shares_percent = 0.
                if company_total_shares:
                    shares_percent = shares / company_total_shares * 100
                    # TODO: there are a lot of insiders who hold more
                    #   than the reported shares
                    if shares_percent > 100:
                        #print(shares_percent, row, self.vertex(symbol))
                        shares_percent = 100.

                self.vertex(insider_id).update({
                    "label": row["insider"],
                    "type": "insider",
                })
                self.edge(insider_id, symbol, relation).update({
                    "shares": shares,
                    "shares_dollar": shares * sale_price,
                    "shares_percent": shares_percent,
                    "weight": shares_percent / 100.,
                    "date": row["lastDate"],
                    "own_type": row["ownType"],
                    "relation": row["relation"],
                })

    def on_stock_chart(self, symbol: str, data: dict):
        try:
            sale_price = to_float(data["lastSalePrice"][1:])
            self.vertex(symbol).update({
                "sale_price": sale_price
            })
        except (TypeError, ValueError):
            pass

    def on_institution_positions(self, id: int, data: Optional[dict]):
        pass

    def on_insider_positions(self, id: int, data: Optional[dict]):
        if not data:
            return

        self.vertex(id).update({
            "label": data["title"][18:],
            "type": "insider",
        })
        rows = get_path(data, "filerTransactionTable.rows")
        if rows:

            # the rows are sorted descending by date
            #   so just pick the newest edge for each
            #   specific insider relation

            relations = dict()
            for row in rows:
                if row["url"]:
                    company_id = row["url"].split("/")[3].upper()
                else:
                    company_id = to_id(row["company"])

                key = (row["ownType"], row["relation"], company_id)
                if key in relations:
                    continue
                relations[key] = row

            for (own_type, relation, company_id), row in relations.items():

                self.vertex(company_id).update({
                    "label": row["company"],
                    "type": "company",
                })
                self.edge(id, company_id, f"insider-{own_type}-{relation}").update({
                    "shares": float(to_int(row["sharesHeld"])),
                    "date": row["lastDate"],
                })

    def finalize(self):
        for (id_from, id_to, type), edge in self.edge_map.items():
            vertex_from = self.vertex_map[id_from]
            vertex_to = self.vertex_map[id_to]

            if type == "Director":
                vertex_to["is_director"] += 1
            elif type == "Officer":
                vertex_to["is_officer"] += 1
            if type.startswith("Beneficial"):
                vertex_to["is_10percent"] += 1

            vertex_from["total_shares"] += edge["shares"]

    # ------ conversion --------

    def to_igraph(self) -> igraph.Graph:

        def _check_edge_key(key):
            ok = True
            if key[0] not in self.vertex_map:
                ok = False
                print(f"institution/insider {key[0]} not in vertex_map!")
            if key[1] not in self.vertex_map:
                ok = False
                print(f"company {key[1]} not in vertex_map!")
            return ok

        graph = igraph.Graph(directed=True)

        if self.vertex_map:
            graph.add_vertices(
                len(self.vertex_map),
                {
                    key: [
                        "None" if n[key] is None else n[key]
                        for n in self.vertex_map.values()
                    ]
                    for key in self.DEFAULT_VERTEX.keys()
                }
            )

        if self.edge_map:
            graph.add_edges(
                [(e[0], e[1]) for e in self.edge_map.keys() if _check_edge_key(e)],
                {
                    key: [
                        "None" if n[key] is None else n[key]
                        for e, n in self.edge_map.items()
                        if _check_edge_key(e)
                    ]
                    for key in self.DEFAULT_EDGE.keys()
                }
            )

            max_weight = max(*graph.es["weight"])
            if max_weight:
                graph.es["weight"] = [max(0.00001, round(w / max_weight, 4)) for w in graph.es["weight"]]

        return graph

