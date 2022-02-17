import os
import time
import json
from pathlib import Path
import traceback
import urllib.parse
from typing import List, Optional

from tqdm import tqdm
import requests
import bs4

from src.config import PROJECT_DIR


class NNDBScraper:

    CACHE_DIR: Path = PROJECT_DIR / "cache" / "nndb"
    BASE_URL = "http://www.nndb.com"

    TYPES = [
        "cemetery",
        "company",
        "crime",
        "detox",
        "edu",
        "event",
        "films",
        "geo",
        "gov",
        "group",
        "honors",
        "lists",
        "media",
        "music",
        "org",
        "people",
        "sports",
        "topics",
        "tv",
    ]

    def __init__(self, max_distance: int = 100000):
        self.max_distance = max_distance
        self.session = requests.Session()
        self.session.headers = {
            "User-Agent": "a fan"
        }
        self.node_map = dict()
        self._todo = set()
        self._done = set()
        self._skipped = set()
        self._seen = set()

    def soup(self, url: str) -> bs4.BeautifulSoup:
        return bs4.BeautifulSoup(self.request(url), features="html.parser")

    def request(self, url: str) -> str:
        url = urllib.parse.urljoin(self.BASE_URL, url)

        filename = url
        if filename.startswith(self.BASE_URL):
            filename = filename[len(self.BASE_URL):]
        if not filename:
            filename = "index"
        else:
            filename = filename.replace("/", "-").replace(":", "-").strip("-")
        filename = self.CACHE_DIR / filename

        if filename.exists():
            return Path(filename).read_text()

        print("requesting", url)
        response = self.session.get(url)
        assert response.status_code in (200, 404), f"{response.status_code} {response.text}"
        text = response.text

        os.makedirs(filename.parent, exist_ok=True)
        filename.write_text(text)

        return text

    def add_url_todo(self, url: str, distance: int = 0):
        if distance > self.max_distance:
            return

        if not url.startswith("/"):
            if not url.startswith(self.BASE_URL):
                return
            url = url[len(self.BASE_URL):]
        assert url.startswith("/"), url

        if url in self._seen:
            return

        for type in self.TYPES:
            if f"/{type}/" in url:
                self._todo.add((url, distance))
                self._seen.add(url)
                return

        print("unhandled url", url)

    def get_node(self, id: str) -> dict:
        if id not in self.node_map:
            self.node_map[id] = {
                "id": id,
                "relations": [],
            }
        return self.node_map[id]

    @classmethod
    def url_to_id(cls, url: str) -> Optional[str]:
        for type in cls.TYPES:
            if f"/{type}/" in url:
                return url.strip("/").split("/")[-1]

    @classmethod
    def url_to_type(cls, url: str) -> Optional[str]:
        for type in cls.TYPES:
            if f"/{type}/" in url:
                return type

    def scrape(self):
        last_print_time = time.time()
        while self._todo:
            url, distance = self._todo.pop()
            handled = False

            for type in self.TYPES:
                if f"/{type}/" in url:
                    if callable(getattr(self, f"scrape_{type}", None)):
                        try:
                            getattr(self, f"scrape_{type}")(url, distance)
                        except KeyboardInterrupt:
                            raise
                        except:
                            print(f"\nERROR in scrape_{type}('{url}')")
                            traceback.print_exc()
                        handled = True
                    break

            if handled:
                self._done.add(url)
            else:
                self._skipped.add(url)

            cur_time = time.time()
            if cur_time - last_print_time >= 2:
                last_print_time = cur_time
                print(f"todo {len(self._todo)}, done {len(self._done)}, skipped {len(self._skipped)}")

    def scrape_index(self):
        index = self.soup("")
        wrapper = index.find("font", {"size": "+1"})

        for a in tqdm(wrapper.find_all("a"), "scrape alphabetical index"):
            soup = self.soup(a["href"])

            for table in soup.find_all("table"):
                if len(list(table.children)) > 1000:
                    for row in table.find_all("tr"):
                        row = row.find_all("td")
                        self._people_node_from_row(row)

        if 0:
            for node in tqdm(self.nodes.values(), desc="scrape people from index"):
                self.scrape_people(node["url"])

    def scrape_people(self, url: str, distance: int = 0):
        node = self.get_node(self.url_to_id(url))
        node.update({
            "type": "people",
            "url": url,
        })
        soup = self.soup(url)

        table = soup.find_all("table")[-2]
        bs = table.find_all("b")
        if not bs:
            print("UNSCRAPED PEOPLE", url)
            return

        node.update({
            "name": bs[0].text.strip(),
        })
        for b in bs[1:]:
            b_text = b.text.strip()
            key = None
            if b_text == "AKA":
                key = "aka"
            elif b_text.endswith(":"):
                key = b_text[:-1]

            if key:
                siblings = []
                while b.next_sibling:
                    tag = getattr(b.next_sibling, "name", None)
                    if tag in ("b", "br", "p"):
                        break
                    b = b.next_sibling
                    siblings.append(b)

                node[key] = "".join(s.text for s in siblings).strip()

        for a in table.find_all("a"):
            if a.get("href"):
                self.add_url_todo(a["href"], distance + 1)

    def scrape_company(self, url: str, distance: int = 0):
        company_id = self.url_to_id(url)
        node = self.get_node(company_id)
        node.update({
            "type": "company",
            "url": url,
        })
        soup = self.soup(url)

        heading = soup.find("font", {"size": "+3"})
        if heading and heading.text and heading.text.strip():
            node["name"] = heading.text.strip()

        table = soup.find_all("table")[4]
        self._parse_p_tags(node, table)

        for p in table.find_all("p"):
            if p.find("font"):
                p_text = p.find("font").text.strip()
                edge_type = None
                if p_text == "EXECUTIVES":
                    edge_type = "executive"
                elif p_text in (
                        "BOARD MEMBERS OR DIRECTORS",
                        "CURRENT BOARD MEMBERS OR DIRECTORS",
                ):
                    edge_type = "board-or-director"
                elif p_text == "PAST BOARD MEMBERS OR DIRECTORS":
                    edge_type = "past-board-or-director"
                elif p_text in ("NOTABLE EMPLOYEES", "EMPLOYMENT"):
                    edge_type = "employed"
                elif p_text == "EXTRANEOUS":
                    edge_type = "extraneous"
                elif p_text == "CELEBRITY ENDORSEMENTS":
                    edge_type = "endorsement"
                elif p_text == "COMPANY":
                    pass
                elif p_text.isupper():
                    print(f"\nMISSING company key '{p_text}'")

                if edge_type:
                    table = p.find("table")
                    for tr in table.find_all("tr"):
                        people_node = self._people_node_from_row(tr.find_all("td"))
                        if people_node:
                            people_node["relations"].append({
                                "to": company_id,
                                "type": edge_type,
                            })

        for a in table.find_all("a"):
            if a.get("href"):
                self.add_url_todo(a["href"], distance + 1)

    def scrape_gov(self, url: str, distance: int = 0):
        gov_id = self.url_to_id(url)
        node = self.get_node(gov_id)
        node.update({
            "type": "gov",
            "url": url,
        })
        soup = self.soup(url)

        heading = soup.find("font", {"size": "+3"})
        if heading and heading.text and heading.text.strip():
            node["name"] = heading.text.strip()

        table = soup.find_all("table")[4]
        self._parse_p_tags(node, table)

        people_table = table.find("table")

        for tr in people_table.find_all("tr"):
            people_node = self._people_node_from_row(tr.find_all("td"))
            if people_node:
                people_node["relations"].append({
                    "to": gov_id,
                    "type": "government",
                })

        for a in table.find_all("a"):
            if a.get("href"):
                self.add_url_todo(a["href"], distance + 1)

    def scrape_edu(self, url: str, distance: int = 0):
        edu_id = self.url_to_id(url)
        node = self.get_node(edu_id)
        node.update({
            "type": "edu",
            "url": url,
        })
        soup = self.soup(url)

        heading = soup.find("font", {"size": "+3"})
        if heading and heading.text and heading.text.strip():
            node["name"] = heading.text.strip()

        table = soup.find_all("table")[4]
        self._parse_p_tags(node, table)

        for a in table.find_all("a"):
            if a.get("href"):
                self.add_url_todo(a["href"], distance + 1)

        for p in table.find_all("p"):
            if p.find("font"):
                p_text = p.find("font").text.strip()
                edge_type = None
                if p_text == "STUDENTS":
                    edge_type = "student"
                elif p_text == "TEACHERS AND PROFESSORS":
                    edge_type = "teacher"
                elif p_text == "ADMINISTRATORS AND TRUSTEES":
                    edge_type = "admin/trustee"
                elif p_text == "COACHES":
                    edge_type = "coach"
                elif p_text == "EDUCATIONAL INSTITUTION":
                    pass
                elif p_text.isupper():
                    print(f"\nMISSING educational role '{p_text}' in {url}")

                if edge_type:
                    table = p.find("table")
                    for tr in table.find_all("tr"):
                        people_node = self._people_node_from_row(tr.find_all("td"))
                        if people_node:
                            people_node["relations"].append({
                                "to": edu_id,
                                "type": edge_type,
                            })

    def scrape_group(self, url: str, distance: int = 0):
        id = url.strip("/").split("/")[-1]
        soup = self.soup(url)

    def scrape_detox(self, url: str, distance: int = 0):
        id = url.strip("/").split("/")[-1]
        soup = self.soup(url)

    def scrape_films(self, url: str, distance: int = 0):
        film_id = self.url_to_id(url)
        node = self.get_node(film_id)
        node.update({
            "type": "film",
            "url": url,
        })
        soup = self.soup(url)

        heading = soup.find("font", {"size": "+3"})
        if heading and heading.text and heading.text.strip():
            node["name"] = heading.text.strip()

        table = soup.find_all("table")[4]
        relations = []
        self._parse_p_tags(node, table, relations)

        for rel in relations:
            self.add_url_todo(rel["url"], distance + 1)
            if self.url_to_type(rel["url"]) == "people":
                people_node = self.get_node(rel["id"])
                people_node["relations"].append({
                    "to": film_id,
                    "type": rel["type"]
                })

        people_table = soup.find("table", {"id": "sort_actors"})

        for tr in people_table.find_all("tr"):
            people_node = self._people_node_from_row(tr.find_all("td"))
            if people_node:
                people_node["relations"].append({
                    "to": film_id,
                    "type": "actor",
                })

        for a in table.find_all("a"):
            if a.get("href"):
                self.add_url_todo(a["href"], distance + 1)

        #print(node)

    def _parse_p_tags(self, node: dict, tag: bs4.Tag, relations: Optional[List[dict]] = None):
        for p in tag.find_all("p"):
            b = p.find("b")
            if b and b.text:
                b_text = b.text.strip()
                if b_text.endswith(":"):
                    key = b_text[:-1]
                    siblings = []
                    sib = b
                    while sib.next_sibling:
                        next_tag = getattr(sib.next_sibling, "name", None)
                        if next_tag in ("b", "p"):
                            break

                        if relations is not None:
                            this_tag = getattr(sib, "name", None)
                            if this_tag == "a" and sib.get("href"):
                                sib_id = self.url_to_id(sib["href"])
                                if sib_id:
                                    relations.append({
                                        "id": sib_id, "url": sib["href"], "name": sib.text.strip(), "type": key
                                    })

                        sib = sib.next_sibling
                        siblings.append(sib)

                    value = "".join(s.text for s in siblings).strip()
                    node[key] = value

    def _people_node_from_row(self, row: List[bs4.Tag]) -> Optional[dict]:
        if not row or not row[0].find("a"):
            return

        href = row[0].find("a")["href"]
        self.add_url_todo(href)
        node = self.get_node(self.url_to_id(href))
        node.update({
            "type": "people",
            "url": href,
            "name": row[0].text.strip(),
            "Occupation": row[1].text.strip(),
            "Born": row[2].text.strip(),
            "Died": row[3].text.strip(),
            "known_for": row[4].text.strip(),
        })
        return node


def scrape_all(json_filename: Path):
    db = NNDBScraper()
    db.scrape_index()
    # db.add_url_todo("/company/028/000124653/")
    db.add_url_todo("/gov/251/000127867/")
    db.scrape()

    json_filename.write_text(json.dumps(list(db.node_map.values())))


def render_graph(nodes_filename: Path, graph_filename: Path):
    import igraph

    nodes = json.loads(nodes_filename.read_text())

    vertex_attributes = {
        "name": [], "label": [], "type": [],
    }
    vertex_id_map = {}
    edge_indices = []
    edge_attributes = {"type": []}

    for node in nodes:
        vertex_attributes["name"].append(node["id"])
        vertex_attributes["label"].append(node.get("name") or node.get("Official website") or node["id"])
        vertex_attributes["type"].append(node["type"])
        vertex_id_map[node["id"]] = len(vertex_id_map)

    for node in nodes:
        for edge in node["relations"]:
            #edge_indices.append((edge["from"], edge["to"]))
            edge_indices.append((vertex_id_map[node["id"]], vertex_id_map[edge["to"]]))
            edge_attributes["type"].append(edge["type"])

    graph = igraph.Graph(directed=True)
    graph.add_vertices(len(vertex_attributes["name"]), vertex_attributes)
    graph.add_edges(edge_indices, edge_attributes)

    print(f"{len(graph.vs)} x {len(graph.es)}")
    graph.delete_vertices([
        i for i, degree in enumerate(graph.degree())
        if degree < 1
    ])
    print(f"{len(graph.vs)} x {len(graph.es)}")
    graph.write(str(graph_filename))


if __name__ == "__main__":
    nodes_filename = (PROJECT_DIR / "stuff" / "nndb" / "many-nodes.json")
    scrape_all(nodes_filename)
    render_graph(nodes_filename, PROJECT_DIR / "stuff" / "nndb" / "graph.graphml")
