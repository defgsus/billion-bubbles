class Network {

    constructor(nodes, edges) {
        this.nodes = nodes;
        this.edges = edges;
        this.node_map = {};
        this.edge_map = {};
        this._node_filter = null;
        this._edge_filter = null;
        for (const n of this.nodes) {
            n.id = `${n.id}`;
            //n.x *= 1000.;
            //n.y *= 1000.;
            n.name_lower = n.name.toLowerCase();
            n.totalHoldingsDollar = n.totalHoldingsMillionDollar * 1000000;
            n.hidden = false;
            n.selected = false;
            n.selection_weight = 0.;
            n.radius = 10.;
            n.color = [.5, .5, .5];
            n.edges = [];
            n.edges_out = [];
            n.edges_in = [];
            this.node_map[n.id] = n;
        }
        for (const e of this.edges) {
            e.id = `${e.from}-${e.to}`;
            e.sharesDollar = e.sharesThousandDollar * 1000;
            e.sharesPercent = e.weight * 100.;
            e.hidden = false;
            e.node_from = this.node_map[e.from];
            e.node_to = this.node_map[e.to];
            e.node_from.edges.push(e);
            e.node_from.edges_out.push(e);
            e.node_to.edges.push(e);
            e.node_to.edges_in.push(e);
            e.width = 1.;
            this.edge_map[e.id] = e;
        }
        for (const n of this.nodes) {
            n.num_edges_in = n.edges_in.length;
            n.num_edges_out = n.edges_out.length;
            n.num_edges = n.num_edges_in + n.num_edges_out;
        }
        this.selected_nodes = [];
        this.on_selection_changed = (
            selected_nodes, unselected_nodes,
            selected_edges, unselected_edges
        ) => {};
    }

    get_node_min_max = (field) => {
        return this._get_min_max(this.nodes, field);
    };

    get_edge_min_max = (field) => {
        return this._get_min_max(this.edges, field);
    };

    _get_min_max = (elements, field) => {
        let mi = null, ma = null;
        for (const n of elements) {
            if (typeof n[field] === "number") {
                if (mi === null) {
                    mi = n[field];
                    ma = n[field];
                } else {
                    mi = Math.min(mi, n[field]);
                    ma = Math.max(ma, n[field]);
                }
            }
        }
        return [mi, ma];
    };

    center = () => {
        let x = 0, y = 0;
        for (const n of this.nodes) {
            x += n.x;
            y += n.y;
        }
        return [x / this.nodes.length, y / this.nodes.length];
    };

    node_at = (x, y) => {
        for (const n of this.nodes) {
            const r2 = (n.x - x) * (n.x - x) + (n.y - y) * (n.y - y);
            if (r2 < n.radius * n.radius)
                return n;
        }
    };

    set_node_filter = (callable) => {
        this._node_filter = callable || (node => true);
        this._apply_filter();
    };

    traverse_nodes = (start_id_or_node, distance=1) => {
        const start_id = typeof start_id_or_node === "object" ? start_id_or_node.id : start_id_or_node;
        if (distance === 0)
            return [[this.node_map[start_id], 0]];
        const todo = new Set([[start_id, 0]]);
        const done = new Set();
        const returned_nodes = [];
        while (todo.size) {
            const node_id_and_distance = todo.entries().next().value[0];
            const node_id = node_id_and_distance[0];
            const cur_distance = node_id_and_distance[1];
            todo.delete(node_id_and_distance);
            done.add(node_id);
            returned_nodes.push([this.node_map[node_id], cur_distance]);
            if (cur_distance >= distance)
                continue;

            const node = this.node_map[node_id];

            for (const e of node.edges_out) {
                if (!done.has(e.to)) {
                    todo.add([e.node_to.id, cur_distance + 1]);
                }
            }
            for (const e of node.edges_in) {
                if (!done.has(e.from)) {
                    todo.add([e.node_from.id, cur_distance + 1]);
                }
            }
        }
        return returned_nodes;
    };

    update_radius = (field, min_radius, max_radius) => {
        const field_min_max = this.get_node_min_max(field);
        for (const n of this.nodes) {
            const radius = min_radius + (max_radius - min_radius) * (
                (n[field] - field_min_max[0]) / (field_min_max[1] - field_min_max[0])
            );
            n.radius = isNaN(radius) ? min_radius : radius;
        }
    };

    update_edge_width = (field, min_width, max_width) => {
        const field_min_max = this.get_edge_min_max(field);
        for (const edge of this.edges) {
            const width = min_width + (max_width - min_width) * (
                (edge[field] - field_min_max[0]) / (field_min_max[1] - field_min_max[0])
            );
            edge.width = isNaN(width) ? min_width : width;
        }
    };

    set_selected = (nodes_or_ids_and_distances, max_distance) => {
        const selected_ids = new Set(nodes_or_ids_and_distances.map(n => (
            typeof n[0] === "object" ? n[0].id : n[0]
        )));
        const selected_edge_ids = new Set();
        for (const node_id of selected_ids) {
            const node = this.node_map[node_id];
            for (const edge of node.edges)
                if (selected_ids.has(edge.from) && selected_ids.has(edge.to))
                    selected_edge_ids.add(edge.id);
        }
        const
            selected_nodes = [],
            unselected_nodes = [],
            selected_edges = new Set(),
            unselected_edges = new Set();
        for (const node of this.selected_nodes) {
            if (!selected_ids.has(node.id)) {
                unselected_nodes.push(node);
                node.selected = false;
                node.selection_weight = 0.;
            }
            for (const edge of node.edges) {
                if (!selected_edge_ids.has(edge.id)) {
                    unselected_edges.add(edge);
                    edge.selected = false;
                    node.selection_weight = 0.;
                }
            }
        }
        for (const node_or_id_and_distance of nodes_or_ids_and_distances) {
            const node = typeof node_or_id_and_distance[0] === "object"
                ? node_or_id_and_distance[0] : this.node_map[node_or_id_and_distance];
            const weight = max_distance
                ? 1. - Math.max(0, node_or_id_and_distance[1] - 1) / max_distance
                : 1.;
            node.selected = true;
            node.selection_weight = weight;
            selected_nodes.push(node);
            for (const edge of node.edges) {
                if (selected_ids.has(edge.from) && selected_ids.has(edge.to)) {
                    selected_edges.add(edge);
                    edge.selected = true;
                    edge.selection_weight = weight;
                }
            }
        }
        this.selected_nodes = selected_nodes;
        this.on_selection_changed(
            selected_nodes, unselected_nodes,
            Array.from(selected_edges), Array.from(unselected_edges),
        );
    };
    
    _apply_filter = () => {
        const visible_nodes = new Set();
        for (const node of this.nodes) {
            node.hidden = !this._node_filter(node);
            if (!node.hidden)
                visible_nodes.add(node.id);
        }
        for (const edge of this.edges) {
            edge.hidden = !(visible_nodes.has(edge.from) && visible_nodes.has(edge.to));
        }
    }
}


class Diagram {

    constructor(svg_element, filter_container) {
        this.element = svg_element;
        this.filter_container = filter_container;
        this.view = {
            x: 0,
            y: 0,
            zoom: 10.
        };
        this.color_mode = null;
        this.color_field_min = 0;
        this.color_field_max = 1;
        this.color_exponent = 1.;
        this.category_color_map = {};
        this.color_filters = {};
        this.color_entries = [];
        this.transparency = .6;
        this.text_query = "";
        this.network = null;
        this.on_click = (x, y) => {};

        this._last_mouse_down = [0, 0];
        this._last_mouse_down_view = [0, 0];
        this._has_mouse_moved = false;
        this.element.addEventListener("mousedown", e => {
            this._last_mouse_down = [e.clientX, e.clientY];
            this._last_mouse_down_view = [this.view.x, this.view.y];
            this._has_mouse_moved = false;
        });
        this.element.addEventListener("mouseup", e => {
            if (!this._has_mouse_moved) {
                const [x, y] = this.event_to_world_coords(e);
                this.on_click(x, y);
            }
        });
        this.element.addEventListener("mousemove", e => {
            if (!e.buttons)
                return;
            this._has_mouse_moved = true;
            if (this._last_mouse_down) {
                this.view.x =
                    this._last_mouse_down_view[0] - (e.clientX - this._last_mouse_down[0]) / this.view.zoom;
                this.view.y =
                    this._last_mouse_down_view[1] - (e.clientY - this._last_mouse_down[1]) / this.view.zoom;
            }
            this.update_view();
        });
        this.element.addEventListener("wheel", e => {
            e.stopPropagation();
            e.preventDefault();
            const
                new_zoom = Math.max(0.0001, this.view.zoom * (e.deltaY > 0 ? .95 : 1.05)),
                coords = this.event_to_world_coords(e);
            // TODO: zoom centered around cursor position
            this.view.zoom = new_zoom;
            this.update_view();
        });
    }

    width = () => this.element.getBoundingClientRect().width;
    height = () => this.element.getBoundingClientRect().height;

    event_to_world_coords = (event) => {
        const
            bb = this.element.getBoundingClientRect(),
            elem_x = event.clientX - bb.left,
            elem_y = event.clientY - bb.top,
            x = (elem_x - bb.width/2) / this.view.zoom + this.view.x,
            y = (elem_y - bb.height/2) / this.view.zoom + this.view.y;
        return [x, y];
    };

    _palette_rgb(t) {
        return [
            Math.pow(t, .5),
            Math.pow(1.-t, .4),
            Math.pow(1.-t, 3.),
        ];
    }

    _category_rgb(i) {
        return [
            .5 + .4 * Math.sin(i ^ 4481),
            .5 + .4 * Math.sin(i ^ 7901),
            .5 + .4 * Math.sin(i ^ 6367),
        ];
    }

    node_rgb = (node) => {
        let [r, g, b] = node.color;
        const highlight = node.selection_weight * .2;
        r += highlight;
        g += highlight;
        b += highlight;
        return [r, g, b];
    };

    node_color = (node) => {
        const [r, g, b] = this.node_rgb(node);
        const alpha = (1. - this.transparency) + node.selection_weight;
        return this._to_rgba(r, g, b, alpha);
    };

    edge_color = (edge) => {
        const t = .7;
        const [r1, g1, b1] = this.node_rgb(edge.node_from);
        const [r2, g2, b2] = this.node_rgb(edge.node_to);
        const alpha1 = (1. - this.transparency) + edge.node_from.selection_weight;
        const alpha2 = (1. - this.transparency) + edge.node_to.selection_weight;
        const r = r1 * (1. - t) + t * r2;
        const g = g1 * (1. - t) + t * g2;
        const b = b1 * (1. - t) + t * b2;
        const a = alpha1 * (1. - t) + t * alpha2;
        return this._to_rgba(r, g, b, a);
    };

    _to_rgba = (r, g, b, a) => {
        r = Math.max(0, Math.min(255, r * 256)).toFixed();
        g = Math.max(0, Math.min(255, g * 256)).toFixed();
        b = Math.max(0, Math.min(255, b * 256)).toFixed();
        a = Math.max(0, Math.min(1, a)).toFixed(3);
        return `rgba(${r},${g},${b},${a})`
    };

    _set_title = (elem, text) => {
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        const tex = document.createTextNode(text);
        title.appendChild(tex);
        elem.appendChild(title);
    };

    _rotate = (x, y, degree) => {
        const a = degree / 180. * Math.PI;
        const si = Math.sin(a);
        const co = Math.cos(a);
        return [
            x * co - y * si,
            x * si + y * co,
        ]
    };

    _get_edge_positions = (edge) => {
        const node1 = edge.node_from;
        const node2 = edge.node_to;
        let dx = (node2.x - node1.x);
        let dy = (node2.y - node1.y);
        const length = Math.sqrt(dx*dx + dy*dy);
        if (length) {
            dx /= length;
            dy /= length;
        }
        const arrow_length = 10 + edge.weight * 10.;
        const arrow_degree = 10 + edge.weight * 10.;
        const [dx1, dy1] = this._rotate(dx, dy, arrow_degree);
        const [dx2, dy2] = this._rotate(dx, dy, -arrow_degree);
        return [
            node1.x + node1.radius * dx,
            node1.y + node1.radius * dy,
            node2.x - node2.radius * dx,
            node2.y - node2.radius * dy,
            node2.x - (node2.radius + arrow_length) * dx2,
            node2.y - (node2.radius + arrow_length) * dy2,
            node2.x - (node2.radius + arrow_length) * dx1,
            node2.y - (node2.radius + arrow_length) * dy1,
        ];
    };

    set_transparency = (t) => {
        this.transparency = t;
    };

    set_text_query = (query) => {
        this.text_query = query.toLowerCase();
        this.network.set_node_filter(this.node_filter);
    };

    set_color_mode = (mode, exponent=1.) => {
        this.color_mode = mode;
        this.color_exponent = exponent;
        if (this.color_mode.startsWith("value_")) {
            const field = this.color_mode.slice(6);
            [this.color_field_min, this.color_field_max] =
                this.network.get_node_min_max(field);

            for (const node of this.network.nodes) {
                node.color = this._palette_rgb(
                    (node[field] - this.color_field_min) / (
                        this.color_field_max - this.color_field_min
                    )
                )
            }
        } else if (this.color_mode.startsWith("category_")) {
            const field = this.color_mode.slice(9);
            const categories = {};
            for (const node of this.network.nodes) {
                categories[node[field]] = (categories[node[field]] || 0) + 1;
            }
            const categories_sorted = Object.keys(categories).sort((a, b) => (
                categories[a] > categories[b] ? -1 : 1
            ));
            this.category_color_map = {};
            let idx = 0;
            for (const cat of categories_sorted) {
                this.category_color_map[cat] = {
                    color: this._category_rgb(idx),
                    percent: (categories[cat] / this.network.nodes.length * 100).toFixed(2),
                };
                idx += 1;
            }
            for (const node of this.network.nodes) {
                node.color = this.category_color_map[node[field]].color;
            }
        }
        this._create_filter_element();
    };

    _create_filter_element = () => {
        this.color_entries = [];
        if (this.color_mode.startsWith("value_")) {
            const field = this.color_mode.slice(6);
            const num = 10;
            for (let i=0; i<=num; ++i) {
                let value = this.color_field_min + Math.pow(i / num, this.color_exponent) * (
                    this.color_field_max - this.color_field_min
                );
                if (value < -1 || value > 1)
                    value = Math.trunc(value);

                this.color_entries.push({
                    value: value.toLocaleString("en"),
                    color: this._palette_rgb(i / num),
                    filter_func: node => (
                        node[field] >= this.color_field_min
                            + Math.pow(i / num, this.color_exponent)
                                * (this.color_field_max - this.color_field_min)
                        && node[field] < this.color_field_min
                            + Math.pow((i + 1) / num, this.color_exponent)
                                * (this.color_field_max - this.color_field_min)
                    )
                });
            }
        } else if (this.color_mode.startsWith("category_")) {
            const field = this.color_mode.slice(9);
            for (const cat of Object.keys(this.category_color_map)) {
                const value = this.category_color_map[cat];
                this.color_entries.push({
                    value: `${cat} ${value.percent}%`,
                    color: value.color,
                    filter_func: node => (
                        node[field] === cat
                    )
                });
            }
        }
        const filter_elem = document.createElement("div");
        for (const entry of this.color_entries) {
            const filter_key = `${this.color_mode}/${entry.value}`;
            const elem = document.createElement("div");
            filter_elem.appendChild(elem);
            entry.element = elem;
            entry.filter_key = filter_key;
            elem.setAttribute("class", "entry");
            elem.setAttribute("data-filter", filter_key);
            elem.onclick = () => {
                if (this.color_filters[filter_key])
                    delete this.color_filters[filter_key];
                else
                    this.color_filters[filter_key] = entry.filter_func;
                console.log(this.color_filters);
                this._update_filter_elements();
                this.network.set_node_filter(this.node_filter);
                this.update_visibility();
            };
            const text = document.createTextNode(entry.value);
            elem.appendChild(text);
        }
        const butt = document.createElement("button");
        this.filter_reset_button = butt;
        butt.appendChild(document.createTextNode("reset"));
        butt.setAttribute("class", "reset");
        filter_elem.appendChild(butt);
        butt.onclick = () => {
            this.color_filters = {};
            this._update_filter_elements();
            this.network.set_node_filter(this.node_filter);
            this.update_visibility();
        };
        this._update_filter_elements();
        this.filter_container.replaceChildren(filter_elem);
    };

    _update_filter_element = (entry) => {
        let color = this._to_rgba(entry.color[0], entry.color[1], entry.color[2], 1);
        if (Object.keys(this.color_filters).length && !this.color_filters[entry.filter_key])
           color = `linear-gradient(0.25turn, #333, ${color}`;
        entry.element.setAttribute("style", `background: ${color}`);
    };

    _update_filter_elements = () => {
        for (const entry of this.color_entries) {
            this._update_filter_element(entry);
        }
        const butt = this.filter_reset_button;
        if (butt) {
            if (Object.keys(this.color_filters).length)
                butt.classList.remove("hidden");
            else
                butt.classList.add("hidden");
        }
    };

    node_filter = (node) => {
        if (this.text_query.length)
            if (node.name_lower.indexOf(this.text_query) < 0)
                return false;

        const keys = Object.keys(this.color_filters);
        if (!keys.length)
            return true;
        for (const key of keys)
            if (this.color_filters[key](node))
                return true;
        return false;
    };

    set_network = (network) => {
        this.network = network;
        this.network.on_selection_changed = this.update_selection;
        const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
        g.setAttribute("class", "world-transform");
        for (const edge of this.network.edges) {
            const edge_g = document.createElementNS("http://www.w3.org/2000/svg", "g");
            edge_g.setAttribute("class", `edge edge-${edge.id}`);
            edge.element = edge_g;
            this._set_title(edge_g,
                `${edge.node_from.name} holds`
                + ` ${(edge.weight*100).toFixed(2)}% of ${edge.node_to.name}`
                + `\n($ ${(edge.sharesDollar).toLocaleString("en")})`
            );
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            edge_g.appendChild(line);
            edge.element_line = line;

            const arrow = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
            edge_g.appendChild(arrow);
            edge.element_polygon = arrow;
            g.appendChild(edge_g);
            this.update_edge(edge);
        }
        for (const node of this.network.nodes) {
            const node_g = document.createElementNS("http://www.w3.org/2000/svg", "g");
            node_g.setAttribute("class", `node node-${node.id}`);
            node.element = node_g;
            this._set_title(
                node_g,
                `${node.name}`
                + `\n$ ${(node.totalHoldingsDollar).toLocaleString("en")} total holdings`
            );
            let elem = document.createElementNS("http://www.w3.org/2000/svg", "circle");
            node_g.appendChild(elem);
            node.element_circle = elem;
            elem = document.createElementNS("http://www.w3.org/2000/svg", "text");
            node_g.appendChild(elem);
            node.element_text = elem;
            elem.setAttribute("style", "stroke: black; font-size: 1rem");
            elem.setAttribute("paint-order", "stroke");
            elem.classList.add("hidden");
            elem.appendChild(document.createTextNode(node.name));
            g.appendChild(node_g);
            this.update_node(node, node_g);
        }
        this.element.appendChild(g);
        this.update_view();
    };

    update_all = () => {
        this.update_view();
        for (const node of this.network.nodes) {
            this.update_node(node);
        }
        this.update_edges();
    };

    update_edges = () => {
        for (const edge of this.network.edges) {
            this.update_edge(edge);
        }
    };

    update_view = () => {
        this.element.setAttribute("width", "100%");
        const
            width = this.element.getBoundingClientRect().width,
            height = window.innerHeight - 30;
        this.element.setAttribute("height", `${height}px`);
        this.element.setAttribute("viewBox", `0 0 ${width} ${height}`);
        const tr = this.element.querySelector("g.world-transform");
        if (tr) {
            tr.setAttribute(
                "transform",
                `translate(${width/2.},${height/2})`
                + ` scale(${this.view.zoom}) translate(${-this.view.x},${-this.view.y})`
            );
        }
    };
    
    update_visibility = () => {
        for (const node of this.network.nodes) {
            if (node.hidden)
                node.element.classList.add("hidden");
            else
                node.element.classList.remove("hidden");
        }
        for (const edge of this.network.edges) {
            if (edge.hidden)
                edge.element.classList.add("hidden");
            else
                edge.element.classList.remove("hidden");
        }
    };

    update_node = (node) => {
        const color = this.node_color(node);
        const elem = node.element;
        elem.setAttribute("style", `fill: ${color}`);
        if (node.hidden)
            elem.classList.add("hidden");
        else
            elem.classList.remove("hidden");
        if (node.selected) {
            elem.classList.add("selected");
            /*
            node.element_text.classList.remove("hidden");
            // TODO: bb is zero-sized
            const bb = node.element_text.getBoundingClientRect();
            node.element_text.setAttribute("transform", `translate(${-bb.width/2},0)`);
            node.element_text.setAttribute("style", `fill: ${color}; stroke: black`);
            node.element_text.setAttribute("x", `${node.x}`);
            node.element_text.setAttribute("y", `${node.y}`);
             */
        } else {
            elem.classList.remove("selected");
            elem.querySelector("text").classList.add("hidden");
        }
        node.element_circle.setAttribute("r", `${node.radius}`);
        node.element_circle.setAttribute("cx", `${node.x}`);
        node.element_circle.setAttribute("cy", `${node.y}`);
    };

    update_edge = (edge) => {
        const elem = edge.element;
        const color = this.edge_color(edge);
        let width = edge.width;
        if (edge.selected)
            width = width + 3.;
        elem.setAttribute(
            "style",
            `stroke: ${color}; fill: ${color}; stroke-width: ${width}px`
        );
        const [x1, y1, x2, y2, x3, y3, x4, y4] = this._get_edge_positions(edge);
        edge.element_line.setAttribute("x1", `${x1}`);
        edge.element_line.setAttribute("y1", `${y1}`);
        edge.element_line.setAttribute("x2", `${x2}`);
        edge.element_line.setAttribute("y2", `${y2}`);
        edge.element_polygon.setAttribute("points", `${x2},${y2} ${x3},${y3} ${x4},${y4}`);
    };

    update_selection = (
        selected_nodes, unselected_nodes,
        selected_edges, unselected_edges,
    ) => {
        for (const node of unselected_nodes) {
            this.update_node(node);
        }
        for (const edge of unselected_edges) {
            this.update_edge(edge);
        }
        for (const edge of selected_edges) {
            const elem = edge.element;
            const parent = elem.parentElement;
            parent.removeChild(elem);
            this.update_edge(edge);
            parent.appendChild(elem);
        }
        for (const node of selected_nodes) {
            const elem = node.element;
            const parent = elem.parentElement;
            parent.removeChild(elem);
            this.update_node(node);
            parent.appendChild(elem);
        }
    };

}


window.addEventListener("DOMContentLoaded", () => {

    const NODE_NUMBER_VALUES = [
        {key: "totalShares", label: "total number of shares"},
        {key: "totalHoldingsDollar", label: "market value of held shares", exponent: 6},
        {key: "pagerank", label: "graph PageRank"},
        {key: "hub", label: "graph hub"},
        {key: "authority", label: "graph authority"},
        {key: "hubOrAuthority", label: "graph max(hub, authority)"},
        {key: "num_edges", label: "number of connections", exponent: 4},
        {key: "num_edges_in", label: "number of input connections"},
        {key: "num_edges_out", label: "number of output connections", exponent: 4},
        {key: "x", label: "x position"},
        {key: "y", label: "y position"},
    ];
    const NODE_CATEGORY_VALUES = [
        {key: "type", label: "node type"},
        {key: "sector", label: "sector"},
        {key: "region", label: "region"},
        {key: "industry", label: "industry"},
        {key: "community1", label: "community 1"},
        {key: "community2", label: "community 2"},
    ];
    const EDGE_NUMBER_VALUES = [
        {key: "sharesPercent", label: "percent of total shares"},
        {key: "sharesDollar", label: "market value of held shares"},
    ];

    const CONTROLS = {
        "query": {
            "type": "string", value: "",
            "headline": "query",
            "title": "search for part of name",
        },
        "radius": {
            type: "select", value: "totalHoldingsDollar",
            headline: "radius", title: "what value determines the radius of the spheres",
            choices: NODE_NUMBER_VALUES,
        },
        "min_radius": {
            type: "number", value: 20, min: 1, max: 1000, step: 10,
            label: "min radius", title: "the smallest possible radius"
        },
        "max_radius": {
            type: "number", value: 60, min: 1, max: 1000, step: 10,
            label: "max radius", title: "the largest possible radius"
        },
        "edge_width": {
            type: "select", value: "sharesPercent",
            headline: "connection width", title: "what value determines the with of a connection",
            choices: EDGE_NUMBER_VALUES,
        },
        "min_edge_width": {
            type: "number", value: 2, min: 1, max: 1000,
            label: "minimum", title: "the thinest possible connection width"
        },
        "max_edge_width": {
            type: "number", value: 10, min: 1, max: 1000,
            label: "maximum", title: "the thickest possible connection width"
        },
        "color": {
            type: "select",
            //value: "category_industry",
            //value: "value_authority",
            value: "value_totalHoldingsDollar",
            headline: "color", title: "what value determines the radius of the spheres",
            choices: NODE_NUMBER_VALUES.map(v => ({
                key: `value_${v.key}`, label: `value: ${v.label}`, exponent: v.exponent
            })).concat(NODE_CATEGORY_VALUES.map(v => ({
                key: `category_${v.key}`, label: `category: ${v.label}`
            }))),
        },
        "select_distance": {
            type: "number", value: 0, min: 0, max: 1000,
            headline: "selection distance",
            title: "How many additional paths to highlight when selecting a company."
                + "\nClick a company multiple times to increase the distance."
        },
        "transparency": {
            type: "number", value: 0.6, min: 0, max: 1, step: 0.1,
            headline: "transparency", title: "The transparency of the unselected graph."
        },
    };

    render_controls();

    const diagram = new Diagram(
        document.querySelector("#network"),
        document.querySelector(".palette"),
    );
    window.diagram = diagram;
    let network = null;
    let selected_root_node = null;

    fetch("data/graph.json")
        .then(response => response.json())
        .then(data => {
            network = new Network(data.nodes, data.edges);
            window.network = network;
            //const {x, y} = network.node_map["61322"];
            const [x, y] = network.center();
            diagram.view.x = x;
            diagram.view.y = y;
            diagram.view.zoom = .167;
            diagram.set_network(network);
            // TODO: each causes an update of the svg
            on_control_change(CONTROLS["radius"], CONTROLS["radius"].value);
            on_control_change(CONTROLS["edge_width"], CONTROLS["edge_width"].value);
            on_control_change(CONTROLS["color"], CONTROLS["color"].value);
        });

    let last_traverse_distance = null;
    diagram.on_click = (x, y) => {
        if (!network)
            return;
        const node = network.node_at(x, y);
        console.log(diagram.view, node);
        if (node) {
            let distance = CONTROLS["select_distance"].value;
            if (node === selected_root_node) {
                if (!last_traverse_distance)
                    distance = distance + 1;
                else
                    distance = last_traverse_distance + 1;
                last_traverse_distance = distance;
            } else {
                last_traverse_distance = null;
            }
            selected_root_node = node;
            const nodes = network.traverse_nodes(node, distance);
            network.set_selected(nodes, distance);
        } else {
            selected_root_node = null;
            network.set_selected([]);
        }
    };
    
    function render_controls() {
        const container = document.querySelector(".controls");
        for (const name of Object.keys(CONTROLS)) {
            const control = CONTROLS[name];
            control.name = name;
            if (!control.element) {
                if (control.type === "select") {
                    control.element = document.createElement("select");
                    control.element.setAttribute("name", control.name);
                    for (const opt of control.choices) {
                        const e = document.createElement("option");
                        e.setAttribute("value", opt.key);
                        if (opt.key === control.value)
                            e.setAttribute("selected", "selected");
                        e.innerText = opt.label;
                        control.element.appendChild(e);
                    }
                    control.element.addEventListener("change", e => {
                        on_control_change(control, control.element.value);
                    });
                } else {
                    control.element = document.createElement("input");
                    control.element.setAttribute("type", control.type);
                    control.element.setAttribute("name", control.name);
                    control.element.setAttribute("value", control.value);
                    if (control.min !== undefined)
                        control.element.setAttribute("min", control.min);
                    if (control.max !== undefined)
                        control.element.setAttribute("max", control.max);
                    if (control.step !== undefined)
                        control.element.setAttribute("step", control.step);
                    control.element.addEventListener("change", e => {
                        on_control_change(control, control.element.value);
                    });
                }
                const wrapper = document.createElement("div");
                wrapper.setAttribute("class", "control");
                wrapper.setAttribute("title", control.title);

                const label = document.createElement("div");
                const text = document.createTextNode(control.label || control.headline);
                label.appendChild(text);
                if (control.headline)
                    label.setAttribute("class", "headline");
                wrapper.appendChild(label);
                wrapper.appendChild(control.element);
                container.appendChild(wrapper);
                if (control.name === "color") {
                    const elem = document.createElement("div");
                    elem.setAttribute("class", "palette");
                    container.appendChild(elem);
                }
            }
        }
    }

    function on_control_change(control, value) {
        if (!network)
            return;

        if (control.type === "number")
            value = parseFloat(value);
        control.value = value;

        switch (control.name) {
            case "query":
                diagram.set_text_query(CONTROLS["query"].value);
                diagram.update_visibility();
                break;

            case "radius":
            case "min_radius":
            case "max_radius":
                network.update_radius(
                    CONTROLS["radius"].value,
                    CONTROLS["min_radius"].value,
                    CONTROLS["max_radius"].value,
                );
                diagram.update_all();
                break;

            case "edge_width":
            case "min_edge_width":
            case "max_edge_width":
                network.update_edge_width(
                    CONTROLS["edge_width"].value,
                    CONTROLS["min_edge_width"].value,
                    CONTROLS["max_edge_width"].value,
                );
                diagram.update_edges();
                break;

            case "transparency":
                diagram.set_transparency(value);
                diagram.update_all();
                break;

            case "color": {
                let exponent = 1.;
                for (const ch of CONTROLS["color"]["choices"])
                    if (ch.key === value && ch.exponent)
                        exponent = ch.exponent;
                diagram.set_color_mode(value, exponent);
                diagram.update_all();
                break;
            }

            case "select_distance":
                if (selected_root_node) {
                    const nodes = network.traverse_nodes(selected_root_node, value);
                    network.set_selected(nodes, value);
                }
                break;
        }
    }

});
