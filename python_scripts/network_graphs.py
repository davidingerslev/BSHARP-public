import pygraphviz as pgv
import seaborn as sns
import numpy as np
from .distinct_pathways import dpw_start_dt, dpw_end_dt


def mkDgraph(
    E,
    title=None,
    threshold=0,
    min_n=3,
    individuals=None,
    edgelabel="xlabel",
    sorted_end_cats=None,
    capacities=None,
):
    sns.set()
    colours = sns.color_palette().as_hex()
    cidx = 0

    def get_next_colour():
        nonlocal cidx, colours
        if cidx >= len(colours):
            raise ValueError("All the colours have been used")
        else:
            colour = colours[cidx]
            cidx += 1
            return colour

    def update_lim(x, y):
        nonlocal lim
        lim["x"]["min"] = min(x, lim["x"]["min"])
        lim["x"]["max"] = max(x, lim["x"]["min"])
        lim["y"]["min"] = min(y, lim["y"]["min"])
        lim["y"]["max"] = max(y, lim["y"]["min"])

    def get_node_hw(node):
        # Area of node should be proportional to size
        # Area of elipse: A = πab, a= half width, b = half height
        # So sizemult = π * height/2 * height/2*aspect; height = sqrt(4 * aspect * sizemult/π)
        aspect = 0.618
        if capacities is not None:
            capacity = capacities[node]
            sizemult = capacity / min(capacities.values())
        else:
            sizemult = 1
        h = np.sqrt(4 * aspect * sizemult / np.pi)
        w = h / aspect
        return h, w

    # Set variables depending whether this is a proportion or count graph:
    if E.weight.min() < 1:
        precision = 3
        threshold_pct = threshold
        title_prefix = "Proportions"
        graphtype = "Proportions of moves out of each node"
        domain = "or more of moves from that node, minimum of 3 moves between nodes"
    else:
        precision = 0
        threshold_pct = threshold / E.weight.max()
        title_prefix = "Counts"
        graphtype = "Counts of moves between nodes"
        domain = "of the maximum number of moves between nodes"

    # Create labels for nodes, including counts
    nodelabels = {}
    for node in ["ENTRY", "_RETURN"]:
        # n_0 = source_counts[node] if node in source_counts else 0
        n_0 = E[E.source == node].n.sum()
        nodelabels[node] = node + r"\n" + f"n={n_0}"
    for node in ["END", "RETURN"]:
        n_0 = E[E.target == node].n.sum()
        nodelabels[node] = node + r"\n" + f"n={n_0}"
    extantsizes = E[E.source == "EXTANT"].set_index("target").n
    # Get rid of EXTANT node now we have logged the sizes:
    E = E[E.source != "EXTANT"]

    # Apply the min/threshold conditions to reduce less-significant edges
    validE = E[(E.n >= min_n) & (E.weight >= threshold)]
    # Exclude edges out of nodes where there are no incoming nodes
    nodes_to_keep = (
        ["ENTRY", "_RETURN", "EXTANT"]
        +
        # Target nodes with any incoming edges at/above threshold
        validE.groupby("target")
        .weight.agg(lambda wt: (wt >= threshold).any())[lambda tru: tru]
        .index.to_list()
    )
    validE = validE[validE.source.isin(nodes_to_keep)]
    # Add node attributes
    validE = validE.assign(
        adj_weight=lambda x: np.sqrt((x.weight - x.weight.min()) / x.weight.max()),
        penwidth=lambda x: ((8 - 0.1) * x.adj_weight) + 0.1,  # Scale 0.2 to 4
        arrowsize=lambda x: (2 / x.penwidth).where(x.penwidth >= 1, other=1.5),
        headport="w",
        tailport="e",
        fontcolor=lambda x: x.adj_weight.apply(
            lambda y: "0 0 %.3f" % (0.8 - (0.8 * y))
        ),  # HSV
        color=lambda x: x.adj_weight.apply(
            lambda y: "0 0 %.3f" % (0.6 - (0.6 * y))
        ),  # HSV
    )
    if edgelabel == "xlabel":
        wtlabel = validE.weight.apply(
            lambda w: """<<table border="0" cellpadding="0"><tr><td bgcolor="white">"""
            + f"""{f"{w:.{precision}f}"}"""
            + """</td></tr></table>>"""
        )
        validE = validE.assign(xlabel=wtlabel)
    elif edgelabel == "label":
        validE = validE.assign(
            label=validE.weight.apply(lambda w: f"""{w:.{precision}f}""")
        )

    # Add pathways nodes in fixed positions:
    dpw_pways = {
        "M": [f"M L{n}" for n in [1, 2, 3, 4]],
        "SU": [f"SU L{n}" for n in [1, 2]],
        "M/F": [f"M/F L{n}" for n in [1, 2, 3, 4]],
        "F": [f"F L{n}" for n in [1, 2, 3, 4]],
    }
    DG = pgv.AGraph(strict=False, directed=True)
    DG.node_attr.update(style="filled")
    x_start = 0
    y_start = 0
    x_mult = 4
    y_mult = 4
    y_lvl = 0
    lim = {"x": {"min": x_start, "max": x_start}, "y": {"min": y_start, "max": y_start}}
    for pway, nodes in dpw_pways.items():
        colour = get_next_colour()
        for i in range(len(nodes)):
            node = nodes[i]
            x_lvl = i
            y_mod = 0
            if pway == "SU":
                x_lvl += 0.5
            elif node[-2:] == "L2":
                y_mod = 0.25
            elif node[-2:] == "L3":
                y_mod = -0.25
            x = (x_lvl * x_mult) + x_start
            y = (y_lvl * y_mult) + y_start + y_mod
            update_lim(x, y)
            h, w = get_node_hw(node)
            extantsize = extantsizes[node] if node in extantsizes else 0
            lab = node + r"\n" + f"n₀={extantsize}"
            DG.add_node(
                node, fillcolor=colour, pos=f"{x},{y}!", height=h, width=w, label=lab
            )
        y_lvl += 1

    # Default height/width for next few nodes
    aspect = 0.618
    h = 0.7
    w = h / aspect

    # End reasons added later on using this colour
    end_cats_colour = get_next_colour()

    # Add ENTRY, END and entrytype nodes in fixed positions
    colour = "darkgrey"
    y = lim["y"]["min"] + ((lim["y"]["max"] - lim["y"]["min"]) / 2) + y_mult / 2
    x = lim["x"]["min"] - 2 * x_mult
    DG.add_node(
        "ENTRY",
        label=nodelabels["ENTRY"],
        fillcolor=colour,
        pos=f"{x},{y}!",
        height=h,
        width=w,
        shape="box",
        style="rounded,filled",
    )
    x = lim["x"]["max"] + 2.5 * x_mult
    DG.add_node(
        "END",
        label=nodelabels["END"],
        fillcolor=colour,
        pos=f"{x},{y}!",
        height=h,
        width=w,
        shape="box",
        style="rounded,filled",
    )
    x = lim["x"]["min"] - 1.25 * x_mult
    y = y + 0.25 * y_mult
    DG.add_node(
        "New",
        pos=f"{x},{y}!",
        height=h,
        width=w,
        fontcolor=colour,
        color=colour,
        shape="box",
        style="bold,rounded,dashed",
    )
    has_known_other = (
        (validE.source == "Known (other)").sum()
        + (validE.target == "Known (other)").sum()
    ) > 0
    if has_known_other:
        y = y - 0.5 * y_mult
        DG.add_node(
            "Known (other)",
            pos=f"{x},{y}!",
            height=h,
            width=w,
            label="Known\n(other)",
            fontcolor=colour,
            color=colour,
            shape="box",
            style="bold,rounded,dashed",
        )
        y = y + 0.25 * y_mult
    else:
        y = y - 0.5 * y_mult
        DG.add_node(
            "Known (adult pw)",
            pos=f"{x},{y}!",
            height=h,
            width=w,
            label="Known\n(adult pw)",
            fontcolor=colour,
            color=colour,
            shape="box",
            style="bold,rounded,dashed",
        )

    # Add RETURN nodes in fixed positions
    colour = get_next_colour()
    y = lim["y"]["min"] + ((lim["y"]["max"] - lim["y"]["min"]) / 2) - y_mult / 2
    x = lim["x"]["max"] + 2.5 * x_mult
    DG.add_node(
        "RETURN",
        label=nodelabels["RETURN"],
        fillcolor=colour,
        pos=f"{x},{y}!",
        height=h,
        width=w,
        shape="box",
        style="rounded,filled",
    )
    x = lim["x"]["min"] - 2 * x_mult
    DG.add_node(
        "_RETURN",
        label=nodelabels["RETURN"],
        fillcolor=colour,
        pos=f"{x},{y}!",
        height=h,
        width=w,
        shape="box",
        style="rounded,filled",
    )

    # Add all the edges
    for _, edge in validE.iterrows():
        edge_properties = edge.to_dict()
        for prop in ["n", "weight", "adj_weight"]:
            del edge_properties[prop]
        if edge.weight >= threshold and edge.n >= min_n:
            DG.add_edge(edge.source, edge.target, **edge_properties)

    # Set attributes for exittype nodes
    end_cat_nodes = []
    for node in DG.nodes():
        if node.name in sorted_end_cats:
            node.attr["color"] = end_cats_colour
            node.attr["fontcolor"] = end_cats_colour
            node.attr["shape"] = "box"
            node.attr["style"] = "bold, dashed, rounded"
            end_cat_nodes.append(node)
    # Distribute nodes vertically
    cat_y_min = lim["y"]["min"] + (0.25 * y_mult)
    cat_y_max = lim["y"]["max"] - (0.25 * y_mult)
    if len(end_cat_nodes) > 1:
        cat_y_sep = (cat_y_max - cat_y_min) / (len(end_cat_nodes) - 1)
    else:
        cat_y_sep = 0
    x = lim["x"]["max"] + 1.5 * x_mult
    y = cat_y_min
    if sorted_end_cats is not None:
        end_cat_nodes.sort(key=lambda x: sorted_end_cats[x], reverse=True)
    for node in end_cat_nodes:
        node.attr["pos"] = f"{x},{y}!"
        y = y + cat_y_sep

    # Set graph attributes (labels, etc.)
    graphlabel = (
        graphtype
        + f", {dpw_start_dt.day} {dpw_start_dt:%b %Y} to {dpw_end_dt.day} {dpw_end_dt:%b %Y}."
        + " Only moves above a threshold are included"
        + f" ({threshold:.{precision}f}, {(100 * (threshold_pct)):.1f}% {domain}, or {min_n}, whichever is larger). "
        + "'n' = total number of people; 'n₀' = number resident on the first date."
    )
    if individuals is not None:
        graphlabel = graphlabel + f" {individuals} individuals."
    DG.graph_attr["label"] = graphlabel
    DG.graph_attr["splines"] = True

    # Add a title
    if title is not None:
        title = title_prefix + ": " + title
    else:
        title = title_prefix
    x = (lim["x"]["max"] - lim["x"]["min"]) / 2
    y = lim["y"]["max"] + 0.5 * y_mult
    w = lim["x"]["max"] + 4 * x_mult
    DG.add_node(
        "title",
        pos=f"{x},{y}!",
        width=w,
        shape="box",
        color="0 0 1",
        fontsize="22pt",
        label=title + r"\l",
    )

    return DG
