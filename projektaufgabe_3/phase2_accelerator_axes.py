from __future__ import annotations

AXES = {
    "ancestor",
    "descendant",
    "following-sibling",
    "preceding-sibling",
}

def axis_accel(conn, node_id: int, axis_name: str) -> list[tuple[int, str | None, str, str | None]]:
    """Verteilerfunktion für die XPath-Accelerator-Achsen."""
    if axis_name == "ancestor":
        return ancestor_accel(conn, node_id)
    if axis_name == "descendant":
        return descendant_accel(conn, node_id)
    if axis_name == "following-sibling":
        return following_sibling_accel(conn, node_id)
    if axis_name == "preceding-sibling":
        return preceding_sibling_accel(conn, node_id)
    raise ValueError(f"Nicht unterstützte Achse: {axis_name!r}. Erwartet wird eine von {sorted(AXES)}")


def ancestor_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """Berechnet die Ancestor-Achse über pre/post-Bedingungen (sortiert von Wurzel zu Kontext)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.pre < ctx.pre AND ctx.post < x.post
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def descendant_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """Berechnet die Descendant-Achse über pre/post-Bedingungen."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON ctx.pre < x.pre AND x.post < ctx.post
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def following_sibling_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Berechnet folgende Geschwisterknoten (following-sibling).
    Bedingung: Gleicher Parent-Knoten und nachfolgende Pre-Order-ID.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.parent = ctx.parent AND ctx.pre < x.pre
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def preceding_sibling_accel(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Berechnet vorangegangene Geschwisterknoten (preceding-sibling).
    Bedingung: Gleicher Parent-Knoten und vorangegangene Pre-Order-ID.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel ctx
            JOIN accel x ON x.parent = ctx.parent AND x.pre < ctx.pre
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()