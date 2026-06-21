"""
Single-Axis XPath Achsen basierend nur auf PRE-Nummern.

Descendant wird als 1D Range Query implementiert:
descendant(v) ⟺ pre_min(v) < pre_min(x) < pre_max(v)
                 AND
                pre_min(v) < pre_max(x) < pre_max(v)
"""

from __future__ import annotations


def descendant_single_axis(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Optimierte Single-Axis Descendant-Suche.
    
    Bedingung (1D Range Query):
        pre_min(v) < pre_min(x) < pre_max(v)
        AND
        pre_min(v) < pre_max(x) < pre_max(v)
    
    Das ist eine konjunktive Bedingung von zwei 1D Range Queries,
    nicht eine 2D Range Query!
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_single ctx
            JOIN accel_single x ON (
                ctx.pre_min < x.pre_min AND x.pre_min < ctx.pre_max
                AND
                ctx.pre_min < x.pre_max AND x.pre_max < ctx.pre_max
            )
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre_min ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def following_sibling_single_axis(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Following-Sibling bleibt gleich (unabhängig von Single-Axis oder nicht).
    
    Bedingung: x.parent = ctx.parent AND ctx.pre_min < x.pre_min
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_single ctx
            JOIN accel_single x ON x.parent = ctx.parent AND ctx.pre_min < x.pre_min
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre_min ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def preceding_sibling_single_axis(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    """
    Preceding-Sibling bleibt gleich.
    
    Bedingung: x.parent = ctx.parent AND x.pre_min < ctx.pre_min
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.id, a.value AS s_id, c.tag AS type, c.text AS content
            FROM accel_single ctx
            JOIN accel_single x ON x.parent = ctx.parent AND x.pre_min < ctx.pre_min
            JOIN node n ON n.id = x.node_id
            JOIN content c ON c.node_id = n.id
            LEFT JOIN attribute a ON a.node_id = n.id AND a.name = 's_id'
            WHERE ctx.node_id = %s
            ORDER BY x.pre_min ASC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def axis_single_axis(conn, node_id: int, axis_name: str) -> list[tuple[int, str | None, str, str | None]]:
    """
    Verteilerfunktion für Single-Axis Achsen.
    """
    if axis_name == "descendant":
        return descendant_single_axis(conn, node_id)
    if axis_name == "following-sibling":
        return following_sibling_single_axis(conn, node_id)
    if axis_name == "preceding-sibling":
        return preceding_sibling_single_axis(conn, node_id)
    raise ValueError(f"Single-Axis unterstützt nur: descendant, following-sibling, preceding-sibling. Erhalten: {axis_name!r}")