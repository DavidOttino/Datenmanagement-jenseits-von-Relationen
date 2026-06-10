from __future__ import annotations


AXES = {
    "ancestor",
    "descendant",
    "following-sibling",
    "preceding-sibling",
}


def axis(conn, node_id: int, axis_name: str) -> list[tuple[int, str | None, str, str | None]]:
    if axis_name == "ancestor":
        return ancestor(conn, node_id)
    if axis_name == "descendant":
        return descendant(conn, node_id)
    if axis_name == "following-sibling":
        return following_sibling(conn, node_id)
    if axis_name == "preceding-sibling":
        return preceding_sibling(conn, node_id)
    raise ValueError(f"Unsupported axis: {axis_name!r}. Expected one of {sorted(AXES)}")


def ancestor(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH RECURSIVE ancestors(id, s_id, type, content, depth) AS (
                SELECT parent.id, parent.s_id, parent.type, parent.content, 1
                FROM edge e
                JOIN node parent ON parent.id = e.from_id
                WHERE e.to_id = %s

                UNION ALL

                SELECT parent.id, parent.s_id, parent.type, parent.content, ancestors.depth + 1
                FROM edge e
                JOIN node parent ON parent.id = e.from_id
                JOIN ancestors ON ancestors.id = e.to_id
            )
            SELECT id, s_id, type, content
            FROM ancestors
            ORDER BY depth DESC;
            """,
            (node_id,),
        )
        return cur.fetchall()


def descendant(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH RECURSIVE descendants(id, s_id, type, content) AS (
                SELECT child.id, child.s_id, child.type, child.content
                FROM edge e
                JOIN node child ON child.id = e.to_id
                WHERE e.from_id = %s

                UNION ALL

                SELECT child.id, child.s_id, child.type, child.content
                FROM edge e
                JOIN node child ON child.id = e.to_id
                JOIN descendants ON descendants.id = e.from_id
            )
            SELECT id, s_id, type, content
            FROM descendants
            ORDER BY id;
            """,
            (node_id,),
        )
        return cur.fetchall()


def following_sibling(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sibling.id, sibling.s_id, sibling.type, sibling.content
            FROM edge context_edge
            JOIN edge sibling_edge ON sibling_edge.from_id = context_edge.from_id
            JOIN node sibling ON sibling.id = sibling_edge.to_id
            WHERE context_edge.to_id = %s
              AND sibling.id > %s
            ORDER BY sibling.id;
            """,
            (node_id, node_id),
        )
        return cur.fetchall()


def preceding_sibling(conn, node_id: int) -> list[tuple[int, str | None, str, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sibling.id, sibling.s_id, sibling.type, sibling.content
            FROM edge context_edge
            JOIN edge sibling_edge ON sibling_edge.from_id = context_edge.from_id
            JOIN node sibling ON sibling.id = sibling_edge.to_id
            WHERE context_edge.to_id = %s
              AND sibling.id < %s
            ORDER BY sibling.id;
            """,
            (node_id, node_id),
        )
        return cur.fetchall()


def find_one(
    conn,
    *,
    s_id: str | None = None,
    type: str | None = None,
    content: str | None = None,
) -> tuple[int, str | None, str, str | None]:
    predicates = []
    params = []
    if s_id is not None:
        predicates.append("s_id = %s")
        params.append(s_id)
    if type is not None:
        predicates.append("type = %s")
        params.append(type)
    if content is not None:
        predicates.append("content = %s")
        params.append(content)
    if not predicates:
        raise ValueError("At least one search predicate is required.")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, s_id, type, content
            FROM node
            WHERE {" AND ".join(predicates)}
            ORDER BY id
            LIMIT 1;
            """,
            params,
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError("No matching node found.")
    return row
