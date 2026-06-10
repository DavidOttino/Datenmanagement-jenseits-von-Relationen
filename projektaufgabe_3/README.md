# Projektaufgabe 3 - Phase 1

Phase 1 implements the EDGE model required by the assignment document.

`edge_model.py` parses `toy_example.txt`, transforms the document into the
required tree shape, and creates an in-memory EDGE model:

- `bib`
- `venue` nodes (`vldb`, `sigmod`, `icde`)
- `year` nodes (`vldb_2023`, ...)
- publication nodes (`article`, `inproceedings`, ...)
- field nodes (`author`, `title`, `pages`, ...)

The assignment says `mdate` and `orcid` can be ignored. They are XML attributes,
so they are not inserted into the EDGE model.

Print the transformed toy tree:

```powershell
python .\projektaufgabe_3\phase1_demo.py
```

Create and fill the PostgreSQL EDGE tables, then print the required Phase 1 axis
checks:

```powershell
python .\projektaufgabe_3\phase1_setup.py
```

The database connection is defined in `connection.py` and follows the same
`.env` variables as project assignments 1 and 2:

- `DB_NAME`, default `e_commerce`
- `DB_USER`
- `DB_PASS`
- `DB_HOST`, default `localhost`
- `DB_PORT`, default `5432`

Data is saved when `save_edge_model()` calls `conn.commit()`. If an exception
happens before that commit, the inserted rows are rolled back by PostgreSQL.

The DB schema is:

```sql
node(id, s_id, type, content)
edge(from_id, to_id)
```

`edge_axes.py` computes the required Phase 1 axes from these tables:

- `ancestor`
- `descendant`
- `following-sibling`
- `preceding-sibling`

The recursive axes are implemented with recursive SQL joins over `edge`.

Run the tests:

```powershell
python -m unittest .\projektaufgabe_3\test_phase1.py
```
