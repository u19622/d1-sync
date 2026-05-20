import os, json, psycopg2, psycopg2.extras
from psycopg2.extras import Json

RAILWAY_URL = os.environ['RAILWAY_URL']
NEON_URL    = os.environ['NEON_URL']

TABLAS = ['alumnos','matriculas','clases','asistencia','audit_log',
          'usuarios','programas','cursos','profesores','sedes','salones','configuracion']

def get_cols(cur, tabla):
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_name=%s ORDER BY ordinal_position""", (tabla,))
    return [r[0] for r in cur.fetchall()]

def has_col(cur, tabla, col):
    cur.execute("""SELECT COUNT(*) FROM information_schema.columns
                   WHERE table_name=%s AND column_name=%s""", (tabla, col))
    return cur.fetchone()[0] > 0

def get_json_cols(cur, tabla):
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_name=%s AND data_type IN ('json','jsonb')""", (tabla,))
    return [r[0] for r in cur.fetchall()]

def adapt_row(row, json_col_indices):
    row = list(row)
    for i in json_col_indices:
        if row[i] is not None and isinstance(row[i], (dict, list)):
            row[i] = Json(row[i])
    return tuple(row)

rw = psycopg2.connect(RAILWAY_URL)
ne = psycopg2.connect(NEON_URL, cursor_factory=psycopg2.extras.RealDictCursor)
rw.autocommit = True

rw_cur = rw.cursor()
ne_cur = ne.cursor()

for tabla in TABLAS:
    print(f"Sincronizando {tabla}...")
    cols = get_cols(rw_cur, tabla)
    json_cols = get_json_cols(rw_cur, tabla)
    json_col_indices = [cols.index(c) for c in json_cols if c in cols]

    if has_col(ne_cur, tabla, 'updated_at'):
        ne_cur.execute(f"SELECT COALESCE(MAX(updated_at), MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    else:
        ne_cur.execute(f"SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    last = ne_cur.fetchone()[list(ne_cur.fetchone().keys())[0]] if False else ne_cur.fetchone()

    ne2 = psycopg2.connect(NEON_URL)
    ne2_cur = ne2.cursor()

    if has_col(ne2_cur, tabla, 'updated_at'):
        ne2_cur.execute(f"SELECT COALESCE(MAX(updated_at), MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    else:
        ne2_cur.execute(f"SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    last = ne2_cur.fetchone()[0]

    if has_col(rw_cur, tabla, 'updated_at'):
        rw_cur.execute(f"SELECT * FROM {tabla} WHERE COALESCE(updated_at, created_at) > %s", (last,))
    else:
        rw_cur.execute(f"SELECT * FROM {tabla} WHERE created_at > %s", (last,))

    rows = rw_cur.fetchall()
    if not rows:
        print(f"  Sin cambios")
        ne2.close()
        continue

    col_str = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    update_set = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c != 'id'])

    adapted = [adapt_row(r, json_col_indices) for r in rows]
    ne2_cur.executemany(
        f"INSERT INTO {tabla} ({col_str}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_set}",
        adapted
    )
    ne2.commit()
    print(f"  OK {len(rows)} filas")
    ne2.close()

print("Sync completado")
rw.close()
ne_cur.close()
