import os
import psycopg2
from psycopg2.extras import Json

RAILWAY_URL = os.environ['RAILWAY_URL']
NEON_URL    = os.environ['NEON_URL']

TABLAS = ['alumnos','matriculas','clases','asistencia','audit_log',
          'usuarios','programas','cursos','profesores','sedes','salones','configuracion']

rw = psycopg2.connect(RAILWAY_URL)
ne = psycopg2.connect(NEON_URL)
rw.autocommit = True
ne.autocommit = False
rc = rw.cursor()
nc = ne.cursor()

def get_cols(cur, tabla):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (tabla,))
    return [r[0] for r in cur.fetchall()]

def has_col(cur, tabla, col):
    cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (tabla, col))
    return cur.fetchone()[0] > 0

def get_json_cols(cur, tabla):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s AND data_type IN ('json','jsonb')", (tabla,))
    return [r[0] for r in cur.fetchall()]

def adapt_row(row, json_indices):
    row = list(row)
    for i in json_indices:
        if row[i] is not None and isinstance(row[i], (dict, list)):
            row[i] = Json(row[i])
    return tuple(row)

for tabla in TABLAS:
    print(f"Sincronizando {tabla}...")
    cols = get_cols(rc, tabla)
    json_cols = get_json_cols(rc, tabla)
    json_indices = [cols.index(c) for c in json_cols if c in cols]

    if has_col(nc, tabla, 'updated_at'):
        nc.execute(f"SELECT COALESCE(MAX(updated_at), MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    else:
        nc.execute(f"SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    last = nc.fetchone()[0]

    if has_col(rc, tabla, 'updated_at'):
        rc.execute(f"SELECT * FROM {tabla} WHERE COALESCE(updated_at, created_at) > %s", (last,))
    else:
        rc.execute(f"SELECT * FROM {tabla} WHERE created_at > %s", (last,))

    rows = rc.fetchall()
    if not rows:
        print(f"  Sin cambios")
        continue

    col_str    = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    update_set = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c != 'id'])
    adapted    = [adapt_row(r, json_indices) for r in rows]

    nc.executemany(
        f"INSERT INTO {tabla} ({col_str}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_set}",
        adapted
    )
    ne.commit()
    print(f"  OK {len(rows)} filas")

print("Sync completado")
rc.close()
nc.close()
rw.close()
ne.close()
