import os, psycopg2, psycopg2.extras

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

rw = psycopg2.connect(RAILWAY_URL)
ne = psycopg2.connect(NEON_URL)
rw.autocommit = True
ne.autocommit = False

rw_cur = rw.cursor()
ne_cur = ne.cursor()

for tabla in TABLAS:
    print(f"Sincronizando {tabla}...")
    cols = get_cols(rw_cur, tabla)

    if has_col(ne_cur, tabla, 'updated_at'):
        ne_cur.execute(f"SELECT COALESCE(MAX(updated_at), MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    else:
        ne_cur.execute(f"SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    last = ne_cur.fetchone()[0]

    if has_col(rw_cur, tabla, 'updated_at'):
        rw_cur.execute(f"SELECT * FROM {tabla} WHERE COALESCE(updated_at, created_at) > %s", (last,))
    else:
        rw_cur.execute(f"SELECT * FROM {tabla} WHERE created_at > %s", (last,))

    rows = rw_cur.fetchall()
    if not rows:
        print(f"  Sin cambios")
        continue

    col_str = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    update_set = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c != 'id'])

    ne_cur.executemany(
        f"INSERT INTO {tabla} ({col_str}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_set}",
        rows
    )
    ne.commit()
    print(f"  OK {len(rows)} filas")

print("Sync completado")
rw.close()
ne.close()
