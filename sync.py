import os
import psycopg2
from psycopg2.extras import Json

RAILWAY_URL = os.environ['RAILWAY_URL']
NEON_URL    = os.environ['NEON_URL']

TABLAS = ['roles','sedes','programas','cursos','salones','facultades','profesores','usuarios','clases','alumnos','matriculas','asistencia','perfiles_facultades','usuario_facultades_override','configuracion','audit_log']

PK_COMPUESTA = {
    'perfiles_facultades': '(perfil_id, facultad_id)',
}
PK_EXCLUIR = {'perfiles_facultades': {'perfil_id','facultad_id'}}
TABLAS_FULL_SYNC = {'roles', 'tabla_valores'}

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
    import json as _json
    row = list(row)
    for i in json_indices:
        if row[i] is not None:
            if isinstance(row[i], str):
                row[i] = Json(_json.loads(row[i]))
            elif isinstance(row[i], (dict, list)):
                row[i] = Json(row[i])
    return tuple(row)

def upsert(nc, ne, tabla, cols, rows, json_indices):
    col_str      = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    pk_conflict  = PK_COMPUESTA.get(tabla, '(id)')
    excluir      = PK_EXCLUIR.get(tabla, {'id'})
    update_set   = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c not in excluir])
    adapted      = [adapt_row(r, json_indices) for r in rows]
    nc.executemany(
        f"INSERT INTO {tabla} ({col_str}) VALUES ({placeholders}) ON CONFLICT {pk_conflict} DO UPDATE SET {update_set}",
        adapted
    )
    ne.commit()

for tabla in TABLAS:
    print(f"Sincronizando {tabla}...")
    cols       = get_cols(rc, tabla)
    json_cols  = get_json_cols(rc, tabla)
    json_indices = [cols.index(c) for c in json_cols if c in cols]

    if tabla in TABLAS_FULL_SYNC:
        rc.execute(f"SELECT * FROM {tabla}")
        rows = rc.fetchall()
        if not rows:
            print(f"  Sin datos")
            continue
        upsert(nc, ne, tabla, cols, rows, json_indices)
        print(f"  OK {len(rows)} filas (full sync)")
        continue

    tiene_updated = has_col(nc, tabla, 'updated_at')
    tiene_created = has_col(nc, tabla, 'created_at')

    if tiene_updated and tiene_created:
        nc.execute(f"SELECT COALESCE(MAX(updated_at), MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    elif tiene_updated:
        nc.execute(f"SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz) FROM {tabla}")
    elif tiene_created:
        nc.execute(f"SELECT COALESCE(MAX(created_at), '1970-01-01'::timestamptz) FROM {tabla}")
    else:
        print(f"  Sin columna de fecha, omitiendo")
        continue
    last = nc.fetchone()[0]

    tiene_updated_rw = has_col(rc, tabla, 'updated_at')
    tiene_created_rw = has_col(rc, tabla, 'created_at')

    if tiene_updated_rw and tiene_created_rw:
        rc.execute(f"SELECT * FROM {tabla} WHERE COALESCE(updated_at, created_at) > %s", (last,))
    elif tiene_updated_rw:
        rc.execute(f"SELECT * FROM {tabla} WHERE updated_at > %s", (last,))
    elif tiene_created_rw:
        rc.execute(f"SELECT * FROM {tabla} WHERE created_at > %s", (last,))
    else:
        rc.execute(f"SELECT * FROM {tabla}")

    rows = rc.fetchall()
    if not rows:
        print(f"  Sin cambios")
        continue

    upsert(nc, ne, tabla, cols, rows, json_indices)
    print(f"  OK {len(rows)} filas")

print("Sync completado")
rc.close()
nc.close()
rw.close()
ne.close()
