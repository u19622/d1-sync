import os
import psycopg2
from psycopg2.extras import Json

RAILWAY_URL = os.environ['RAILWAY_URL']
NEON_URL    = os.environ['NEON_URL']

TABLAS = ['roles','sedes','programas','cursos','salones','facultades','profesores','usuarios','clases','alumnos','matriculas','ciclos','ciclo_cursos','ciclo_periodos','ciclo_renovacion_jobs','asistencia','perfiles_facultades','usuario_facultades_override','configuracion','audit_log','evaluaciones','alumno_programa_progreso','tabla_valores','organizaciones','planes','features','limites','plan_features','plan_limites','org_feature_overrides','org_limite_overrides']
# 'tabla_valores' ya estaba en TABLAS_FULL_SYNC (abajo) desde antes, pero nunca
# se agregó aquí — el loop principal itera sobre TABLAS, así que nunca se
# sincronizaba en la práctica (ni se chequeaba su drift). Fase 3.14 / hallazgo
# Continuidad LVIII, corregido aquí.

PK_COMPUESTA = {
    'perfiles_facultades': '(perfil_id, facultad_id)',
    'plan_features': '(plan_id, feature_id)',
    'plan_limites': '(plan_id, limite_id)',
    'org_feature_overrides': '(organizacion_id, feature_id)',
    'org_limite_overrides': '(organizacion_id, limite_id)',
}
PK_EXCLUIR = {
    'perfiles_facultades': {'perfil_id','facultad_id'},
    'plan_features': {'plan_id','feature_id'},
    'plan_limites': {'plan_id','limite_id'},
    'org_feature_overrides': {'organizacion_id','feature_id'},
    'org_limite_overrides': {'organizacion_id','limite_id'},
}
TABLAS_FULL_SYNC = {'roles', 'tabla_valores', 'ciclo_cursos', 'ciclo_periodos', 'ciclo_renovacion_jobs'}

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
                if row[i].strip():
                    try:
                        row[i] = Json(_json.loads(row[i]))
                    except _json.JSONDecodeError:
                        row[i] = None
                else:
                    row[i] = None
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

# ── Detección de schema drift ────────────────────────────────────────────────
# Compara columnas de cada tabla en Railway vs Neon antes de sincronizar.
# Si Railway tiene columnas que no existen en Neon, aborta con mensaje legible.
# Esto previene errores crípticos de psycopg2 mid-sync y garantiza que Neon
# esté estructuralmente al día antes de cualquier conmutación DRP.
print("Verificando schema drift...")
drift_detectado = False
for tabla in TABLAS:
    cols_railway = get_cols(rc, tabla)
    cols_neon    = get_cols(nc, tabla)
    faltantes    = [c for c in cols_railway if c not in cols_neon]
    if faltantes:
        print(f"  DRIFT en '{tabla}': Railway tiene {faltantes} que no existen en Neon")
        drift_detectado = True
    else:
        print(f"  OK {tabla}")
if drift_detectado:
    rw.close()
    ne.close()
    raise SystemExit("Schema drift detectado. Aplicar migraciones en Neon antes de continuar.")
print("Schema OK — sin drift detectado")
# ─────────────────────────────────────────────────────────────────────────────

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

# ── Resync de secuencias ─────────────────────────────────────────────────────
# Descubre dinámicamente todas las secuencias en Neon y las sincroniza con
# MAX(id) de su tabla asociada. Esto garantiza que si Neon es activada como
# BD primaria (DR-01 Paso 4C), los INSERTs no fallen por duplicate key.
# El descubrimiento es dinámico: tablas nuevas con columna id serial quedan
# cubiertas automáticamente sin modificar este script.
print("Resyncing secuencias...")
try:
    nc2 = ne.cursor()
    nc2.execute("""
        SELECT s.relname AS seq, t.relname AS tabla, a.attname AS col
        FROM pg_class s
        JOIN pg_depend d ON d.objid = s.oid
        JOIN pg_class t ON d.refobjid = t.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE s.relkind = 'S' AND n.nspname = 'public'
        ORDER BY t.relname
    """)
    secuencias = nc2.fetchall()
    for seq, tabla, col in secuencias:
        try:
            nc2.execute(f"SELECT COALESCE(MAX({col}), 1) FROM {tabla}")
            max_id = nc2.fetchone()[0]
            nc2.execute(f"SELECT last_value FROM {seq}")
            last_val = nc2.fetchone()[0]
            if max_id > last_val:
                nc2.execute(f"SELECT setval('{seq}', %s)", (max_id,))
                ne.commit()
                print(f"  {tabla}.{col}: {last_val} -> {max_id}")
            else:
                print(f"  {tabla}.{col}: OK ({last_val})")
        except Exception as e:
            ne.rollback()
            print(f"  ERROR en {tabla}.{col}: {e}")
    nc2.close()
except Exception as e:
    print(f"ERROR resync secuencias: {e}")
# ─────────────────────────────────────────────────────────────────────────────

print("Sync completado")
rc.close()
nc.close()
rw.close()
ne.close()
