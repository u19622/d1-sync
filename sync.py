import os
import psycopg2
from psycopg2.extras import Json

RAILWAY_URL = os.environ['RAILWAY_URL']
NEON_URL    = os.environ['NEON_URL']

TABLAS = ['organizaciones','roles','facultades','planes','features','limites','sedes','programas','profesores','cursos','salones','usuarios','clases','alumnos','ciclos','ciclo_cursos','ciclo_periodos','ciclo_renovacion_jobs','matriculas','asistencia','evaluaciones','alumno_programa_progreso','perfiles_facultades','usuario_facultades_override','configuracion','audit_log','tabla_valores','plan_features','plan_limites','org_feature_overrides','org_limite_overrides','audit_report_recipients']
# 'audit_report_recipients' (Continuidad LXIV): sin columna updated_at, por eso
# va tambien en TABLAS_FULL_SYNC abajo -- un UPDATE (toggle de 'activo' en
# config.js) no mueve created_at, asi que el sync incremental normal nunca lo
# habria detectado.
# 'tabla_valores' ya estaba en TABLAS_FULL_SYNC (abajo) desde antes, pero nunca
# se agregó aquí — el loop principal itera sobre TABLAS, así que nunca se
# sincronizaba en la práctica (ni se chequeaba su drift). Fase 3.14 / hallazgo
# Continuidad LVIII, corregido aquí.
# Orden reordenado (sesión de hoy, Continuidad LXVI+1): topológico por
# dependencias FK -- 'organizaciones' primero (referenciada por casi todas),
# 'ciclos' antes que 'matriculas' (matriculas.ciclo_id depende de ciclos),
# 'matriculas' antes que 'asistencia' (asistencia.matricula_id depende de
# matriculas). Antes, 'matriculas' iba antes que 'ciclos' y todas las tablas
# organizacion_id iban antes que 'organizaciones' -- causa raíz del incidente
# de Continuidad LXV y riesgo latente para el alta de la organización #2.

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
TABLAS_FULL_SYNC = {'roles', 'tabla_valores', 'ciclo_cursos', 'ciclo_periodos', 'ciclo_renovacion_jobs', 'planes', 'features', 'limites', 'plan_features', 'plan_limites', 'org_feature_overrides', 'org_limite_overrides', 'audit_report_recipients'}

# ── Poda de filas huérfanas en Neon ──────────────────────────────────────────
# Railway es la fuente de verdad. upsert() nunca borra filas en Neon (solo
# INSERT ... ON CONFLICT DO UPDATE) -- una fila borrada en Railway queda
# huérfana en Neon para siempre si no se poda explícitamente. Confirmado en
# sesión de hoy: 'matriculas' tenía 43 huérfanas (2063 en Neon vs 2020 en
# Railway). Alcance inicial deliberadamente acotado a 'matriculas' -- PK
# simple, único caso confirmado hoy -- no a las 31 tablas, mismo criterio de
# "un paso a la vez" que el resto de la sesión. 'audit_log' nunca debe entrar
# acá: es un registro histórico, no algo que deba "coincidir" con Railway.
# UMBRAL_PODA es el freno de seguridad: si los huérfanos calculados superan
# este número, no se borra nada en esa tabla y se reporta como fallo -- evita
# que una lectura parcial (conexión cortada a mitad del SELECT id) se
# interprete como "toda la tabla es huérfana" y la vacíe de un saque.
TABLAS_PODABLES = {'matriculas'}
UMBRAL_PODA = 50

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

# ── Sync por tabla, con manejo de errores individual ─────────────────────────
# Cada tabla corre en su propio try/except: si una falla (ej. FK violation por
# una dependencia todavía no resuelta), no tumba las tablas restantes de la
# lista. 'ne.rollback()' es obligatorio en el except: con ne.autocommit=False,
# una excepción a mitad de un INSERT deja la conexión en estado de transacción
# abortada -- sin el rollback, TODAS las tablas siguientes fallarían en
# cascada con "current transaction is aborted", no solo la que tiene el
# problema real. Los fallos se acumulan en 'fallos' y al final del script se
# fuerza un exit no-cero si hubo alguno, para no perder la alerta por email
# (ver incidente Continuidad LXV) aunque el resto haya sincronizado bien.
fallos = []
for tabla in TABLAS:
    print(f"Sincronizando {tabla}...")
    try:
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

    except Exception as e:
        ne.rollback()
        print(f"  ERROR en {tabla}: {e}")
        fallos.append(tabla)
# ─────────────────────────────────────────────────────────────────────────────

# ── Poda de filas huérfanas (ver comentario junto a TABLAS_PODABLES arriba) ──
print("Podando filas huérfanas...")
for tabla in TABLAS_PODABLES:
    print(f"Revisando huérfanos en {tabla}...")
    try:
        rc.execute(f"SELECT id FROM {tabla}")
        ids_railway = {r[0] for r in rc.fetchall()}
        nc.execute(f"SELECT id FROM {tabla}")
        ids_neon = {r[0] for r in nc.fetchall()}
        huerfanos = ids_neon - ids_railway

        if not huerfanos:
            print(f"  Sin huérfanos")
            continue

        if len(huerfanos) > UMBRAL_PODA:
            print(f"  ABORTADO: {len(huerfanos)} huérfanos supera el umbral de seguridad ({UMBRAL_PODA}) -- no se borró nada, revisar manualmente")
            fallos.append(f"poda:{tabla}:{len(huerfanos)}_huerfanos_supera_umbral")
            continue

        ids_lista = list(huerfanos)
        nc.execute(f"DELETE FROM {tabla} WHERE id = ANY(%s)", (ids_lista,))
        ne.commit()
        print(f"  OK {len(huerfanos)} filas huérfanas eliminadas: {sorted(huerfanos)}")

    except Exception as e:
        ne.rollback()
        print(f"  ERROR en poda de {tabla}: {e}")
        fallos.append(f"poda:{tabla}")
# ─────────────────────────────────────────────────────────────────────────────

# ── Resync de secuencias ─────────────────────────────────────────────────────
# Descubre dinámicamente todas las secuencias en Neon y las sincroniza con
# MAX(id) de su tabla asociada. Esto garantiza que si Neon es activada como
# BD primaria (DR-01 Paso 4C), los INSERTs no fallen por duplicate key.
# El descubrimiento es dinámico: tablas nuevas con columna id serial quedan
# cubiertas automáticamente sin modificar este script.
# Los fallos acá también se acumulan en 'fallos' (sesión de hoy) -- antes,
# este bloque tenía su propio try/except aislado del resto: un
# "permission denied" en una secuencia (como pasó con ciclos_id_seq,
# ciclo_cursos_id_seq y alumno_programa_progreso_id_seq antes del GRANT de
# hoy) se imprimía pero el job terminaba "succeeded" igual, sin disparar la
# alerta por email. Mismo tipo de gap que el que motivó el fallos.append()
# del loop principal, aplicado acá también.
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
            fallos.append(f"secuencia:{tabla}.{col}")
    nc2.close()
except Exception as e:
    print(f"ERROR resync secuencias: {e}")
    fallos.append("resync_secuencias")
# ─────────────────────────────────────────────────────────────────────────────

if fallos:
    rc.close()
    nc.close()
    rw.close()
    ne.close()
    raise SystemExit(f"Sync con errores en: {', '.join(fallos)}")

print("Sync completado")
rc.close()
nc.close()
rw.close()
ne.close()
