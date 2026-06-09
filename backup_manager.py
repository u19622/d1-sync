import json, urllib.request, urllib.error, os, sys
from datetime import datetime, timezone, timedelta, date

PROJECT_ID = os.environ['PROJECT_ID']
NEON_API_KEY = os.environ['NEON_API_KEY']

with open('/tmp/branches.json') as f:
    data = json.load(f)

branches = data.get('branches', [])
cutoff = datetime.now(timezone.utc) - timedelta(days=7)

deleted = 0
for b in branches:
    if not b['name'].startswith('backup-'):
        continue
    created = datetime.fromisoformat(b['created_at'].replace('Z', '+00:00'))
    if created < cutoff:
        req = urllib.request.Request(
            f"https://console.neon.tech/api/v2/projects/{PROJECT_ID}/branches/{b['id']}",
            method='DELETE',
            headers={'Authorization': f"Bearer {NEON_API_KEY}"}
        )
        try:
            urllib.request.urlopen(req)
            print(f"Eliminado: {b['name']}")
            deleted += 1
        except urllib.error.HTTPError as e:
            print(f"ERROR eliminando {b['name']}: {e.code}")

print('Branches eliminados: ' + str(deleted) if deleted else 'Sin branches expirados')

names = sorted([b['name'] for b in branches if b['name'].startswith('backup-')])
print('Branches activos: ' + str(names))

today = 'backup-' + date.today().strftime('%Y-%m-%d')
if today in names:
    print('OK - backup del dia verificado: ' + today)
else:
    print('ERROR - backup del dia no encontrado: ' + today)
    sys.exit(1)
