#!/usr/bin/env bash
# Vérifications avant la démo.
# Depuis la racine du dépôt, venv activé :   bash demo/preflight.sh
cd "$(dirname "$0")/.." || exit 1
export PYTHONPATH=. CASSANDRA_SINGLE_HOST=1 CASSANDRA_HOSTS=127.0.0.1
FAIL=0

chk() { local label="$1"; shift
        if "$@" >/dev/null 2>&1; then echo "OK      $label"; else echo "ERREUR  $label"; FAIL=1; fi; }
cnt() { docker exec cassandra1 cqlsh -e "SELECT count(*) FROM fleet.$1;" 2>/dev/null \
          | grep -E '^ +[0-9]+$' | tr -d ' '; }

t_nodes()   { [ "$(docker exec cassandra1 nodetool status | grep -c '^UN')" -eq 3 ]; }
t_count()   { [ "$(cnt "$1")" = "$2" ]; }
t_geohash() { [ "$(cnt vehicles_by_geohash)" -gt 0 ]; }
t_python()  { python -c 'from common.db import connect; c,s=connect(); s.execute("select release_version from system.local"); c.shutdown()'; }
t_http()    { [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ]; }
t_workers() { curl -s http://localhost:8090/json/ | python3 -c 'import sys,json; w=json.load(sys.stdin)["workers"]; sys.exit(0 if sum(x["state"]=="ALIVE" for x in w)==2 else 1)'; }
t_jars()    { docker exec spark-master sh -c 'ls /tmp/.ivy/jars | grep -q cassandra'; }
t_files()   { for f in "$@"; do [ -f "$f" ] || return 1; done; }
t_ram()     { [ "$(free -m | awk '/^Mem:/{print $7}')" -gt 1500 ]; }
t_venv()    { [ -n "$VIRTUAL_ENV" ]; }

echo "=== Docker et Cassandra ==="
chk "Docker répond"                                   docker ps
chk "3 nœuds Cassandra en UN"                         t_nodes
echo "=== Données ==="
chk "2000 chauffeurs"                                 t_count drivers 2000
chk "1500 véhicules"                                  t_count vehicles 1500
chk "1500 dernières positions"                        t_count last_position_by_vehicle 1500
chk "table géohash non expirée (A20, TTL de 30 min)"  t_geohash
chk "connexion Python (driver Cassandra)"             t_python
echo "=== Spark ==="
chk "interface Spark (port 8090)"                     t_http http://localhost:8090
chk "2 workers ALIVE"                                 t_workers
chk "connecteur en cache (pas de téléchargement)"     t_jars
echo "=== Fichiers et dashboard ==="
chk "mesures des 30 requêtes"                         t_files benchmarks/results/all_queries.csv benchmarks/results/consistency.csv
chk "résultats Spark S28, S29, S30"                   t_files spark-jobs/out/S28.json spark-jobs/out/S29.json spark-jobs/out/S30.json
chk "dashboard lancé (port 8501)"                     t_http http://localhost:8501
echo "=== Machine ==="
chk "au moins 1,5 Go de RAM disponible"               t_ram
chk "venv Python actif"                               t_venv

echo
if [ "$FAIL" -eq 0 ]; then echo "TOUT EST PRÊT"; else echo "À CORRIGER avant la démo (lignes ERREUR)"; fi
exit $FAIL