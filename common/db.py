"""Connexion Cassandra et chronomètre, partagés par A et B.

Variables d'environnement (ou fichier .env à la racine) :
  CASSANDRA_HOSTS        defaut 127.0.0.1  (liste séparée par des virgules)
  CASSANDRA_PORT         defaut 9042
  CASSANDRA_KEYSPACE     defaut fleet
  CASSANDRA_CL           defaut ONE        (ONE / QUORUM / ALL ...)
  CASSANDRA_SINGLE_HOST  1 = ne parler qu'aux hôtes listés (utile si les IP
                         internes de Docker ne sont pas joignables depuis WSL)
"""
import os
import time
from contextlib import contextmanager

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv est optionnel
    pass

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.policies import (DCAwareRoundRobinPolicy, TokenAwarePolicy,
                                WhiteListRoundRobinPolicy)

HOSTS = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")]
PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "fleet")
DEFAULT_CL = os.getenv("CASSANDRA_CL", "ONE")


def connect(consistency=None, keyspace=KEYSPACE, single_host=None, timeout=30):
    """Retourne (cluster, session). Le niveau de cohérence s'applique par défaut
    à toutes les requêtes de la session (modifiable par statement)."""
    cl = getattr(ConsistencyLevel, (consistency or DEFAULT_CL).upper())
    if single_host is None:
        single_host = os.getenv("CASSANDRA_SINGLE_HOST", "0") == "1"
    if single_host:
        policy = WhiteListRoundRobinPolicy(HOSTS)
    else:
        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="dc1"))
    profile = ExecutionProfile(load_balancing_policy=policy,
                               consistency_level=cl,
                               request_timeout=timeout)
    cluster = Cluster(HOSTS, port=PORT,
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect(keyspace) if keyspace else cluster.connect()
    return cluster, session


@contextmanager
def timed(label, results=None):
    """with timed("T4") as t: ...   ->   t["ms"] contient la durée en millisecondes."""
    rec = {"label": label, "ms": None}
    start = time.perf_counter()
    try:
        yield rec
    finally:
        rec["ms"] = (time.perf_counter() - start) * 1000
        if results is not None:
            results.append(rec)
