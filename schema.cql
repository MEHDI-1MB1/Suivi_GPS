-- =====================================================================
-- Télématique de flotte - Schéma Cassandra 4.1 (conçu à partir du workload)
--
-- Chargement (profil full, RF=3) :
--   docker exec -i cassandra1 cqlsh < schema.cql
-- Chargement (profil dev, 1 noeud, RF=1) :
--   sed "s/'dc1': 3/'dc1': 1/" schema.cql | docker exec -i cassandra1 cqlsh
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS fleet
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
  AND durable_writes = true;

USE fleet;

-- ---------------------------------------------------------------------
-- Types définis par l'utilisateur (UDT)
-- ---------------------------------------------------------------------
CREATE TYPE IF NOT EXISTS geo_point (
  lat double,
  lon double
);

CREATE TYPE IF NOT EXISTS place (
  name    text,
  address text,
  lat     double,
  lon     double,
  geohash text
);

CREATE TYPE IF NOT EXISTS part_item (
  name      text,
  reference text,
  qty       int,
  unit_cost decimal
);

-- ---------------------------------------------------------------------
-- Référentiels : dépôts, véhicules, chauffeurs
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS depots (
  depot_id text PRIMARY KEY,
  name     text,
  city     text,
  location frozen<geo_point>
);

-- T3 (fiche véhicule), T12 (ajout équipement : equipment = equipment + {...})
-- A24 : last_maintenance_date mis à jour par T11 (scan de 1 500 lignes)
CREATE TABLE IF NOT EXISTS vehicles (
  plate                 text PRIMARY KEY,
  vehicle_type          text,
  brand                 text,
  model                 text,
  year                  int,
  capacity_kg           int,
  depot_id              text,
  equipment             set<text>,
  features              map<text, text>,
  last_maintenance_date date
);

-- Table dédiée "véhicules d'un dépôt" (comparée à un index secondaire sur vehicles.depot_id)
CREATE TABLE IF NOT EXISTS vehicles_by_depot (
  depot_id     text,
  plate        text,
  vehicle_type text,
  model        text,
  PRIMARY KEY ((depot_id), plate)
);

CREATE TABLE IF NOT EXISTS drivers (
  driver_id           text PRIMARY KEY,
  name                text,
  cin                 text,
  phone               text,
  license_categories  list<text>,
  hire_date           date,
  depot_id            text
);

-- ---------------------------------------------------------------------
-- Positions GPS
-- T1, T2 : écriture ; T5 : trace d'un jour ; T13 : TTL 60 jours ;
-- T14 : DELETE par plage (plate, day, ts)
-- Partition = véhicule + jour (~2 880 lignes/jour à 30 s), tri par temps décroissant
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS positions_by_vehicle_day (
  plate        text,
  day          date,
  ts           timestamp,
  lat          double,
  lon          double,
  speed_kmh    double,
  heading      int,
  fuel_pct     double,
  engine_state text,          -- ON / OFF / IDLE
  PRIMARY KEY ((plate, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND default_time_to_live = 5184000   -- 60 jours (T13)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
  };

-- T4 (dernière position, lecture O(1)) et A21 (véhicules sans position depuis 1 h)
-- Mise à jour à chaque T1 (même batch que l'insertion de la position)
CREATE TABLE IF NOT EXISTS last_position_by_vehicle (
  plate        text PRIMARY KEY,
  ts           timestamp,
  lat          double,
  lon          double,
  speed_kmh    double,
  fuel_pct     double,
  engine_state text,
  geohash      text
);

-- A20 : véhicules dans un rayon de 5 km.
-- Cellule geohash précision 5 (~4,9 km) : on interroge la cellule + ses 8 voisines,
-- puis on filtre avec Haversine côté client. TTL court : une position non rafraîchie disparaît.
CREATE TABLE IF NOT EXISTS vehicles_by_geohash (
  geohash text,
  plate   text,
  ts      timestamp,
  lat     double,
  lon     double,
  PRIMARY KEY ((geohash), plate)
) WITH default_time_to_live = 1800;

-- ---------------------------------------------------------------------
-- Missions
-- ---------------------------------------------------------------------
-- T6 (création), T7 (démarrage / fin), T8 (affectation chauffeur)
-- Source pour Spark S28 (jointure missions x positions)
CREATE TABLE IF NOT EXISTS missions (
  mission_id    uuid PRIMARY KEY,
  plate         text,
  driver_id     text,
  depot_id      text,
  origin        frozen<place>,
  destination   frozen<place>,
  planned_start timestamp,
  planned_end   timestamp,
  actual_start  timestamp,
  actual_end    timestamp,
  distance_km   double,
  status        text          -- PLANNED / IN_PROGRESS / DONE / CANCELLED
);

-- T10 : missions d'un chauffeur sur une semaine (week_start = lundi)
CREATE TABLE IF NOT EXISTS missions_by_driver_week (
  driver_id     text,
  week_start    date,
  planned_start timestamp,
  mission_id    uuid,
  plate         text,
  status        text,
  distance_km   double,
  PRIMARY KEY ((driver_id, week_start), planned_start, mission_id)
) WITH CLUSTERING ORDER BY (planned_start ASC, mission_id ASC);

-- T15 : missions en cours d'un dépôt (insert au démarrage, delete à la fin)
CREATE TABLE IF NOT EXISTS active_missions_by_depot (
  depot_id     text,
  mission_id   uuid,
  plate        text,
  driver_id    text,
  actual_start timestamp,
  PRIMARY KEY ((depot_id), mission_id)
);

-- ---------------------------------------------------------------------
-- Événements de conduite
-- T9 : écriture ; source de S27 (score de conduite)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_by_driver_day (
  driver_id  text,
  day        date,
  ts         timestamp,
  event_id   uuid,
  plate      text,
  event_type text,            -- SPEEDING / HARSH_BRAKING / LONG_IDLE / GEOFENCE_EXIT
  value      double,
  position   frozen<geo_point>,
  PRIMARY KEY ((driver_id, day), ts, event_id)
) WITH CLUSTERING ORDER BY (ts DESC, event_id ASC);

-- ---------------------------------------------------------------------
-- Maintenances
-- T11 : écriture (+ mise à jour de vehicles.last_maintenance_date)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS maintenances_by_vehicle (
  plate          text,
  maint_date     date,
  maint_id       uuid,
  odometer_km    int,
  maint_type     text,
  parts          list<frozen<part_item>>,
  cost           decimal,
  PRIMARY KEY ((plate), maint_date, maint_id)
) WITH CLUSTERING ORDER BY (maint_date DESC, maint_id ASC);

-- ---------------------------------------------------------------------
-- Tables d'agrégats (compteurs) : analytiques natives
-- Alimentées au moment des écritures (T1, T7, T9). Pas de TTL possible sur les compteurs.
-- ---------------------------------------------------------------------

-- A16 : missions et distance par jour (30 lectures de partitions)
CREATE TABLE IF NOT EXISTS mission_stats_by_day (
  day            date PRIMARY KEY,
  missions_count counter,
  distance_m     counter
);

-- A18 (taux de retard) et A25 (durée moyenne) par dépôt
CREATE TABLE IF NOT EXISTS mission_stats_by_depot (
  depot_id         text PRIMARY KEY,
  missions_done    counter,
  missions_late    counter,
  total_duration_s counter
);

-- A17 : excès de vitesse par chauffeur (scan de 2 000 partitions, tri côté client)
CREATE TABLE IF NOT EXISTS event_counts_by_driver (
  driver_id  text,
  event_type text,
  cnt        counter,
  PRIMARY KEY ((driver_id), event_type)
);

-- A22 : événements par type et par jour
CREATE TABLE IF NOT EXISTS event_counts_by_day (
  day        date,
  event_type text,
  cnt        counter,
  PRIMARY KEY ((day), event_type)
);

-- A19 : vitesse moyenne par type de véhicule (somme x10 pour rester en entiers)
CREATE TABLE IF NOT EXISTS speed_agg_by_type (
  vehicle_type text PRIMARY KEY,
  speed_sum_x10 counter,
  samples       counter
);

-- A23 : carburant moyen par modèle
CREATE TABLE IF NOT EXISTS fuel_agg_by_model (
  model         text PRIMARY KEY,
  fuel_sum_x10  counter,
  samples       counter
);

-- ---------------------------------------------------------------------
-- Index secondaire (pour la comparaison "effet de l'indexation")
-- Comparer : SELECT ... FROM vehicles WHERE depot_id = ?   (index)
--       vs   SELECT ... FROM vehicles_by_depot WHERE depot_id = ?  (table dédiée)
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS vehicles_depot_idx ON vehicles (depot_id);
