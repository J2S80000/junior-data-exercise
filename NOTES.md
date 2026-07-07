# NOTES

## Hypotheses Et Arbitrages
- Le code source du job Spark principal est `src/jobs/build_patient_identity.py`.
- Le pipeline lit des CSV locaux depuis `resources/` et ecrit un CSV consolide dans `output/`.
- L'identite patient est consolidee par `ipp_canonique` (arbitrage central pour deduplication).
- En cas de valeur ambigue ou absente (`sexe`, `opposition`), la logique privilegie une valeur explicite `unknown`/`null` plutot qu'une imputation heuristique.
- Sous Windows, l'export final s'appuie sur `toPandas().to_csv(...)` pour contourner les limitations d'ecriture Spark/Hadoop locale.

## Anomalies Detectees Et Traitement
- Encodage heterogene observe dans certaines valeurs texte (exemples: `OpposÃ©`, `RÃ©publique`, `Ã‰lodie`).
- Le traitement normalise `opposition` via `upper + trim + translate` pour capter des variantes (`oui`, `O`, `TRUE`, `1`, etc.).
- Les dates sont heterogenes et gerees via plusieurs formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, `yyyy/MM/dd`).
- Des champs adresse peuvent etre partiellement manquants (ex: code postal absent) et sont conserves tels quels dans `adresse_json`.
- Des valeurs vides ou textuelles `NULL` sont converties en `null` pour eviter les faux positifs analytiques.

## Avec Plus De Temps
- Ajouter des tests automatiques (unitaires + integration) sur les fonctions de normalisation et de consolidation.
- Mettre en place un rapport de qualite de donnees (taux de nulls, invalides, conflits d'IPP, distributions des formats dates).
- Renforcer la gestion d'encodage en entree (detection/standardisation UTF-8) et tracer les enregistrements corriges.
- Produire un schema cible explicite (types Spark imposes) pour reduire les conversions implicites.
- Ajouter un mode de sortie Parquet/Delta pour usages analytiques et meilleures performances.
