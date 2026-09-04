## Synthèse de la Peer-Review

Nous avons réalisé une revue croisée du dépôt `multi_container_data_lake_engine`.

Globalement, le projet présente une **bonne architecture Data Engineering**, avec une stack cohérente : MongoDB, Spark, MinIO, Airflow et Docker.

### Points forts

Premier point fort : **l’architecture est claire et bien séparée**.  
Les différentes étapes du pipeline sont bien organisées entre extraction, stockage, traitement Spark et orchestration.

Deuxième point fort : **la conteneurisation est bien pensée**.  
Docker Compose permet de lancer plusieurs services, et des `healthchecks` sont présents pour MongoDB, MinIO et Spark.

Troisième point fort : **le traitement des données est assez robuste**.  
Le projet gère plusieurs formats de données, les doublons, les dates et certaines erreurs liées aux API externes.

Enfin, l’utilisation d’Airflow permet de structurer les différentes étapes du pipeline et d’avoir une orchestration claire.

### Axes d’amélioration

Le premier point important concerne **la jointure entre les données Vélib et météo**.

La jointure est réalisée sur la station et sur l’heure. Comme plusieurs observations peuvent exister dans une même heure, cela peut créer une jointure `many-to-many` et donc dupliquer certaines lignes.

Il serait préférable d’agréger les données météo par heure avant la jointure.

Deuxième point : **la gestion des secrets**.

Certains identifiants MinIO sont directement écrits dans le code. Il serait préférable d’utiliser uniquement les variables d’environnement dans le fichier `.env`.

Troisième point : **la reproductibilité du projet**.

Le fichier de référence des stations utilisé par Spark n’est pas présent dans le dépôt, et le bucket MinIO `gold-zone` n’est pas créé automatiquement.

Une nouvelle personne qui clone le projet peut donc rencontrer des erreurs au démarrage.

Quatrième point : **la performance Spark**.

Dans certains jobs, on trouve `coalesce(1)`, qui oblige Spark à écrire les données avec une seule partition. Cela limite fortement l’intérêt du traitement distribué.

Enfin, il manque actuellement **des tests automatisés** permettant de vérifier les transformations et les règles de qualité des données.

### Conclusion

En conclusion, le projet possède une bonne base technique et une architecture cohérente.

Les améliorations prioritaires seraient donc :

1. corriger la jointure Vélib-météo ;
2. sécuriser les identifiants avec les variables d’environnement ;
3. rendre le projet totalement reproductible ;
4. améliorer la scalabilité Spark ;
5. ajouter des tests automatisés.

Ce sont principalement des améliorations de robustesse et de qualité, plutôt qu’une remise en cause de l’architecture générale du projet.