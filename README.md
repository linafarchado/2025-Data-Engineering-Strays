# Système de Détection d'Animaux Errants

## Présentation du Projet
Le Système de Détection d'Animaux Errants tire parti d'un réseau d'appareils IoT interconnectés pour identifier et localiser les animaux errants dans les zones urbaines et rurales. Cette technologie vise à assister les organisations de sauvetage d'animaux et la gestion urbaine pour garantir la sécurité et le bien-être des animaux. Notre système intègre une gamme de technologies de traitement de données en temps réel et des solutions de stockage scalable, résultant en une architecture réactive et fiable capable de fonctionner à grande échelle.

## Questions et Détails de l'Architecture

### Question 1 : Contraintes de Stockage des Données pour "Statistiques"
**What technical/business constraints should the data storage component of the program architecture meet to fulfill the requirement described by the customer in paragraph «Statistics» ? 
So what kind of component(s) (listed in the lecture) will the architecture need?**

Le composant de stockage des données doit gérer des volumes massifs de données provenant de millions d'appareils IoT tout en fournissant des capacités d'écriture et de lecture rapides. Il doit supporter :

- Un débit élevé pour les écritures de données fréquentes.
- Une architecture évolutive pour croître avec le nombre d'appareils.
- Une performance fiable pour une récupération et une analyse rapides des données.

**Composants Nécessaires :**

- **Base de données NoSQL (MongoDB, Consistent et Partition Tolerance)** : Pour un stockage évolutif sans schéma, capable de gérer une variété de types de données.

### Question 2 : Contraintes Commerciales pour "Alerte"
**What business constraint should the architecture meet to fulfill the requirement describe in the paragraph «Alert»? Which component to choose?**

L'architecture doit fournir une analyse en temps réel et des capacités de notification immédiate pour permettre une action rapide.

**Composant à Choisir :**

- **Système de Stream Processing** : Apache Spark pour analyser les flux de données en temps réel.
- **Système de Notifications** : Intégration avec un service de messagerie instantanée tel que Telegram pour diffuser les alertes. Le système doit pouvoir gérer 2 types de notifications : les normales et les urgentes (en cas d'animaux blessés)

## Composants de l'Architecture du Système

- **Appareils IoT** : Drones avec caméras, IA intégré (pour détecter les animaux et calculer les indices de blessure) et capteurs collectant des données.
- **Ingestion Layer** : Apache Kafka gérant le flux de données.
- **Stream Processing** : Apache Spark pour l'analyse des données en temps réel.
- **Système d'Alerte** : Telegram pour les notifications instantanées.
- **Stockage de Données** : MongoDB pour les besoins de stockage NoSQL.
- **Analytique** : Python, Pandas et TensorFlow pour la manipulation et l'analyse des données pour des statistiques à long terme.
