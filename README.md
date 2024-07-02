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

Voici une explication en Markdown de l'architecture :

---

## Vue d'ensemble de l'architecture

Cette architecture décrit le système de traitement des données et d'alerte impliquant des appareils IoT, ingestion de données, traitement en flux, alertes et analyses.

### Composants

1. **Appareils IoT**
    - Ceux-ci incluent des drones, des caméras et des capteurs qui collectent divers types de données.
    
2. **Couche d'ingestion**
    - Utilise **Kafka** pour ingérer des données des appareils IoT. Kafka est responsable de la gestion des flux de données en temps réel et de garantir le transfert fiable des données.
    
3. **Traitement en flux**
    - **Apache Spark** est utilisé pour le traitement en flux. Il traite les données en temps réel pour assurer des analyses et des actions rapides.
    
4. **Système d'alerte**
    - Intégré avec **Telegram** pour envoyer des alertes en temps réel basées sur les données traitées par le composant de traitement en flux.
    
5. **Datalake**
    - Un réservoir de stockage qui conserve une grande quantité de données brutes dans leur format natif jusqu'à ce qu'elles soient nécessaires pour un traitement ou une analyse ultérieure.
    
6. **Analytique**
    - Utilise **Pandas** pour la manipulation des données. Ce composant est crucial pour extraire des informations et des analyses prédictives à partir des données stockées.

### Flux de travail

- **Collecte de données** : Les appareils IoT capturent des données et les envoient à la couche d'ingestion.
- **Ingestion de données** : Kafka ingère les données des appareils IoT et les transmet à la couche de traitement.
- **Traitement en flux** : Apache Spark traite les flux de données entrants en temps réel.
- **Génération d'alertes** : Basé sur les données traitées, des alertes en temps réel sont générées et envoyées via Telegram.
- **Stockage des données** : Les données traitées sont stockées dans le datalake pour un stockage à long terme et une analyse ultérieure.
- **Analyse des données** : Les données dans le datalake sont analysées à l'aide de Pandas.

### Instructions pour lancer le code

Vous pouvez trouver les instructions pour lancer le code dans chaque dossier contenant des fichiers `build.sbt`. Ces dossiers comprennent tous les détails nécessaires de build et de configuration pour démarrer les composants respectifs.
