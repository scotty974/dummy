# Project

Une pipeline d'intégration qui récupére des données produits et des commandes depuis une API REST, les charges dans POSTGRESQL, les relie via une donnée pivot (l'id du produit), controle leur qualité et publie une table métier agregee, le tout orchestrer par Airflow et re-executable.