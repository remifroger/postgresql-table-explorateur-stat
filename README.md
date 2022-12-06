# Résumé statistique d'une table PostgreSQL

Génère un fichier Excel de croisements statistiques à partir de mesuresColumns, qualitativeColumns et crossColumns

## Configuration

mesuresColumns : les mesures - variables quantitatives - pouvant être une expression SQL
qualitativeColumns : les axes d'analyses sur lesquels les mesures seront calculées
crossColumns : croisement supplémentaire (qualitativeColumns x crossColumns)