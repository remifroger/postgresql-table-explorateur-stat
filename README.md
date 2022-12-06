# Résumé statistique d'une table PostgreSQL

Génère un fichier Excel de croisements statistiques à partir de mesuresColumns, qualitativeColumns, crossColumns et whereGlobal

## Configuration

`mesuresColumns` : les mesures - variables quantitatives - pouvant être une expression SQL  
`qualitativeColumns` : les axes d'analyses sur lesquels les mesures seront calculées  
`crossColumns` : croisement supplémentaire (qualitativeColumns x crossColumns)  
`whereGlobal` : conditions s'appliquant au niveau global   

## Aide

```
python main.py --help
```

```
usage: main.py [-h] -s SCHEMA -t TABLE -c CROISEMENT -o FICHIER_SORTIE

Exploration statistique dune table PostgreSQL

options:
  -h, --help            show this help message and exit
  -s SCHEMA, --schema SCHEMA
                        Nom du schéma source
  -t TABLE, --table TABLE
                        Nom de la table source
  -c CROISEMENT, --croisement CROISEMENT
                        Type de croisement, par mesure ou par axe d'analyse (valeurs possibles : 'mesure' ou 'axe' ou 'par_annee_par_insee')
  -o FICHIER_SORTIE, --fichier_sortie FICHIER_SORTIE
                        Nom du fichier de sortie, automatiquement suffixé par le nom de la table et l'extension (XLSX)
```

## Exemple d'usage

```
python main.py -c par_annee_par_insee -s travail -t dv3f_d75_mutation -o stat_test
```