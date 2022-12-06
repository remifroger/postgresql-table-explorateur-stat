#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# Python 2.7.X

"""
Présentation et logique du programme
------------------------------------
Générer un fichier Excel de croisements statistiques à partir de mesuresColumns, qualitativeColumns et crossColumns
mesuresColumns : les mesures - variables quantitatives - pouvant être une expression SQL
qualitativeColumns : les axes d'analyses sur lesquels les mesures seront calculées
crossColumns : croisement supplémentaire (qualitativeColumns x crossColumns)
whereGlobal : conditions s'appliquant au niveau global 
"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from ast import literal_eval
import argparse
import json
import utils
load_dotenv()

PGBINPATH = os.getenv('PGBINPATH')
PGHOST = os.getenv('PGPRODHOST')
PGDB = os.getenv('PGPRODDB')
PGUSER = os.getenv('PGPRODUSER')
PGPORT = os.getenv('PGPRODPORT')
PGPASSWORD = os.getenv('PGPRODPASSWORD')

parser = argparse.ArgumentParser(description = 'Exploration statistique d''une table PostgreSQL')
parser.add_argument("-s", "--schema", required = True, help = "Nom du schéma source")
parser.add_argument("-t", "--table", required = True, help = "Nom de la table source")
parser.add_argument("-c", "--croisement", required = True, help = "Type de croisement, par mesure ou par axe d'analyse (valeurs possibles : 'mesure' ou 'axe')")
parser.add_argument("-o", "--fichier_sortie", required = True, help = "Nom du fichier de sortie, automatiquement suffixé par le nom de la table et l'extension (XLSX)")
args = parser.parse_args()

CROISEMENT = args.croisement
schemaName = args.schema
tableName = args.table
output = args.fichier_sortie

# Liste des mesures
# La propriété expr peut être de type array pour générer une opération particulière (division, addition, etc.), et doit être énuméré dans l'ordre (exemple : ["avg(CAST(NULLIF(valeurfonc, '') as numeric))", "/", "avg(CAST(NULLIF(sbati, '') as numeric))"])
mesuresColJson = open('config/mesuresColumns.json')
mesuresColumns = json.load(mesuresColJson)


# Liste des variables qualitatives
qualitativeColJson = open('config/qualitativeColumns.json')
qualitativeColumns = json.load(qualitativeColJson)

# Liste des variables qualitatives pour croiser (qualitativeColumns x crossColumns)
crossColJson = open('config/crossColumns.json')
crossColumns = json.load(crossColJson)

# Where : conditions s'appliquant au niveau global (et pas au niveau ligne)
whereGlobalJson = open('config/globalWhere.json')
whereGlobal = json.load(whereGlobalJson)

whereText = ''
for i, where in enumerate(whereGlobal):
    if len(whereGlobal) > 1:
        print('config/globalWhere.json ne peut contenir qu''un objet')
        exit()
    else:
        whereText += where["expr"]

engine = create_engine('postgresql+psycopg2://{0}:{1}\@{3}:{4}/{2}'.format(PGUSER, PGPASSWORD, PGDB, PGHOST, PGPORT))
print(engine)

pd.options.display.float_format = '{:.2f}'.format
dataResult = {}
dataResultTot = {}
listTot = []

if CROISEMENT == 'axe':
    # Expression SQL des mesures à partir de mesuresColumns
    textMesures = ''
    textMesuresTot = ''
    for i, mes in enumerate(mesuresColumns):
        if i < len(mesuresColumns) - 1:
            textMesures += "{0} as {1}, ".format(mes['expr'], mes['col'])
            textMesuresTot += "sum({0}::numeric) as {0}, ".format(mes['col'])
        else:
            textMesures += "{0} as {1}".format(mes['expr'], mes['col'])
            textMesuresTot += "sum({0}::numeric) as {0}".format(mes['col'])
    if len(crossColumns) > 0:
        for i, q in enumerate(qualitativeColumns):
            query = pd.read_sql("WITH sum AS (SELECT {1} FROM {2}.{0} GROUP BY {1}) SELECT * FROM sum".format(tableName, q['col'], schemaName), engine) 
            col_name = q['col']
            distinctValues = query[col_name].tolist()
            for d, dist in enumerate(distinctValues):
                if isinstance(dist, str):
                    if dist.startswith('['):
                        # Gestion des arrays (clause WHERE 'val' = ANY(col))
                        dist = np.array(literal_eval(dist))
                        dist = dist[0]
                        for c, cross in enumerate(crossColumns):
                            if q['col'] != cross['col']: # On ne croise pas des colonnes identiques
                                dataResult[q['col'] + str(c) + str(dist)[:15]] = pd.read_sql("WITH sum AS (SELECT {5}, '{6}' as variable, {0} FROM {3}.{1} WHERE '{6}' = any(replace(replace({2}, '[', '{{'), ']', '}}')::text[]) GROUP BY {5}) SELECT * FROM sum UNION SELECT '_Totaux', '' as variable, {4} FROM sum ORDER BY {5} DESC".format(textMesures, tableName, q['col'], schemaName, textMesuresTot, cross['col'], dist.replace("'", "''")), engine)
                    else:
                        # Sinon texte (clause WHERE col = 'val')
                        for c, cross in enumerate(crossColumns):
                            if q['col'] != cross['col']: # On ne croise pas des colonnes identiques
                                dataResult[q['col'] + str(c) + str(dist)[:15]] = pd.read_sql("WITH sum AS (SELECT {5}, '{6}' as variable, {0} FROM {3}.{1} WHERE {2}::text = '{6}'::text GROUP BY {5}) SELECT * FROM sum UNION SELECT '_Totaux', '' as variable, {4} FROM sum ORDER BY {5} DESC".format(textMesures, tableName, q['col'], schemaName, textMesuresTot, cross['col'], dist.replace("'", "''")), engine)
                
        if len(dataResult) > 0:
            with pd.ExcelWriter('{1}_{0}.xlsx'.format(tableName, output)) as writer:
                for i, g in  enumerate(dataResult):
                    dataResult[g].to_excel(writer, sheet_name=g, index=False)

    else:
        for i, q in enumerate(qualitativeColumns):
            dataResult[q['col']] = pd.read_sql("WITH sum AS (SELECT {2}, {0} FROM {3}.{1} WHERE {5} GROUP BY {2}) SELECT * FROM sum UNION SELECT '_Totaux', {4} FROM sum ORDER BY {2} DESC".format(textMesures, tableName, q['col'], schemaName, textMesuresTot, whereText), engine)

        for i, q in enumerate(qualitativeColumns):
            query = pd.read_sql("WITH sum AS (SELECT {2}, {0} FROM {3}.{1} WHERE {5} GROUP BY {2}) SELECT '{2}' as variable, {4} FROM sum".format(textMesures, tableName, q['col'], schemaName, textMesuresTot, whereText), engine)
            dataResultTot[q['col']] = query
            listTot.append(pd.DataFrame(query))

        if len(dataResult) > 0:
            with pd.ExcelWriter('{1}_{0}.xlsx'.format(tableName, output)) as writer:
                for i, g in  enumerate(dataResult):
                    dataResult[g].to_excel(writer, sheet_name=g, index=False)

        if len(listTot) > 0:
            with pd.ExcelWriter('{1}_{0}.xlsx'.format(tableName, output)) as writer:
                row = 0
                for dataframe in listTot:
                    dataframe.to_excel(writer, sheet_name='Résumé totaux', startrow=row, startcol=0)   
                    row = row + len(dataframe.index) + 2

elif CROISEMENT == 'mesure':
    print('En cours de développement')
         
elif CROISEMENT == 'par_annee_par_insee':
    # Ajout d'une méthode spécifique pour générer, à partir de qualitativeColumns et mesuresColumns, un fichier Excel avec une feuille par variables qualitatives, puis un tableau par mesure dans chaque feuille
    # Pour chaque tableau, on groupe par annee_column, et on filtre chaque mesure selon insee_column (donnant pour chaque tableau, les valeurs par année - en lignes - et par code Insee - en colonnes)
    # crossColumns est ignoré
    insee_column = 'l_codinsee'
    annee_column = 'anneemut'
    # Récupération des valeurs Insee distinctes - on ajoute une valeur TOT pour générer un total par la suite
    query = pd.read_sql("with a as (select replace(replace({0}, '[', '{{'), ']', '}}')::varchar[] as {0} from {2}.{1}), union_ as (select {0}[1] as {0} from a group by {0}[1] union select 'TOT' as {0}) select * from union_ order by {0}".format(insee_column, tableName, schemaName), engine)
    distinctInsee = query[insee_column]
    # Boucle sur les variables qualitatives
    for a, q in enumerate(qualitativeColumns):
        # Initialisation d'un dictionnaire avec comme porte d'entrée l'alias de la colonne qualitative
        dataResult[q['alias']] = {}
        # Boucle sur les mesures
        for i, mes in enumerate(mesuresColumns):
            textMesuresSpec = ''
            textMesuresTotSpec = ''
            # Puis pour chaque valeur Insee
            for u, insee in enumerate(distinctInsee):
                # Si la mesure est de type array
                if hasattr(mes['expr'], '__len__') and (not isinstance(mes['expr'], str)):
                    mesureFormat = ''
                    # Boucle sur l'array pour générer l'expression en ajoutant la clause filter à chaque fois
                    for k, elMes in enumerate(mes['expr']):
                        # Détection des opérateurs arithmétiques 
                        if elMes not in ['/', '-', '*', '+']:
                            # Distinction entre TOT et le reste (si TOT : pas de filtre, sinon : filtre)
                            # Puis écriture de l'expression
                            if insee == 'TOT':
                                mesureFormat += "nullif(" + elMes + ", 0)"
                            else:
                                mesureFormat += "nullif(" + elMes + " filter (where '{0}' = any(replace(replace({1}, '[', '{{'), ']', '}}')::text[])), 0)".format(insee, insee_column)
                        else:
                            mesureFormat += elMes
                # Si la mesure est de type texte, on applique l'expression 
                else:
                    # Distinction entre TOT et le reste (si TOT : pas de filtre, sinon : filtre)
                    if insee == 'TOT':
                        mesureFormat = mes['expr']
                    else:
                        mesureFormat = mes['expr'] + " filter (where '{0}' = any(replace(replace({1}, '[', '{{'), ']', '}}')::text[]))".format(insee, insee_column)
                # Ecriture finale de l'expression SQL des mesures
                # Pour la dernière valeur de distinctInsee, on ne met pas de virgule
                if u < len(distinctInsee) - 1:
                    textMesuresSpec += "{4} as {1}_{2}, ".format(mes['expr'], mes['col'], insee, insee_column, mesureFormat)
                    textMesuresTotSpec += "sum({0}_{1}::numeric) as {0}_{1}, ".format(mes['col'], insee, insee_column)
                else:
                    textMesuresSpec += "{4} as {1}_{2}".format(mes['expr'], mes['col'], insee, insee_column, mesureFormat)
                    textMesuresTotSpec += "sum({0}_{1}::numeric) as {0}_{1}".format(mes['col'], insee, insee_column) 
            # Ajout dans le dictionnaire dataResult[q['alias']] du résultat de la requête dans le nom de la mesure (mes['col'])
            dataResult[q['alias']][mes['col']] = []
            dataResult[q['alias']][mes['col']].append(pd.read_sql("WITH sum AS (SELECT {5}, {2}, array_agg(distinct {6}) as {6}, {0} FROM {3}.{1} WHERE {7} GROUP BY {5}, {2}) SELECT * FROM sum ORDER BY {5} ASC".format(textMesuresSpec, tableName, q['col'], schemaName, textMesuresTotSpec, annee_column, q['desc'], whereText), engine))

    # Si dataResult n'est pas vide
    if len(dataResult) > 0:
        # Chemin de sauvegarde du Excel
        path = '{1}_{0}.xlsx'.format(tableName, output)
        if os.path.exists(path):
            os.remove(path)
        # Pour chaque variable qualitative de dataResult
        for i, g in enumerate(dataResult):
            # Pour chaque mesure de chaque variable qualitative
            for k, key in enumerate(dataResult[g]):
                # Récupération du DataFrame
                data = dataResult[g].get(key)
                dataDf = data[0]
                if k == 0:
                    startrow = 0
                else:
                    startrow = startrow + dataDf.shape[0] + 2
                # On sauvegarde en incrémentant startrow
                # Un fichier Excel, dont chaque feuille = une variable qualitative, et chaque tableau d'une feuille = une mesure
                utils.save_excel_sheet(dataDf, path, g, startrow)
