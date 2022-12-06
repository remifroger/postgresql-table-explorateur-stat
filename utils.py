import os
import pandas as pd

# Fonction de sauvegarde d'une liste de DataFrame dans un Excel avec écriture dans la même feuille Excel si le nom de feuille est identique
def save_excel_sheet(df, filepath, sheetname, startrow, index=False):
    if not os.path.exists(filepath):
        df.to_excel(filepath, sheet_name=sheetname, index=False)
    else:
        with pd.ExcelWriter(filepath, engine = 'openpyxl', if_sheet_exists = 'overlay', mode= 'a') as writer:
            df.to_excel(writer, sheet_name = sheetname, startrow = startrow, index = False)