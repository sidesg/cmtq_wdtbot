#!/usr/local/bin/python3

from pathlib import Path
import pandas as pd

QCPRODS = Path.cwd() / "donnees_source" / "oeuvres_qc.xlsx"
WDTQIDMAPPING = Path.cwd().parent / "wdt_cmtqId.csv"

qcproddf = pd.read_excel(QCPRODS)
qcproddf.rename(columns={'FilmoID': "FilmoId"}, inplace=True)
print(len(qcproddf))

#Charger le mapping FilmoID-WdtURI
wdt_cmtqId = pd.read_csv(WDTQIDMAPPING)
wdt_cmtqId = wdt_cmtqId[wdt_cmtqId["FilmoId"].str.isnumeric()]
wdt_cmtqId = wdt_cmtqId.astype({"FilmoId": "int64"})

qcprod_absent = qcproddf[~qcproddf["FilmoId"].isin(wdt_cmtqId["FilmoId"])]


qcprod_mappe = pd.merge(wdt_cmtqId, qcproddf, on="FilmoId")
qcprod_mappe = qcprod_mappe.sort_values(by="FilmoId")

qcprod_absent.to_csv("rapports/qcprods_absent.csv", index=False)
qcprod_mappe.to_csv("rapports/qcprods_mappes.csv", index=False)

print(len(qcprod_absent))
print(qcprod_absent.head())