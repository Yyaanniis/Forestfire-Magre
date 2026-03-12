import geopandas as gpd
import rasterio
import rasterio.mask
import pandas as pd
import numpy as np
import pyodbc
import os

# =============================================================================
# 📚 ÉTAPE 1 : EXTRAIRE LES DONNÉES DE LA BASE MDB
# =============================================================================
print("="*70)
print("📚 EXTRACTION DES ATTRIBUTS DEPUIS HWSD2.mdb (LAYER D1)")
print("="*70)

mdb_path = "HWSD2.mdb"
csv_path = "HWSD2_LAYERS.csv"

# Vérifier si le CSV existe déjà
if os.path.exists(csv_path):
    print(f"✅ CSV existant trouvé : {csv_path}")
    soil_attributes = pd.read_csv(csv_path)
else:
    print(f"🔄 Extraction depuis {mdb_path}...")
    
    if not os.path.exists(mdb_path):
        print(f"❌ Fichier {mdb_path} introuvable")
        exit()
    
    try:
        conn_str = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            f'DBQ={os.path.abspath(mdb_path)};'
        )
        conn = pyodbc.connect(conn_str)
        
        query = "SELECT * FROM HWSD2_LAYERS"
        soil_attributes = pd.read_sql(query, conn)
        conn.close()
        
        soil_attributes.to_csv(csv_path, index=False)
        print(f"✅ Extraction réussie : {len(soil_attributes)} lignes")
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
        exit()

# ✅ FILTRE : Garder uniquement la couche D1 (0-20cm)
if "LAYER" in soil_attributes.columns:
    soil_attributes = soil_attributes[soil_attributes["LAYER"] == "D1"]
    print(f"✅ Filtrage LAYER='D1' : {len(soil_attributes)} enregistrements")
else:
    print("⚠️  Colonne 'LAYER' introuvable")

# ✅ SELECTION : Garder uniquement les attributs demandés
attributes_required = [
    'HWSD2_SMU_ID',  # ID obligatoire
    'COARSE', 'SAND', 'SILT', 'CLAY', 
    'TEXTURE_USDA', 'TEXTURE_SOTER', 
    'BULK', 'REF_BULK', 'ORG_CARBON', 
    'PH_WATER', 'TOTAL_N', 'CN_RATIO', 
    'CEC_SOIL', 'CEC_CLAY', 'CEC_EFF', 
    'TEB', 'BSAT', 'ALUM_SAT', 'ESP', 
    'TCARBON_EQ', 'GYPSUM', 'ELEC_COND'
]

# Vérifier quels attributs existent
attributes_available = [col for col in attributes_required if col in soil_attributes.columns]
print(f"✅ Attributs disponibles : {len(attributes_available)}/{len(attributes_required)}")

# Garder uniquement ces colonnes
soil_attributes = soil_attributes[attributes_available].copy()

# =============================================================================
# 🌍 ÉTAPE 2 : DÉCOUPER LE RASTER PAR PAYS
# =============================================================================
print("\n" + "="*70)
print("🌍 DÉCOUPAGE DU RASTER HWSD2 - ALGÉRIE & TUNISIE")
print("="*70)

print("\n📍 Chargement des frontières...")
world = gpd.read_file("ne_110m_admin_0_countries.shp")
countries = world[world["NAME"].isin(["Algeria", "Tunisia"])]

print("🗺️  Chargement et découpage du raster HWSD2...")
raster_path = "data/HWSD2.bil"

with rasterio.open(raster_path) as src:
    countries = countries.to_crs(src.crs)
    geometries = countries.geometry.values
    out_image, out_transform = rasterio.mask.mask(src, geometries, crop=True)
    nodata = src.nodata if src.nodata is not None else 0

raster_ids = out_image[0]
print(f"✅ Raster découpé : {raster_ids.shape[0]} x {raster_ids.shape[1]} pixels")

# =============================================================================
# 🗺️  ÉTAPE 3 : EXTRACTION BRUTE (SANS AGRÉGATION)
# =============================================================================
print("\n🔄 Extraction BRUTE (sans agrégation, sans moyenne)...")

# ⚠️ CRUCIAL : Créer un dictionnaire qui CONSERVE tous les enregistrements
# Structure : {HWSD2_SMU_ID: [liste de tous les enregistrements]}
soil_dict = {}
for _, row in soil_attributes.iterrows():
    soil_id = row['HWSD2_SMU_ID']
    if soil_id not in soil_dict:
        soil_dict[soil_id] = []
    soil_dict[soil_id].append(row.to_dict())

print(f"✅ Dictionnaire créé : {len(soil_dict)} IDs uniques")

# Statistiques sur les doublons
single_records = sum(1 for records in soil_dict.values() if len(records) == 1)
multi_records = sum(1 for records in soil_dict.values() if len(records) > 1)
max_records = max(len(records) for records in soil_dict.values())

print(f"   ℹ️  {single_records} IDs avec 1 seul enregistrement")
print(f"   ℹ️  {multi_records} IDs avec plusieurs enregistrements (max: {max_records})")
print(f"   ⚠️  TOUS les enregistrements seront conservés (pas de moyenne)")

# =============================================================================
# 🗺️  ÉTAPE 4 : CRÉER UNE LIGNE PAR PIXEL × ENREGISTREMENT
# =============================================================================
print("\n📊 Création du tableau (1 ligne par pixel × enregistrement)...")

data_rows = []
rows, cols = raster_ids.shape
total_pixels = rows * cols
valid_pixels = 0

for row in range(rows):
    for col in range(cols):
        soil_id = raster_ids[row, col]
        
        # Ignorer NoData ou 0
        if soil_id == nodata or soil_id == 0 or np.isnan(soil_id):
            continue
        
        # Coordonnées géographiques
        x, y = rasterio.transform.xy(out_transform, row, col, offset='center')
        
        # ⚠️ SI l'ID a plusieurs enregistrements → PLUSIEURS lignes
        if soil_id in soil_dict:
            for record in soil_dict[soil_id]:
                pixel_data = {
                    'X': round(x, 6),
                    'Y': round(y, 6)
                }
                # Ajouter TOUS les attributs BRUTS (avec NaN si manquants)
                pixel_data.update(record)
                data_rows.append(pixel_data)
            valid_pixels += 1
    
    # Progression
    if (row + 1) % 100 == 0:
        progress = (row + 1) / rows * 100
        print(f"   Progression : {progress:.1f}% ({valid_pixels:,} pixels)", end='\r')

print(f"\n✅ {len(data_rows):,} lignes créées ({valid_pixels:,} pixels uniques)")

# =============================================================================
# 💾 ÉTAPE 5 : CRÉER LE DATAFRAME
# =============================================================================
print("\n📋 Création du DataFrame final...")

df_final = pd.DataFrame(data_rows)

# Réorganiser : X, Y, HWSD2_SMU_ID en premier
cols_order = ['X', 'Y', 'HWSD2_SMU_ID'] + [c for c in df_final.columns if c not in ['X', 'Y', 'HWSD2_SMU_ID']]
df_final = df_final[cols_order]

print(f"✅ DataFrame créé : {len(df_final):,} lignes × {len(df_final.columns)} colonnes")

# Aperçu
print("\n📊 Aperçu des 15 premières lignes :")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
print(df_final.head(15))

# Statistiques détaillées
print("\n📈 Statistiques :")
print(f"   - Total de lignes : {len(df_final):,}")
print(f"   - Pixels uniques (X,Y) : {df_final[['X','Y']].drop_duplicates().shape[0]:,}")
print(f"   - IDs de sol distincts : {df_final['HWSD2_SMU_ID'].nunique()}")

# Vérifier les doublons de coordonnées
coord_duplicates = df_final.duplicated(subset=['X', 'Y']).sum()
if coord_duplicates > 0:
    print(f"   ℹ️  {coord_duplicates:,} lignes avec coordonnées dupliquées")
    print(f"      (= pixels avec plusieurs composants de sol)")
else:
    print(f"   ✅ Aucun doublon de coordonnées")

# Valeurs manquantes
print(f"\n📊 Valeurs manquantes par colonne :")
for col in df_final.columns:
    if col not in ['X', 'Y']:
        missing = df_final[col].isna().sum()
        if missing > 0:
            pct = missing / len(df_final) * 100
            print(f"      • {col:15s}: {missing:7,} ({pct:5.1f}%)")

# =============================================================================
# 💾 ÉTAPE 6 : EXPORT CSV
# =============================================================================
print("\n💾 Export des données BRUTES...")

os.makedirs("output", exist_ok=True)

csv_output = "output/HWSD2_Algeria_Tunisia_D1_RAW.csv"
print(f"   📊 Création du fichier CSV...")

# Export BRUT (toutes les valeurs, même NaN)
df_final.to_csv(csv_output, index=False, sep=';', decimal=',', encoding='utf-8-sig')

csv_size = os.path.getsize(csv_output) / (1024**2)
print(f"✅ CSV créé : {csv_output} ({csv_size:.2f} MB)")

# =============================================================================
# 🎉 RÉSUMÉ
# =============================================================================
print("\n" + "="*70)
print("🎉 EXTRACTION BRUTE TERMINÉE (LAYER D1)")
print("="*70)
print(f"\n📁 Fichier : {csv_output}")
print(f"\n📊 Contenu :")
print(f"   - {len(df_final):,} lignes (BRUTES, sans agrégation)")
print(f"   - {len(df_final.columns)} colonnes")
print(f"   - Couche : D1 (0-20cm de profondeur)")
print(f"   - Valeurs manquantes : CONSERVÉES")
print(f"   - Doublons : CONSERVÉS (composants multiples)")
print(f"\n💡 Pour ouvrir dans Excel :")
print(f"   Données → Importer → CSV → Séparateur : point-virgule (;)")
print("="*70)