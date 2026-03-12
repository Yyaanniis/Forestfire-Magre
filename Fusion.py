import pandas as pd
import numpy as np
from scipy.spatial import cKDTree

print("="*70)
print("JOINTURE SPATIALE OBJECTIF 95% DE COUVERTURE")
print("="*70)

fire = pd.read_csv(r"C:\Users\Y A N I S\Desktop\set\output\fire_fire.csv", sep=";")
shape = pd.read_csv(r"C:\Users\Y A N I S\Desktop\set\output\merged_shape_precise.csv", sep=";")
soil = pd.read_csv(r"C:\Users\Y A N I S\Desktop\set\output\HWSD2_Algeria_Tunisia_D1_PREPROC_POINT.csv", sep=";")
elev = pd.read_csv(r"C:\Users\Y A N I S\Desktop\set\output\gmted_elevation_preprocessed.csv", sep=";")
clim = pd.read_csv(r"C:\Users\Y A N I S\Desktop\set\output\rasters_seasonal_capped.csv", sep=";")

# Harmoniser les coordonnées et forcer float
fire = fire.rename(columns={"longitude": "X", "latitude": "Y"})
for df in [fire, shape, soil, elev, clim]:
    df["X"] = df["X"].astype(float)
    df["Y"] = df["Y"].astype(float)

print(f"\n📊 Données chargées:")
print(f"   Fire:  {fire.shape[0]:,} lignes")
print(f"   Shape: {shape.shape[0]:,} lignes")
print(f"   Elev:  {elev.shape[0]:,} lignes")
print(f"   Clim:  {clim.shape[0]:,} lignes")
print(f"   Soil:  {soil.shape[0]:,} lignes")

print("\nAnalyse des distances de jointure (percentiles)")
def analyze_distances(fire_df, data_df, name):
    data_coords = data_df[['X', 'Y']].values
    tree = cKDTree(data_coords)
    fire_coords = fire_df[['X', 'Y']].values
    distances, _ = tree.query(fire_coords, k=1)
    perc95 = np.percentile(distances, 95)
    perc98 = np.percentile(distances, 98)
    perc99 = np.percentile(distances, 99)
    print(f"{name:10s}: 95% ≤ {perc95:.5f}°, 98% ≤ {perc98:.5f}°, 99% ≤ {perc99:.5f}° (~{perc95*111:.2f} km)")
    return distances

dist_elev  = analyze_distances(fire, elev,  "elevation")
dist_clim  = analyze_distances(fire, clim,  "climat")
dist_soil  = analyze_distances(fire, soil,  "sol")
dist_shape = analyze_distances(fire, shape, "shape")


MAX_DIST_ELEV  = max(0.00833, np.percentile(dist_elev, 95))   
MAX_DIST_CLIM  = max(0.08333, np.percentile(dist_clim, 95))   
MAX_DIST_SOIL  = max(0.00833, np.percentile(dist_soil, 95))   
MAX_DIST_SHAPE = max(0.00833, np.percentile(dist_shape, 95))  # shape resolution

print(f"\nDistances utilisées :")
print(f"   Elevation: {MAX_DIST_ELEV:.5f}° (~{MAX_DIST_ELEV*111:.2f} km)")
print(f"   Climate:   {MAX_DIST_CLIM:.5f}° (~{MAX_DIST_CLIM*111:.2f} km)")
print(f"   Soil:      {MAX_DIST_SOIL:.5f}° (~{MAX_DIST_SOIL*111:.2f} km)")
print(f"   Shape:     {MAX_DIST_SHAPE:.5f}° (~{MAX_DIST_SHAPE*111:.2f} km)")

def spatial_join_nearest(fire_df, data_df, max_distance):
    data_coords = data_df[['X', 'Y']].values
    tree = cKDTree(data_coords)
    fire_coords = fire_df[['X', 'Y']].values
    distances, indices = tree.query(fire_coords, k=1)
    valid_mask = distances <= max_distance
    result = fire_df.copy()
    data_cols = [col for col in data_df.columns if col not in ['X', 'Y']]
    for col in data_cols:
        result[col] = np.nan
        result.loc[valid_mask, col] = data_df.iloc[indices[valid_mask]][col].values
    print(f"{data_df.shape[0]} → {valid_mask.sum()} / {fire_df.shape[0]} appariés ({100*valid_mask.sum()/fire_df.shape[0]:.1f}%)")
    return result

# Jointures
df_result = spatial_join_nearest(fire, elev,  MAX_DIST_ELEV)
df_result = spatial_join_nearest(df_result, clim,  MAX_DIST_CLIM)
df_result = spatial_join_nearest(df_result, soil,  MAX_DIST_SOIL)
df_result = spatial_join_nearest(df_result, shape, MAX_DIST_SHAPE)

# Variables climatiques saisonnières
df_result['acq_date'] = pd.to_datetime(df_result['acq_date'], errors='coerce')
def get_season(date):
    if pd.isna(date): return None
    m = date.month
    if m in [12, 1, 2]: return 'hiver'
    elif m in [3, 4, 5]: return 'printemps'
    elif m in [6, 7, 8]: return 'ete'
    elif m in [9, 10, 11]: return 'automne'
    return None
df_result['saison_temp'] = df_result['acq_date'].apply(get_season)
for v in ['prec', 'tmin', 'tmax']:
    df_result[v] = np.nan
for idx, row in df_result.iterrows():
    s = row['saison_temp']
    if pd.notna(s):
        for v in ['prec', 'tmin', 'tmax']:
            df_result.at[idx, v] = row.get(f'{v}_{s}', np.nan)
print("✅ Variables climatiques créées (prec, tmin, tmax)")

# Nettoyage des colonnes saisonnières
cols_to_drop = [col for col in df_result.columns if any(suffix in col for suffix in ['_hiver', '_printemps', '_ete', '_automne'])]
cols_to_drop.append('saison_temp')
df_final = df_result.drop(columns=cols_to_drop, errors='ignore')

print(f"\n📊 Dataset final: {df_final.shape[0]:,} lignes × {df_final.shape[1]} colonnes")

# Statistiques couverture
missing_elev = df_final['elevation_prep'].isna().sum()
missing_clim = df_final['prec'].isna().sum()
missing_soil_sand = df_final['SAND'].isna().sum() if 'SAND' in df_final.columns else len(df_final)
complete_rows = df_final.dropna(subset=['elevation_prep', 'prec', 'SAND'] if 'SAND' in df_final.columns else ['elevation_prep', 'prec'])

print(f"\n📈 Couverture par source :")
print(f"   Elevation : {len(df_final)-missing_elev:,} / {len(df_final):,} ({100*(len(df_final)-missing_elev)/len(df_final):.1f}%)")
print(f"   Climate   : {len(df_final)-missing_clim:,} / {len(df_final):,} ({100*(len(df_final)-missing_clim)/len(df_final):.1f}%)")
print(f"   Soil      : {len(df_final)-missing_soil_sand:,} / {len(df_final):,} ({100*(len(df_final)-missing_soil_sand)/len(df_final):.1f}%)")
print(f"\n✅ Lignes complètes (toutes sources): {len(complete_rows):,} / {len(df_final):,} ({100*len(complete_rows)/len(df_final):.1f}%)")

output_path = r"C:\Users\Y A N I S\Desktop\set\output\full_merged_FINAL_95PCT.csv"
df_final.to_csv(output_path, sep=";", index=False)
print(f"\n💾 Fichier sauvegardé: {output_path}")

cols_to_show = ['X', 'Y', 'acq_date', 'fire', 'elevation_prep', 'prec', 'tmin', 'tmax']
if 'SAND' in df_final.columns: cols_to_show.append('SAND')
cols_available = [c for c in cols_to_show if c in df_final.columns]
print(df_final[cols_available].head(10))

print("\n" + "="*70)
print("✅ TERMINÉ !")
print("="*70)
