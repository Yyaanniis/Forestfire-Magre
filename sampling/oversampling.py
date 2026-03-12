import pandas as pd
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler

# =======================
# 1. Charger les données
# =======================
df = pd.read_csv("donnees_normalisees_encodes.csv", sep=",")

# Séparer X et y
y = df['fire']
X = df.drop(columns=['fire'])
X = X.drop(columns=['acq_date']) 

# =======================
# 3. SMOTE oversampling
# =======================
over = SMOTE(sampling_strategy=1.0, random_state=42, k_neighbors=5)
X_res, y_res = over.fit_resample(X, y)

# =======================
# 4. Dataset final équilibré
# =======================
df_balanced = pd.concat([X_res, y_res], axis=1)
df_balanced.to_csv("balanced_dataset_over.csv", index=False)

print("balanced_dataset_over.csv saved")
print(df_balanced['fire'].value_counts())
print(df_balanced.shape)
