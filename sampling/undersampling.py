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

# =======================
# 2. UNDERSAMPLING
# =======================
under = RandomUnderSampler(sampling_strategy=1, random_state=42)
X_under, y_under = under.fit_resample(X, y)


# =======================
# 4. Dataset final équilibré
# =======================
df_balanced = pd.concat([X_under, y_under], axis=1)
df_balanced.to_csv("balanced_dataset_under.csv", index=False)

print("✔ balanced_dataset_under.csv saved")
print(df_balanced['fire'].value_counts())
print(df_balanced.shape)
