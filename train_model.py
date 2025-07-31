import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib

# Simulate transaction data (amount + frequency features)
data = pd.DataFrame({
    "amount": np.random.uniform(1, 500, 1000),
    "txn_per_hour": np.random.randint(1, 10, 1000)
})

# Train IsolationForest
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(data)

# Save model
joblib.dump(model, "fraud_model.pkl")
print("✅ Model trained & saved as fraud_model.pkl")
