import pandas as pd

df = pd.read_csv("output.csv")
df.insert(0, "id", range(14602, 14602 + len(df)))    # 1-based IDs
df.to_csv("output_with_id.csv", index=False)