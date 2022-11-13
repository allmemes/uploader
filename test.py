import pandas as pd

a = pd.DataFrame({"a":[1,2,34], "b":[2,3,4]})
# for _,row in a.iterrows():
#     print(row["a"])
print(a["a"][0])