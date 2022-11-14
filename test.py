import pandas as pd

a = pd.DataFrame({"a":[1,2,34], "b":[2,3,4]})
for index,row in a.iterrows():
    print(index)
    print(row["a"])