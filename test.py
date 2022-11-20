import pandas as pd
import datetime

a = pd.DataFrame({"a":[1,2,34], "b":[2,3,4]})
# for index,row in a.iterrows():
#     print(index, row["a"])
b = datetime.datetime.now()
strB = b.strftime("%m/%d/%Y, %H:%M %p")
print(strB)