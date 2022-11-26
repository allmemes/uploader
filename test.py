import pandas as pd
import datetime
from tkinter import *
from threading import Thread
import time

# class test:
#     def __init__(self):
#         self.root = Tk()
#         # Set geometry
#         self.root.geometry("400x400")
#         self.record = {"a":0, "b":0}
#         # Create Button
#         Button(self.root,text="Click Me",command = self.threading).pack()

#     # use threading
#     def threading(self):
#         # Call work function
#         t1=Thread(target=self.work1)
#         t2 = Thread(target=self.work2)
#         t1.start()
#         t2.start()
#         t1.join()
#         t2.join()
#         print(self.record)

#     # work function
#     def work1(self):
#         print("thread 1 sleep time start")
#         for i in range(10):
#             self.record["a"] += 1
#             print(i)
#             time.sleep(1)
#         print("thread 1 sleep time stop")

#     def work2(self):
#         print("thread 2 sleep time start")
#         for i in range(5):
#             self.record["b"] += 1
#             print(i)
#             time.sleep(1)
#         print("thread 2 sleep time stop")
  
#     def run(self):
#         # Execute Tkinter
#         self.root.mainloop()

# go1 = test()
# go1.run()


summary = Tk()
summary.geometry("800x450")
task = "Inficon" + " Task Summary:"
title = Label(summary, text = task)
title.config(font=('helvetica', 15))
title.place(x=10, y=10)

pointFinish = Label(summary, text = 'Points appended:')
pointFinish.place(x=20, y=60)
pointList = Listbox(summary, height=8, width=34)
for line in range(50):
   pointList.insert(END, 'This is line number' + str(line))
pointList.place(x=20, y=90)

polySucL = Label(summary, text = 'Polygons appended:')
polySucL.place(x=280, y=60)
polySucList = Listbox(summary, height=8, width=34)
for line in range(50):
   polySucList.insert(END, 'This is line number' + str(line))
polySucList.place(x=280, y=90)

polyFailL = Label(summary, text = 'Polygons appending fail:')
polyFailL.place(x=540, y=60)
polyFailList = Listbox(summary, height=8, width=34)
for line in range(50):
   polyFailList.insert(END, 'This is line number' + str(line))
polyFailList.place(x=540, y=90)

peakSucL = Label(summary, text = 'Peaks appended:')
peakSucL.place(x=20, y=250)
peakSucList = Listbox(summary, height=8, width=34)
for line in range(50):
   peakSucList.insert(END, 'This is line number' + str(line))
peakSucList.place(x=20, y=280)

peakFailL = Label(summary, text = 'Peaks appending fail:')
peakFailL.place(x=280, y=250)
peakFailList = Listbox(summary, height=8, width=34)
for line in range(50):
   peakFailList.insert(END, 'This is line number' + str(line))
peakFailList.place(x=280, y=280)

invalid = Label(summary, text = 'Invalid json format:')
invalid.place(x=540, y=250)
invalidList = Listbox(summary, height=8, width=34)
for line in range(50):
   invalidList.insert(END, 'This is line number' + str(line))
invalidList.place(x=540, y=280)

mainloop()

# a = pd.DataFrame({"a":[1,2,34], "b":[2,3,4]})
# # for index,row in a.iterrows():
# #     print(index, row["a"])
# b = datetime.datetime.now()
# strB = b.strftime("%m/%d/%Y, %H:%M %p")
# print(strB)

# restarted = True
# added = True

# if (not restarted) or (restarted and (not added)):
#     print("yes")

# a = {"a":[1,2,3], "b":[2,3,5]}
# for i in a.values():
#     i.clear()
# print(a)