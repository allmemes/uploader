import pandas as pd
import os
import json
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import csvprocessing as cp
import geometry_tools as gt
import urllib
import urllib.request
import collections
from shapely.geometry import mapping, Point
import warnings
from shapely.errors import ShapelyDeprecationWarning
from threading import Thread
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning) 

class uploader:
    def __init__(self):
        # basic metaData info
        self.token = None
        self.loginSuccess = False
        self.taskType = None
        self.defualtRadius = 0
        self.dronePointUrl = None
        self.droneBufferUrl = None
        self.dronePeakUrl = None
        self.manualPointsUrl = None
        self.manualBufferUrl = None
        self.validMetaData = False
        # basic input csv info
        self.inputCsvs = []
        self.csvDict = {}
        ''' appended check
        Every time app starts/restarts: self.appRestarted is true

        when selecting csv folder 
        loop csv and check csv name with hashtable: self.csvCounter:
            if new csv name in key:
                1, pass, continue
            if not in key:
                1, process the csv
                    - if no error, add processed df to inputCsv and record the csv in a temporary dict
                    - if error, clear the self.inputCsv. return 
        merge the new dict batch with the previous csvCounter with update.

        when appending
        check inputCsvs length = 0!
        if restart = true: loop every inital input and use query to check
                    if appended (do an initial, all around check once), 
                    and then set self.restarted to false after loop.
        if restart = false: do not uses query api. this means this app
                    has at least gone through an initial, all around check, 
                    csv names have been recorded, can only rely on the 
                    hashtable to check appended or not.
        clear self.ipnutCsv after this batch append.
        '''
        self.appRestarted = True
        self.summary = {"points":[], "polySuc":[], "polyFail":[], "peakSuc":[], "peakFail":[], "invalid":[]}
        # basic gui set up
        self.window = tk.Tk()
        self.canvas = tk.Canvas(self.window, width = 500, height = 320,  relief = 'raised')
        self.canvas.pack()
        # title
        self.title = tk.Label(self.window, text = 'Field data uploader')
        self.title.config(font=('helvetica', 20))
        self.canvas.create_window(250, 40, window=self.title)
        # userName
        self.userNameLabel = tk.Label(self.window, text = 'UserName: ')
        self.userNameLabel.config(font=('helvetica', 13))
        self.canvas.create_window(120, 90, window=self.userNameLabel)
        self.userNameEntry = tk.Entry(self.window) 
        self.canvas.create_window(270, 90, window=self.userNameEntry)
        # passWord
        self.passWordLabel = tk.Label(self.window, text = 'PassWord: ')
        self.passWordLabel.config(font=('helvetica', 13))
        self.canvas.create_window(120, 120, window=self.passWordLabel)
        self.passWordEntry = tk.Entry(self.window, show="*") 
        self.canvas.create_window(270, 120, window=self.passWordEntry)
        # log in
        self.loginButton = tk.Button(text='login', command=self.login, font=('helvetica', 15))
        self.canvas.create_window(410, 105, window=self.loginButton)
        # metadata file path
        self.metaLabel = tk.Label(self.window, text = 'Select metaData:')
        self.metaLabel.config(font=('helvetica', 13))
        self.canvas.create_window(120, 170, window=self.metaLabel)
        self.metaEntry = tk.Entry(self.window)
        self.canvas.create_window(270, 170, window=self.metaEntry)
        self.metaButton = tk.Button(text='browse', command=self.searchMeta, font=('helvetica', 15))
        self.canvas.create_window(410, 170, window=self.metaButton)
        # input csv folder path
        self.csvLabel = tk.Label(self.window, text = 'Select input csvs:')
        self.csvLabel.config(font=('helvetica', 13))
        self.canvas.create_window(120, 220, window=self.csvLabel)
        self.csvEntry = tk.Entry(self.window)
        self.canvas.create_window(270, 220, window=self.csvEntry)
        self.csvButton = tk.Button(text='browse', command=self.searchCsv, font=('helvetica', 15))
        self.canvas.create_window(410, 220, window=self.csvButton)
        # start button
        self.startButton = tk.Button(text='append', command=self.appendAllData, font=('helvetica', 15))
        self.canvas.create_window(410, 270, window=self.startButton)

    def clearSummary(self):
        for i in self.summary.values():
            i.clear()

    def searchMeta(self):
        path = filedialog.askopenfile(filetypes=[("json files", (".json"))])
        self.metaEntry.delete(0, tk.END)
        self.metaEntry.insert(tk.END, path)
        # process metaData.json
        metaDataPath = self.metaEntry.get()
        metaData = metaDataPath.split(" ")[1].split("'")[1]
        try:
            with open(metaData,'r',encoding='cp936') as f:
                data = json.load(f)
            # determine task info
            self.taskType = data["Inspection Type"]
            self.defaultRadius = data["Default Buffer Radius"]
            if self.taskType == "Inficon":
                self.manualBufferUrl = data["List of Layers and types"]["manualbuffer"]
                self.manualPointsUrl = data["List of Layers and types"]["manualpoints"]
            else:
                self.droneBufferUrl = data["List of Layers and types"]["dronebuffer"]
                self.dronePointUrl = data["List of Layers and types"]["dronepoints"]
                self.dronePeakUrl = data["List of Layers and types"]["peaks"]
            self.validMetaData = True
        except: 
            messagebox.showerror("Error", "Invalid metadata!")
            self.validMetaData = False
            return

    def readInficonDf(self, csvPath):
        with open(csvPath, "r") as file:
            if file.read(1) == "I":
                while file.readline() != '\n':
                    pass
                df = pd.read_csv(file)
            else:
                df = None
        return df 

    def searchCsv(self):
        path = filedialog.askdirectory()
        self.csvEntry.delete(0, tk.END)
        self.csvEntry.insert(tk.END, path)

    def preprocess(self, cleanedDf):
        gt.add_points_to_df(cleanedDf)
        points = gt.series_to_multipoint(cleanedDf["points"])
        sr = gt.find_utm_zone(points[0].y, points[0].x)
        points = gt.reproject(points, sr)
        return points, cleanedDf

    def toEsriGeometry(self, geoJson):
        esriGeometry = {'rings': []}
        if geoJson["type"] == 'Polygon':
            for i in geoJson["coordinates"]:
                    esriGeometry['rings'].append(i)
        else:
            for i in geoJson["coordinates"]:
                for j in i:
                    esriGeometry['rings'].append(j)
        return json.dumps(esriGeometry)
 
    def createBuff(self, points):
        buff = points.buffer(15, resolution=6)
        rawJson = mapping(buff)
        esriJson = self.toEsriGeometry(rawJson)
        return json.loads(esriJson)

    def get_token(self, userName, passWord):
        referer = "http://www.arcgis.com/"
        query_dict = {'username': userName, 'password': passWord, 'referer': referer, 'expiration': 900}
        query_string = urllib.parse.urlencode(query_dict).encode('utf-8')
        url = "https://www.arcgis.com/sharing/rest/generateToken"
        token = json.loads(urllib.request.urlopen(url + "?f=json", query_string).read())
        if "token" not in token:
            # print(token['error'])
            return None
        else:
            return token["token"]

    def login(self):
        userName = self.userNameEntry.get()
        passWord = self.passWordEntry.get()
        if not userName:
            messagebox.showerror("Error", "Username is empty!")
            return  
        if not passWord:
            messagebox.showerror("Error", "Password is empty!")
            return  
        self.token = self.get_token(userName, passWord)
        if not self.token:
            messagebox.showerror("Error", "Wrong login info!")
            return  
        else:
            messagebox.showinfo("Success", "Login success!")
            self.loginSuccess = True

    def query_feature(self, token, sql, targetUrl):
        # sql = "Source_Name = 'N1_15_2.8_20220620_153056.csv'"
        query_dict = {"f": "json", "token": token, "where": sql}
        jsonResponse = urllib.request.urlopen(targetUrl + r"/query", urllib.parse.urlencode(query_dict).encode('utf-8'))
        jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
        if len(jsonOutput["features"]) > 0:
            return True
        else:
            return False

    def add_point_features(self, features, targetUrl):
        appending_dict = {"f": "json",
                    "token": self.token,
                    "features": features}
        urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))

    def add_peak_features(self, features, targetUrl, peaksDict):
        appending_dict = {"f": "json",
                    "token": self.token,
                    "features": features}
        jsonResponse = urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))
        jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
        if "error" in jsonOutput.keys():
            self.summary["invalid"].append("peaks")
        else:
            appendIndex = 0
            skippedIndexNumber = 0
            while appendIndex < len(jsonOutput['addResults']):
                result = jsonOutput['addResults'][appendIndex]
                # recombine the layer name
                layerName = features[appendIndex]["attributes"]["Source_Name"] + "-peaks"
                # get the total number of peaks of current appended layers and compare it with the current appendIndex
                totalPeaksNumber = peaksDict[features[appendIndex]["attributes"]["Source_Name"]]
                if result["success"]:
                    # if append success and if the current index - previously skipped index + 1 = total peaks number, then add peaks layer name to the returnJson
                    if appendIndex - skippedIndexNumber + 1 == totalPeaksNumber:
                        self.summary["peakSuc"].append(layerName)
                        skippedIndexNumber += totalPeaksNumber
                    appendIndex += 1
                else:
                    # if append fail, skip to the index of next layer, make skipping step = old skipped index number + current total peaks number.
                    self.summary["peakFail"].append(layerName)
                    appendIndex = skippedIndexNumber + totalPeaksNumber
                    skippedIndexNumber += totalPeaksNumber

    def add_buffer_features(self, features, targetUrl):
        appending_dict = {"f": "json",
                    "token": self.token,
                    "features": features}
        jsonResponse = urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))
        jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
        if "error" in jsonOutput.keys():
            self.summary["invalid"].append("buffer")
        else:
            for i,j in enumerate(jsonOutput['addResults']):
                layerName = features[i]["attributes"]["Source_Name"] + "-buffer"
                if j['success']:
                    self.summary["polySuc"].append(layerName)
                else:
                    self.summary["polyFail"].append(layerName)

    def bufferThread(self, bufferFeatures, bufferUrl):
        for i in self.inputCsvs:
            points, cleanedDf = self.preprocess(i)
            sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
            if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, bufferUrl))):
                print("Appending buffer for ", cleanedDf["Source_Name"][0])
                geoJson = self.createBuff(points)
                uploadStruct = {
                    "attributes" : {"Source_Name": cleanedDf["Source_Name"][0]},
                    "geometry" : geoJson}
                bufferFeatures.append(uploadStruct)
        if len(bufferFeatures) > 0:
            self.add_buffer_features(bufferFeatures, bufferUrl)
        else:
            print("No new buffer appended since last appending")

    def inficonPointThread(self, pointFeatures):
        for i in self.inputCsvs:
            points, cleanedDf = self.preprocess(i)
            sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
            if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.manualPointsUrl))):
                print("Appending points for ", cleanedDf["Source_Name"][0])
                for index,row in cleanedDf.iterrows():
                    esriPoint = {"attributes" : {
                                    "Inspection_Date": row["Flight_Date"],
                                    "Inspection_Time": row["Flight_Date"].split(" ")[1],
                                    "Lat": row["SenseLat"],
                                    "Long": row["SenseLong"],
                                    "CH4": row["CH4"],
                                    "Source_Name" : row["Source_Name"]
                                    },
                                    "geometry" :
                                    {"x" : points[index].x, "y" : points[index].y}}
                    pointFeatures.append(esriPoint) 
                self.summary["points"].append(cleanedDf["Source_Name"][0] + "-points")
        if len(pointFeatures) > 0:
            self.add_point_features(pointFeatures, self.manualPointsUrl)
        else:
            print("No new points appended since last appending")
        
    def snifferPointThread(self, pointFeatures):
        for j in self.inputCsvs:
            points, cleanedDf = self.preprocess(j)
            sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
            if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.dronePointUrl))):
                print("Appending points for ", cleanedDf["Source_Name"][0])
                for index,row in cleanedDf.iterrows():
                    esriPoint = {"attributes" : {
                            "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                            "SenseLat": row["SenseLat"],
                            "SenseLong": row["SenseLong"],
                            "CH4": row["CH4"],
                            "Source_Name" : row["Source_Name"]
                            },
                            "geometry" :
                            {"x" : points[index].x, "y" : points[index].y}}
                    pointFeatures.append(esriPoint)
                self.summary["points"].append(cleanedDf["Source_Name"][0] + "-points")
        if len(pointFeatures) > 0:
            self.add_point_features(pointFeatures, self.dronePointUrl)
        else:
            print("No new points appended since last appending")

    def peakThread(self, peaksFeatures, peaksDict):
        for j in self.inputCsvs:
            points, cleanedDf = self.preprocess(j)
            sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
            if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.dronePeakUrl))): 
                print("Appending peaks for ", cleanedDf["Source_Name"][0])
                orig_id = 1
                cp.find_ch4_peaks(cleanedDf)
                peaks = cleanedDf[cleanedDf['Peak'] == 1]
                if (len(peaks) > 0):
                    peaksDict[cleanedDf["Source_Name"][0]] = len(peaks)*2
                    for index, row in peaks.iterrows():
                        peakCenter = Point(points[index].x, points[index].y)
                        outerCircle = {"attributes" : {
                        "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                        "SenseLat": row["SenseLat"],
                        "SenseLong": row["SenseLong"],
                        "CH4": row["CH4"],
                        "Source_Name" : row["Source_Name"],
                        "BUFF_DIST": 13.57884,
                        "ORIG_FID": orig_id}}
                        outerBuffer = mapping(peakCenter.buffer(13.57884, resolution=6))
                        esriOuterBuffer = json.loads(self.toEsriGeometry(outerBuffer))
                        outerCircle["geometry"] = esriOuterBuffer
                        peaksFeatures.append(outerCircle)
                        innerCircle = {"attributes" : {
                        "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                        "SenseLat": row["SenseLat"],
                        "SenseLong": row["SenseLong"],
                        "CH4": row["CH4"],
                        "Source_Name" : row["Source_Name"],
                        "BUFF_DIST": 5.876544,
                        "ORIG_FID": orig_id}}
                        innerBuffer = mapping(peakCenter.buffer(5.876544, resolution=6))
                        esriInnerBuffer = json.loads(self.toEsriGeometry(innerBuffer))
                        innerCircle["geometry"] = esriInnerBuffer  
                        peaksFeatures.append(innerCircle)
                        orig_id += 1
                else:
                    print("no peaks for ", cleanedDf["Source_Name"][0])
        if len(peaksFeatures) > 0:
            self.add_peak_features(peaksFeatures, self.dronePeakUrl, peaksDict)
        else:
            print("No new peaks appended since last appending")

    def appendAllData(self):
        # some checks before appending
        if not self.userNameEntry.get():
            messagebox.showerror("Error", "Username is empty!")
            return     
        if not self.passWordEntry.get():
            messagebox.showerror("Error", "Password is empty!")
            return   
        if not self.loginSuccess:
            messagebox.showerror("Error", "You have not successfully login")
            return  
        if not self.metaEntry.get():
            messagebox.showerror("Error", "MetaData path is empty!")
            return
        if not self.validMetaData: 
            messagebox.showerror("Error", "Cannot proceed with invalid metadata!")
            return 
        # check input csvs
        path = self.csvEntry.get()
        if not path:
            messagebox.showerror("Error", "Csv input path is empty!")
            return

        # open loading popup for appending
        popup = tk.Toplevel(self.window)
        popup.geometry("200x150")
        tk.Message(popup, text="Appending..", padx=20, pady=20).pack()
        popup.update()
        # process csv
        all_csvs = os.listdir(path)    
        names = list(filter(lambda f: f.endswith('.csv'), all_csvs))
        tempDict = {}
        for i in names:
            try:
                if self.csvDict[i]:
                    continue
            except:
                if self.taskType == "Inficon":
                    newInput = self.readInficonDf(path + "\\" + i)
                    if not isinstance(newInput, pd.DataFrame):
                        messagebox.showerror("Error", "Wrong csv type.")
                        self.inputCsvs.clear()
                        popup.destroy()
                        return
                    cleanedDf = cp.cleanInficon(i, newInput)
                else:
                    try:
                        newInput = pd.read_csv(path + "\\" + i)
                    except:
                        messagebox.showerror("Error", "Wrong csv type.")
                        popup.destroy()
                        self.inputCsvs.clear()
                        return
                    cleanedDf = cp.clean_flight_log(i, newInput)
                self.inputCsvs.append(cleanedDf)
                tempDict[i] = 1
        self.csvDict.update(tempDict)

        # start appending.
        if len(self.inputCsvs) > 0:
            pointFeatures, bufferFeatures = [], []
            # if the inputCsvs collector contains df, then start the append process. 
            # Use self.appRestart to check whether we need to do a first, all around api query check.
            # always set the self.appRestart to False after we do a first, all around api query check.
            if self.taskType == "Inficon":
                infiBufferTask = Thread(target=self.bufferThread, args=[bufferFeatures, self.manualBufferUrl])
                infiPointTask = Thread(target=self.inficonPointThread, args=[pointFeatures])
                infiBufferTask.start()
                infiPointTask.start() 
                infiBufferTask.join()
                infiPointTask.join()

            else:
                peaksFeatures = []
                peaksDict = {}
                snifBufferTask = Thread(target=self.bufferThread, args=[bufferFeatures, self.droneBufferUrl])
                snifPeakTask = Thread(target=self.peakThread, args=[peaksFeatures, peaksDict])
                SnifPointTask = Thread(target=self.snifferPointThread, args=[pointFeatures])
                snifBufferTask.start()
                snifPeakTask.start() 
                SnifPointTask.start()
                snifBufferTask.join()
                snifPeakTask.join()
                SnifPointTask.join()

                # for j in self.inputCsvs:
                #     points, cleanedDf = self.preprocess(j)
                #     sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
                #     # append snifferdrone buffers
                #     if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.droneBufferUrl))):
                #         print("Appending buffer for ", cleanedDf["Source_Name"][0])
                #         geoJson = self.createBuff(points)
                #         uploadStruct = {
                #             "attributes" : {"Source_Name": cleanedDf["Source_Name"][0]},
                #             "geometry" : geoJson}
                #         bufferFeatures.append(uploadStruct)
                #     # append snifferdrone peaks
                #     if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.dronePeakUrl))): 
                #         print("Appending peaks for ", cleanedDf["Source_Name"][0])
                #         orig_id = 1
                #         cp.find_ch4_peaks(cleanedDf)
                #         peaks = cleanedDf[cleanedDf['Peak'] == 1]
                #         if (len(peaks) > 0):
                #             peaksDict[cleanedDf["Source_Name"][0]] = len(peaks)*2
                #             for index, row in peaks.iterrows():
                #                 peakCenter = Point(points[index].x, points[index].y)
                #                 outerCircle = {"attributes" : {
                #                 "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                #                 "SenseLat": row["SenseLat"],
                #                 "SenseLong": row["SenseLong"],
                #                 "CH4": row["CH4"],
                #                 "Source_Name" : row["Source_Name"],
                #                 "BUFF_DIST": 13.57884,
                #                 "ORIG_FID": orig_id}}
                #                 outerBuffer = mapping(peakCenter.buffer(13.57884, resolution=6))
                #                 esriOuterBuffer = json.loads(self.toEsriGeometry(outerBuffer))
                #                 outerCircle["geometry"] = esriOuterBuffer
                #                 peaksFeatures.append(outerCircle)
                #                 innerCircle = {"attributes" : {
                #                 "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                #                 "SenseLat": row["SenseLat"],
                #                 "SenseLong": row["SenseLong"],
                #                 "CH4": row["CH4"],
                #                 "Source_Name" : row["Source_Name"],
                #                 "BUFF_DIST": 5.876544,
                #                 "ORIG_FID": orig_id}}
                #                 innerBuffer = mapping(peakCenter.buffer(5.876544, resolution=6))
                #                 esriInnerBuffer = json.loads(self.toEsriGeometry(innerBuffer))
                #                 innerCircle["geometry"] = esriInnerBuffer  
                #                 peaksFeatures.append(innerCircle)
                #                 orig_id += 1
                #         else:
                #             print("no peaks for ", cleanedDf["Source_Name"][0])
                #     # append snifferdrone points
                #     if (not self.appRestarted) or (self.appRestarted and (not self.query_feature(self.token, sql, self.dronePointUrl))):
                #         print("Appending points for ", cleanedDf["Source_Name"][0])
                #         for index,row in cleanedDf.iterrows():
                #             esriPoint = {"attributes" : {
                #                     "Flight_Date": row["Flight_Date"].strftime("%m/%d/%Y, %H:%M %p"),
                #                     "SenseLat": row["SenseLat"],
                #                     "SenseLong": row["SenseLong"],
                #                     "CH4": row["CH4"],
                #                     "Source_Name" : row["Source_Name"]
                #                     },
                #                     "geometry" :
                #                     {"x" : points[index].x, "y" : points[index].y}}
                #             pointFeatures.append(esriPoint)
                #         self.summary["points"].append(cleanedDf["Source_Name"][0] + "-points") 

                # if len(bufferFeatures) > 0:
                #     self.add_buffer_features(bufferFeatures, self.droneBufferUrl)
                # else:
                #     print("No new buffer appended since last appending")
                # if len(peaksFeatures) > 0:
                #     self.add_peak_features(peaksFeatures, self.dronePeakUrl, peaksDict)
                # else:
                #     print("No new peaks appended since last appending")
                # if len(pointFeatures) > 0:
                #     self.add_point_features(pointFeatures, self.dronePointUrl)
                # else:
                #     print("No new points appended since last appending")

            self.appRestarted = False  
            popup.destroy()
            self.inputCsvs.clear()
            
            summary = tk.Toplevel(self.window)
            summary.geometry("800x450")
            title = tk.Label(summary, text = self.taskType + " Task Summary:")
            title.config(font=('helvetica', 15))
            title.place(x=10, y=10)

            pointFinish = tk.Label(summary, text = 'Points appended:')
            pointFinish.place(x=20, y=60)
            pointList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["points"]:
                pointList.insert(tk.END, i)
            pointList.place(x=20, y=90)

            polySucL = tk.Label(summary, text = 'Polygons appended:')
            polySucL.place(x=280, y=60)
            polySucList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["polySuc"]:
                polySucList.insert(tk.END, i)
            polySucList.place(x=280, y=90)

            polyFailL = tk.Label(summary, text = 'Polygons appending fail:')
            polyFailL.place(x=540, y=60)
            polyFailList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["polyFail"]:
                polyFailList.insert(tk.END, i)
            polyFailList.place(x=540, y=90)

            peakSucL = tk.Label(summary, text = 'Peaks appended:')
            peakSucL.place(x=20, y=250)
            peakSucList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["peakSuc"]:
                peakSucList.insert(tk.END, i)
            peakSucList.place(x=20, y=280)

            peakFailL = tk.Label(summary, text = 'Peaks appending fail:')
            peakFailL.place(x=280, y=250)
            peakFailList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["peakFail"]:
                peakFailList.insert(tk.END, i)
            peakFailList.place(x=280, y=280)

            invalid = tk.Label(summary, text = 'Invalid json format:')
            invalid.place(x=540, y=250)
            invalidList = tk.Listbox(summary, height=8, width=34)
            for i in self.summary["invalid"]:
                invalidList.insert(tk.END, i)
            invalidList.place(x=540, y=280)

            summary.update()
            self.clearSummary()

        else: 
            popup.destroy()
            messagebox.showerror("Warning", "All csvs have already been appended for this batch.")
            return 

    def run(self):
        self.window.mainloop()
        

if __name__ == "__main__":
    app = uploader()
    app.run()