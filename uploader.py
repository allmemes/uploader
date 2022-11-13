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
from shapely.geometry import MultiPoint, mapping, Point

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
        self.validAllCsv = False
        ''' appended check
        Every time app starts/restarts: self.appRestarted is true

        when selecting csv folder 
        loop csv and check csv name with hashtable: self.csvCounter:
            if new csv name in key:
                1, pass, continue
            if not in key:
                1, process the csv
                    - if no error, add processed df to inputCsv and record the csv in a temporary dict
                    - if error, clear the self.inputCsv. set validCsv to false. return 
        merge the new dict batch with the previous csvCounter with update.
        set validCsv to true

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
        # read in the input folder path and obtain all csvs.
        path = filedialog.askdirectory()
        self.csvEntry.delete(0, tk.END)
        self.csvEntry.insert(tk.END, path)
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
                        self.validAllCsv = False
                        return
                    cleanedDf = cp.cleanInficon(i, newInput)
                else:
                    try:
                        newInput = pd.read_csv(path + "\\" + i)
                    except:
                        messagebox.showerror("Error", "Wrong csv type.")
                        self.inputCsvs.clear()
                        self.validAllCsv = False
                        return
                    cleanedDf = cp.clean_flight_log(i, newInput)
                self.inputCsvs.append(cleanedDf)
                tempDict[i] = 1
        self.csvDict.update(tempDict)
        # print(self.csvDict)
        self.validAllCsv = True

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

    def add_buffer_features(self, features, targetUrl):
        appending_dict = {"f": "json",
                    "token": self.token,
                    "features": features}
        urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))

    def appendAllData(self):
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
        if not self.csvEntry.get():
            messagebox.showerror("Error", "Csv input path is empty!")
            return
        if not self.validAllCsv: 
            messagebox.showerror("Error", "Cannot append with wrong csvs!")
            return 

        if len(self.inputCsvs) > 0:
            pointFeatures, bufferFeatures = [], []
            # if the inputCsvs collector contains df, then start the append process. 
            # Use self.appRestart to check whether we need to do a first, all around api query check.
            # always set the self.appRestart to False after we do a first, all around api query check.
            if self.taskType == "Inficon":
                if self.appRestarted:
                    for i in self.inputCsvs:
                        points, cleanedDf = self.preprocess(i)
                        cleanedDf["Utmlong"] = [i.x for i in points]
                        cleanedDf["Utmlat"] = [i.y for i in points]
                        geoJson = self.createBuff(points)
                        sql = "Source_Name = '" + cleanedDf["Source_Name"][0] + "'"
                        # append inficon buffers
                        if not self.query_feature(self.token, sql, self.manualBufferUrl):
                            uploadStruct = {
                                "attributes" : {"Source_Name": cleanedDf["Source_Name"][0]},
                                "geometry" : geoJson
                            }
                            print(uploadStruct)
                            bufferFeatures.append(uploadStruct)
                            # print(bufferFeatures)
                        # append inficon points
                        if not self.query_feature(self.token, sql, self.manualPointsUrl):
                            for _,row in cleanedDf.iterrows():
                                esriPoint = {"attributes" : {
                                                "Inspection_Date": row["Flight_Date"],
                                                "Inspection_Time": row["Flight_Date"].split(" ")[1],
                                                "Lat": row["SenseLat"],
                                                "Long": row["SenseLong"],
                                                "CH4": row["CH4"],
                                                "Source_Name" : row["Source_Name"]
                                                },
                                                "geometry" :
                                                {
                                                    "x" : row["Utmlong"],
                                                    "y" : row["Utmlat"]
                                                }}
                                pointFeatures.append(esriPoint)
                            # print(pointFeatures)
                    self.appRestarted = False
                else:
                    pass


                if len(bufferFeatures) > 0:
                    self.add_buffer_features(bufferFeatures, self.manualBufferUrl)
                if len(pointFeatures) > 0:
                    self.add_point_features(pointFeatures, self.manualPointsUrl)
                messagebox.showinfo("Success", "Append Finished!")
            else:
                peaksFeatures = []
                pass


            # always clear the dfs for this batch append.
            self.inputCsvs.clear()
        else: 
            messagebox.showerror("Warning", "All csvs have already been appended for this batch.")
            return 

    def run(self):
        self.window.mainloop()
        

if __name__ == "__main__":
    app = uploader()
    app.run()