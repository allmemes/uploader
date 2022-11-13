from flask import Flask, request
from flask_cors import CORS, cross_origin
import sqlite3
import os
import json
import pandas as pd
import numpy as np
import csvprocessing as cp
import geometry_tools as gt
from shapely.geometry import MultiPoint, mapping, Point
import urllib
import urllib.request
import collections
import multiprocessing as mp


################
# Flask set up #
################

app = Flask(__name__)
app.config["DEBUG"] = True
app.config["APPLICATION_ROOT"] = "/"
app.config.from_object(__name__)
CORS(app, resources={r"/*":{'origins':'http://localhost:8080', "allow_headers":"Access-Control-Allow-Origin"}})

####################
# Global variables #
####################

sqliteTableList = ["PointsTable", "BuffersTable", "LinesTable", "PeaksTable"]
bufferResolution = 6

####################################
# basic database related functions #
####################################

def checkMetaDataBase(DBpath):
    '''
    ------------------------
    This function is to check whether there has been a database in some path. It will
    open a json called MetaDataBase.json and check if the input parameter DBpath is 
    with in the path list.
    ------------------------
    Input parameter: 
        DBpath: string
                Incoming sqlite database path.
    ------------------------
    Return:
        True or False: bool
                If the input string for database is found, return true, else return false.
    '''
    with open('MetaDataBase.json', 'r') as f:
        mataDataBase = json.load(f)
        allPaths = mataDataBase["paths"]
        if DBpath in allPaths:
            return True
        else:
            return False

def insertIntoMetaDataBase(DBpath):
    '''
    ------------------------
    This function is to APPEND the input database path into the MetaDataBase.json, so that
    it remembers that at certain location, there has been a data base there.
    This json will automatically be created when starting this program for the first time.
    ------------------------
    Input parameter: 
        DBpath: string
                Incoming sqlite database path.
    ------------------------
    Return:
        None
    '''
    with open('MetaDataBase.json', 'r+') as f:
        path_data = json.load(f)
        path_data["paths"].append(DBpath)
        f.seek(0)
        json.dump(path_data, f, indent = 4)

def connectAndUpload(DBpath, tableList):
    '''
    ------------------------
    This function is to 1, whenever a data base path is passed in, check if there is such a data base
    under the CURRENT FILE DIRECTORY (where this .py is lcoated). 

    2 a, If there is no such database, check if the MetaDataBase.json has previously recorded such 
    database path.
    3, If it has been recorded previously (but no such database has been found), report an
    error and send that to the front end.
    4, if it has not been recorded either, then create a new database with four table names (peaks, 
    buffers, points, and path) and insert this database path into the MetaDataBase.json.

    2 b, If there is such database, query data inside the path, peaks (if there is any), and buffer
    table only. Organize them in a json format that will be sent to the front end.
    ------------------------
    Input parameter: 
        DBpath: string
                Incoming sqlite database path.
        tableList: list
                this is just a lsit with predefined table name for looping purpose
    ------------------------
    Return:
        initialData: dict
            Json format for back and front end communication: 
            {
                tableName1: {
                    dataName1: geojson1,
                    dataName2: geojson2 .... 
                    },
                tableName2: {
                    dataName3: geojson3....
                }
            }
    '''
    initialData = {}
    connection = sqlite3.connect(DBpath)
    cursor = connection.cursor()
    if os.path.exists(DBpath):
        # load every geojson
        for i in tableList[1:]:
            data = cursor.execute(f"SELECT * FROM {i}").fetchall()
            if not len(data) == 0:
                initialData[i] = {}
                for j in data:
                    initialData[i][j[1]] = j[0]
                # {tableName: {name1: geometry1, name2: geometry2}}
    else:
        if not checkMetaDataBase(DBpath):
            # create database if not exist
            for i,j in enumerate(tableList):
                if i == 0:
                    create_table = '''\
                        CREATE TABLE IF NOT EXISTS {tableName} (
                        "Microsec" INTEGER,
                        "Flight_date" TEXT,
                        "Senselong" REAL NOT NULL,
                        "Senselat" REAL NOT NULL,
                        "CH4" INTEGER,
                        "Peak" INTEGER,
                        "Source_name" TEXT NOT NULL,
                        "Utmlong" REAL NOT NULL,
                        "Utmlat" REAL NOT NULL
                        )\
                        '''.format(tableName = j)
                    cursor.execute(create_table)
                else:
                    create_table = '''\
                    CREATE TABLE IF NOT EXISTS {tableName} (
                        "Geometry" TEXT,
                        "Source_name" TEXT,
                        "EsriGeometry" TEXT
                    )\
                    '''.format(tableName = j)
                    cursor.execute(create_table)
            connection.commit()
            insertIntoMetaDataBase(DBpath)
        else:
            initialData["error"] = "DB missing"
    return initialData

def deleteFromDB(DBpath, tableName, dataName):
    '''
    ------------------------
    This function is to delete any data base on the tableName and dataName, for example: delete path from csv1 from the lineTable.
    ------------------------
    Input parameter: 
        DBpath: string
                Incoming sqlite database path.
        tableName: string
                from which table to delete the data
        dataName: string
                which data to delete, usually this is the Source_Name of the layers.
    ------------------------
    Return:
        true or false: bool
                indicate whether it has been deleted successfully or not.
    '''
    try:
        connection = sqlite3.connect(DBpath)
        cursor = connection.cursor()
        delete_code = '''DELETE FROM {tableName} WHERE Source_name=="{dataName}"'''.format(tableName = tableName, dataName = dataName)
        cursor.execute(delete_code)
        connection.commit()
        return True
    except:
        return False

def insertGeoJsonIntoDB(DBcursor, tableName, dataName, geometry, esriGeo):
    '''
    ------------------------
    This function is to insert geojson and esrigeometry into the database (not used for inserting points)
    ------------------------
    Input parameter: 
        DBcursor: a sqlite database cursor object
                this is just to convenient use cursor, instead of passing in database path and initiate 
                connection everytime we need to insert a data. Instead we let the cursor do the work and 
                commit cursor at the end. 
        tableName: string
                which table to insert data
        dataName: string
                name of the inserted data, this is usually the Source_Name of the layers.
        geometry: geojson string
                text version of geojson that can be loaded into a json object and sent to the front end.
        esriGeo: esriJson string
                text version of esriGeometry, created by the toEsriGeometry function, used to be loaded
                passed into appended features when using the Arcgis rest api.
    ------------------------
    Return:
        None
    '''
    insert_code = '''INSERT INTO {tableName} VALUES (?, ?, ?)'''.format(tableName = tableName)
    DBcursor.execute(insert_code, [geometry, dataName, esriGeo])

##############################################
# functions to handle geojson and conversion #
##############################################

# convert geojson into esri geometry for polygons only.
def toEsriGeometry(geoJson):
    '''
    ------------------------
    This function is to use looping to convert regular polygon type of geojson into an esriGeometry json string.
    Example:
    convert {"type": "Polygon", "coordinates": [[[xxxx, xxxx], [xxxxx, xxxxx]]}
    to {"rings": [[xxxxx, xxxx], [xxxxx, xxxx]]}
    Likely to be the source of bug, since when the polygon is complicated and large, their inner nested list structed is hard to
    be cleared looped and converted to an esriGeometry.
    ------------------------
    Input parameter: 
        geoJson: geojson object
                input geojson for conversion
    ------------------------
    Return:
        esriGeometry: esriGemometry json string
                output esriGeometry string to be saved into database
    '''
    esriGeometry = {'rings': []}
    if geoJson["type"] == 'Polygon':
        for i in geoJson["coordinates"]:
                esriGeometry['rings'].append(i)
    else:
        for i in geoJson["coordinates"]:
            for j in i:
                esriGeometry['rings'].append(j)
    return json.dumps(esriGeometry)

# functions to create buffer and path json.
def createBuff(points, buffDis, sr):
    '''
    ------------------------
    This function is create the buffer around the given points and perform coordinate conversion for front end leaflet
    (WGS to UTM), while also crating the esriGeoemtry along the way.
    ------------------------
    Input parameter: 
        points: shapely multipoint list
                multipoints based on which the buffer will be created.
        buffDis: float or int
                buffer distance, usually 15m.
        sr: int
            utm zone number, see function find_utm_zone
    ------------------------
    Return:
        geo_j: geojson string
                string that represents the geojson structure, used to be saved into database, thus replacing double quote with
                single quote, since sqlite only accepts string with single quote.
        esriJson: esriGemometry string
                string that represents the esriGeometry, used to be saved into database, also with double quote replaced.
    '''
    buff = points.buffer(buffDis, resolution=bufferResolution)
    # create esriJson
    rawJson = mapping(buff)
    esriJson = toEsriGeometry(rawJson).replace('"', "'")
    # create geoJson
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j, esriJson

def createPath(df):
    '''
    ------------------------
    This function is to create a geojson string that represents a simplified line from a dataframe that contains the points cooridnates.
    The exact method of converting a group of points to a line is called rdp(), see rdp() from geometry_tool.py.
    ------------------------
    Input parameter: 
        df: dataframe
            dataframe that contains points coordiantes, which are SenseLong and SenseLat
    ------------------------
    Return:
        str(lineDict): geojson string
                        geojson string that represents a line
    '''
    points = df[["SenseLong", "SenseLat"]].to_numpy()
    lineDict = {'type': 'LineString'}
    lineDict['coordinates'] = gt.rdp(points, 0.0001)
    return str(lineDict)

# sniffer drone peaks creating function
def createSnifferPeaks(points, sr, df, conn):
    '''
    ------------------------
    This function has several tasks:
    1, it will firstly identify peaks from the input dataframe, which contains coordinate and ch4 information. It will add a new column to the
    dataframe, with 1 indicating peaks, 0 indicating none-peaks.
    2, it will then extract x and y coordinate from the input points list (in utm), make them into new columns, and add to the dataframe.
    This step is critical, since when appending the points and peaks later, the field map is in utm projection.
    3, Then it will turn the dataframe with selected columns into a sqlite table. By setting if-exist attributes, to_sql() can "append"
    the data into database.
    4, Then it filters the input point list, if there is no peaks, then return none. This looping step may seem unnecessary. However, 
    this is because: 
        a) leaflet only receives wgs projection, while shapely buffer works under UTM projection. 
        b) To create the points for the buffer function, we have to 1) find the utm zone of the points, 2) do the reproject() function on 
           the wgs points data from the field. 
        c) then we have do b) for peak and buffer data, which is unnecessary, as we reproject data to utm twice, and the utm zone number can be
           shared among two tasks. 
        d) thus, we obtain all the points under UTM projection first (do the reproject() once), no matter whether we are doing peak task 
           or buffer task. Then we filter out the peaks within the points data from the field.
    5, if there are peaks, we create the peak multipoints, create only the outer buffer so far (for front end view only), reproject json back to wgs,
    and create the json string for inserting into database.
    ------------------------
    Input parameter: 
        points: shapely multipoints
                all the points for this csv data.
        sr: int
            utm zone number, see function find_utm_zone 
        df: dataframe
            cleaned dataframe with all the points information.
        conn: sqlite3 database connection
            this is mainly to excute the to_sql() function.
    ------------------------ 
    Return:
        geo_j: geojson string
            geojson string that represents the outer buffer polygon for the peaks
    '''
    cp.find_ch4_peaks(df)
    # 1, add utmlong and utmlat colums.
    df["Utmlong"] = [i.x for i in points]
    df["Utmlat"] = [i.y for i in points]
    # 2, add the new df into sqlite point table
    df[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name", "Utmlong", "Utmlat"]].to_sql(name="PointsTable", con=conn, if_exists='append', index=False)
    # 3, filter peaks
    peaks = df[df['Peak'] == 1]
    if len(peaks) == 0:
        return None
    peakList = []
    for i in peaks.index:
        peakList.append((points[i].x, points[i].y))
    peakPoints = MultiPoint(peakList)
    # 4, create outer buffer for front end view only.
    buff = peakPoints.buffer(13.57884, resolution=bufferResolution)
    buff = gt.reproject(buff, 4326, sr)
    geo_j = gt.shapely_to_geojson(buff).replace('"', "'")
    return geo_j

#############################
# Arcgis rest api functions #
#############################

def get_token(userName, passWord):
    '''
    ------------------------
    This function is to get the token from the arcgis rest api. The token is used for following arcgis rest api functions.
    ------------------------
    Input parameter: 
        userName: string
                username string passed from the front end.
        passWord: string
                password string passed from the front end.
    ------------------------
    Return:
        token: string
    '''
    referer = "http://www.arcgis.com/"
    query_dict = {'username': userName, 'password': passWord,
                    'referer': referer, 'expiration': 900}
    query_string = urllib.parse.urlencode(query_dict).encode('utf-8')
    url = "https://www.arcgis.com/sharing/rest/generateToken"
    token = json.loads(urllib.request.urlopen(url + "?f=json", query_string).read())

    if "token" not in token:
        print(token['error'])
        return None
    else:
        return token["token"]

def add_point_features(token, features, targetUrl):
    '''
    ------------------------
    This function is to call the add_feature arcgis rest api to append point features. Since the arcgis rest api
    shows adding response for each feature added, the response for this rest api is usually gigantic. Thus, this
    function so far is designed in a way that we assume we will always add points data successfully, since the
    esri geometry for points data are usually extremely simple. 
    ------------------------
    Input parameter: 
        token: rest api token
               token for following rest api functions.
    features: list
            a list that contains all the points that need to be appended. Points object structures contain
            "attribute" and "geometry", see https://developers.arcgis.com/rest/services-reference/enterprise/add-features.htm
    targetUrl: url string
            url for target web layer. Usually, this will be stored in the project file for the inspection.
    ------------------------
    Return:
        None
    '''
    appending_dict = {"f": "json",
                    "token": token,
                    "features": features
                    }
    urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))

def add_buffer_features(token, features, targetUrl, returnJson):
    '''
    ------------------------
    Similar to the add point feature function, this function is to call the add_feature rest api to append buffer polygon features. 
    However, this function will modify the returnJson base on the appending result. If the appended json is invalid, layer name
    will be appended to the invalidJson list. If being appended successfully, the layer name will be appended to the success list, 
    otherwise to the fail list. 
    ------------------------
    Input parameter: 
        token: rest api token
                this token is used for this and following api calls.
        features: list
                a list of esri geometry that represents the buffers.
        targetUrl: string 
                url for the target web layer.
        returnJson: multiprocess dictionary
                this is a multiprocess json dictionary that will record the appending result and will be sent to the front end
                for rendering. Multiprocess is used to save time.
    ------------------------
    Return:
        None
    '''
    appending_dict = {"f": "json",
                    "token": token,
                    "features": features
                    }
    jsonResponse = urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))
    jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    if "error" in jsonOutput.keys():
        # the entire feature collection json is not formated correctly, not matter whether it is peaks or buffer
        returnJson["invalidJson"] += ["buffer"]
    else:
        for i,j in enumerate(jsonOutput['addResults']):
            layerName = features[i]["attributes"]["Source_Name"] + "-buffer"
            if j['success']:
                returnJson["bufferSuccess"] += [layerName]
            else:
                returnJson["bufferFail"] += [layerName]

def add_peak_features(token, features, targetUrl, returnJson, peaksDict):
    '''
    ------------------------
    1, Similar to the add point feature function, this function is to append peak features to the target url through rest api. However, this
    function will also record the layer name if the input json is in invalid json format.
    2, If the json is valid, the layer name will be added into the success list, if being appneded successfully. Otherwise, the layer name
    will be added to the fail list.
    3, There is more to the fail and success judging process. Since for peaks, each row in the field map is the center of a peak, and there
    are always even number of rows and every two rows contain duplicated attributes (but with different buffer distance), there will always
    be even number of appending response from the rest api. If all of peaks for a single layer are showing success response, then we add 
    the layer name to the success list. If any one of those peaks from a single layer is showing failing reponse, we skip all of the rest of
    the appending response for that layer, and insert the layer name into the fail list. This is achieved through an external dictionary that
    record how many peaks are there in a single layer, as well as performing index jump on the response.
    ------------------------
    Input parameter: 
        token: rest api token
                this token is used for this and following api calls.
        features: list
                a list of esri geometry that represents the peaks.
        targetUrl: string 
                url for the target web layer.
        returnJson: multiprocess dictionary
                this is a multiprocess json dictionary that will record the appending result and will be sent to the front end
                for rendering. Multiprocess is used to save time.
        peaksDict: dictionary
                a dictionary to record how many peaks are there inside a layer, usually containing even number of elements.
    ------------------------
    Return:
        None
    '''
    appending_dict = {"f": "json",
                    "token": token,
                    "features": features
                    }
    jsonResponse = urllib.request.urlopen(targetUrl + r"/addFeatures", urllib.parse.urlencode(appending_dict).encode('utf-8'))
    jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    if "error" in jsonOutput.keys():
        # the entire feature collection json is not formated correctly, not matter whether it is peaks or buffer
        returnJson["invalidJson"] += ["peaks"]
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
                    returnJson["peaksSuccess"] += [layerName]
                    skippedIndexNumber += totalPeaksNumber
                appendIndex += 1
            else:
                # if append fail, skip to the index of next layer, make skipping step = old skipped index number + current total peaks number.
                returnJson["peaksFail"] += [layerName]
                appendIndex = skippedIndexNumber + totalPeaksNumber
                skippedIndexNumber += totalPeaksNumber

def query_feature(token, sql, targetUrl):
    '''
    ------------------------
    This function is to query whether there has been data in the field map. This is to avoid appending features repeatedly. 
    Usually, the source name is used for the query.
    ------------------------
    Input parameter: 
        token: rest api token
                this token is used for this and following api calls.
        sql: string
            a similar sql where clause that is used for the query.
        targetUrl: string
                    url for the target web layer
    ------------------------
    Return:
        true or false: bool
                        indicate whether queried data exist or not.
    '''
    # sql = "Source_Name = 'N1_15_2.8_20220620_153056.csv'"
    query_dict = {"f": "json", "token": token, "where": sql}
    jsonResponse = urllib.request.urlopen(targetUrl + r"/query", urllib.parse.urlencode(query_dict).encode('utf-8'))
    jsonOutput = json.loads(jsonResponse.read(), object_pairs_hook=collections.OrderedDict)
    if len(jsonOutput["features"]) > 0:
        return True
    else:
        return False

##################################
# communicate with the front end #
##################################

@app.route('/', methods=["GET", "POST"])
def index():
    '''
    ------------------------
    This function is just an empty default route.
    ------------------------
    Input parameter: 
        None
    ------------------------
    Return:
        string for demo
    '''
    return ("Welcome to sniffer web app")

# Back end functions.
@app.route('/accessDB', methods=["GET", "POST"])
@cross_origin()
def accessDB():
    '''
    ------------------------
    This function is responding to the front end when the user first access the database after doing the authentication.
    1, It will first check if there exists a MetaDataBase.json, If not, create one for database path recording purpose.
    2, Then it will extract the database path/name sent from the front end. The json for communication has the format of
    {"DBpath": "some/path/or/name"}. Note that usually the front end just sends a database name, and that name will be 
    converted to an absolute path and saved to the MetaDataBase.json.
    3, at last connectAndUpload() is called to query database and fetch data.
    ------------------------
    Input parameter: 
        json sent from the front end, obtained by using the request.json["keyname"]
    ------------------------
    Return:
        response json: json dictionary
                        connection result that is sent to the front end for initial rendering.
                        Json format for back and front end communication: 
                        {
                            tableName1: {
                                dataName1: geojson1,
                                dataName2: geojson2 .... 
                                },
                            tableName2: {
                                dataName3: geojson3....
                            }
                        }
    '''
    if request.method == "POST":
        # create the collection of all database paths.
        if not os.path.exists('MetaDataBase.json'):
            allDBpaths = {"paths": []}
            with open('MetaDataBase.json', 'w') as f:
                f.write(json.dumps(allDBpaths, indent=4))
        path = request.json["DBpath"]
        # create absolute path from relative path.
        global localPath
        localPath = os.path.abspath(path)
        return connectAndUpload(localPath, sqliteTableList)
    return ("Connect to database")

@app.route('/delete/<table>/<dataName>', methods=["DELETE"])
@cross_origin()
def delete(table, dataName):
    '''
    ------------------------
    This function is responding to the front end delete api calls. Usually, the table name and data name will be incorporated
    into the url. Note that, only when the deleted table name starts with L (indicating LineTable), the related points data
    will also be deleted. Since at the front end, when user deletes the a path feature, the back end will delete related feature
    from the LineTable and points table. This is because the path features are not really for appending, they are just the 
    representation for the point features. So path feature controls both path geojson and related points inside the database.
    ------------------------
    Input parameter: 
        table: string
                from which table to delete features.
        dataName: string
                delete which data
    ------------------------
    Return:
        response json: json dictionary
                       a json indicate delete status, if success, return 200, 204 otherwise.
    '''
    if table[0] == "L":
        deleteFromDB(localPath, "PointsTable", dataName)
    if deleteFromDB(localPath, table, dataName):
        return ({"status": 200})
    else:
        return ({"status": 204})

@app.route('/buffer', methods=["GET", "POST"])
@cross_origin()
def buffer():
    '''
    ------------------------
    This function is responding to the front end buffer calls.
    1, it will receive the buffer list from the front end, although usually the values are 15s. Also, it will
    record the inspection type.
    2, Then it set up the response json structure, the structure is the same as the accessDB() and 
    connectAndUpload() function above. Also, it will start the sqlite database connection.
    3, Then it starts looping through every file and file name sent from the front end. 
        3.a, if it is the sniffer drone task, the csv will be cleaned and converted to the dataframe, add a
        column of points, reproject those points to utm, create points, buffer, peaks, and path, insert related
        geojson into database, and finally add to the returnjson for the front end to render.
        3.b, if it is the inficon task, the first few header rows will be discarded, csv will be cleaned, 
        and the dataframe will be created. Since inficon task does not create peaks, only buffer and path
        data are created and inserted into database.  
    4, when inserting into database, the source name for each data type, ie buffer, path, and peaks, follows a 
    format of "csvName" + "-type". For example: "200993_04_IRW00081.csv-buffer". This naming is critical for 
    the front end rendering. The points inside the point table follow the name of the path, since deleting path
    will delete the points.
    5, commit the database insersion for this loop, and update the buffer distance for the next layer.
    ------------------------
    Input parameter: 
       a form sent from the front end. 
       form structure: 
       {
        "bufferText": "bufferDistance1, bufferDistance2.....",
        "task": "S" for sniffer drone task, or "I" for inficon task,
        "csv name1": csv file1,
        "csv name2": csv file2.... 
        (The number of csv files are the same as the number of buffer distance in the bufferText)
       }
    ------------------------
    Return:
        returnedJson: json dictionary
                        buffered result that is sent to the front end for rendering.
                        Json format for back and front end communication: 
                        {
                            tableName1: {
                                dataName1: geojson1,
                                dataName2: geojson2 .... 
                                },
                            tableName2: {
                                dataName3: geojson3....
                            }
                        }
    '''
    if request.method == "POST":
        # obtain buffer distance list, and inspection type.
        bufferList = request.form['bufferText'].split(",")
        insepctionType = request.form['task']
        bufferIndex = 0
        # prepare json for front end
        returnedJson = {"BuffersTable": {}, "LinesTable": {}, "PeaksTable": {}}
        # connect to database
        connection = sqlite3.connect(localPath)
        cursor = connection.cursor()
        # obtain csv data and related info.
        for i in request.files:
            # 1, obtain csv name, buffer distance, and dataframe
            csvName = i
            bufferDistance = float(bufferList[bufferIndex])
            data = request.files.get(i)
            if insepctionType == "S":
                # 2, SnifferDrone: Ben's algorithm to create geojson.
                csvDf = pd.read_csv(data)
                cleanedDf = cp.clean_flight_log(csvName+"-path", csvDf)

                # 3, add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # 4, pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 5, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson[0], buffJson[1])
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson[0]

                # while creating peaks, also insert points into point table.
                peakJson = createSnifferPeaks(points, sr, cleanedDf, connection)
                if peakJson:
                    insertGeoJsonIntoDB(cursor, "PeaksTable", csvName+"-peaks", peakJson, "")
                returnedJson["PeaksTable"][csvName+"-peaks"] = peakJson

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson, "")
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            else:
                # 2, Inficon: Ben's algorithm to create geojson.
                while data.readline().decode() != '\r\n':
                    pass
                csvDf = pd.read_csv(data)
                cleanedDf = cp.cleanInficon(csvName+"-path", csvDf)

                # 3, add a column of points
                gt.add_points_to_df(cleanedDf)
                points = gt.series_to_multipoint(cleanedDf["points"])
                # 4, pre project the points to prepare for buffer.
                sr = gt.find_utm_zone(points[0].y, points[0].x)
                points = gt.reproject(points, sr)

                # 5, add utm points into points table.
                cleanedDf["Utmlong"] = [i.x for i in points]
                cleanedDf["Utmlat"] = [i.y for i in points]

                # 6, make the new df into sql
                cleanedDf[["Microsec", "Flight_Date", "SenseLong", "SenseLat", "CH4", "Peak", "Source_Name", "Utmlong", "Utmlat"]].to_sql(name="PointsTable", con=connection, if_exists='append', index=False)

                # 7, add into database based on types and load onto json. name: csvName-buffer, csvName-peaks.....
                buffJson = createBuff(points, bufferDistance, sr)
                insertGeoJsonIntoDB(cursor, "BuffersTable", csvName+"-buffer", buffJson[0], buffJson[1])
                returnedJson["BuffersTable"][csvName+"-buffer"] = buffJson[0]

                pathJson = createPath(cleanedDf)
                insertGeoJsonIntoDB(cursor, "LinesTable", csvName+"-path", pathJson, "")
                returnedJson["LinesTable"][csvName+"-path"] = pathJson

            bufferIndex += 1

        connection.commit()
        return (returnedJson)

    return ("This is a buffer page")

@app.route('/append', methods=["GET", "POST"])
@cross_origin()
def append():
    '''
    ------------------------
    This function is mainly calling a bunch of add_feature rest api calls base on the inspection type and layer names.
    1, the function will first record some basic information about appending, like userName and password, insepction
    type, etc.
    2, Then it will prepare a multiprocess return json, which contains 6 lists: appended point layer names, buffer 
    success list, buffer fail list, peak success list, peak fail list, invalid json list.
    3, Then it will perform similar but a little bit different operation based on the inspection type.
        3.a, Loop through every layer name, if it is the sniffer drone task, obtain three target urls from the form 
        sent from the front end. Based on the last character of the layer name, it will first check if there has been
        data with such source name in the field map. If there is no such data, go to query the database. If query 
        result is not empty, create points, buffer, and peaks esri features, append those features into feature 
        collection list (since rest api accept a feature list). For peaks features, the double buffer is actually 
        implemented here, since we have already recorded the utm coordinate for the peaks. We just buffer twice, create
        two peak circles, and append them consecutively into the feature collection.
        3.b, if it is the inficon task, we repeat the similar process but with only buffer and points. 
        3.c, with either inspection type, the bool appendedAtLeastOnce is to quickly tell whether there has been data 
        being appended even once.
    4, At last, we set up multiprocess api calls to save time. 
    ------------------------
    Input parameter: 
        a form sent from the front end. 
        form structure for sniffer drone task:
        {
            "userName": xxxxx,
            "passWord": xxxxx,
            "sourceLayers": "csvlayer1, csvLayer2, csvLayer3...." (follow the format of csvname + '-type'),
            "task": "S",
            "bufferUrl": xxxxxx,
            "peaksUrl": xxxxx,
            "pointsUrl": xxxx
        }
        form structure for inficon task:
        {
            "userName": xxxxx,
            "passWord": xxxxx,
            "sourceLayers": "csvlayer1, csvLayer2, csvLayer3...." (follow the format of csvname + '-type'),
            "task": "I",
            "inficonPoints": xxxxxx,
            "inficonBuffer": xxxxx
        }
    ------------------------
    Return:
        returnJson: a copy of multiprocess json dictionary. if not bing copied, it will not be serilized.
                    {
                        "task": "SnifferDrone" or "Inficon",
                        six lists that indicate the appending status.
                    }
    '''
    if request.method == "POST":
        # start the database.
        connection = sqlite3.connect(localPath)
        cursor = connection.cursor()
        # authentication
        userName = request.form["userName"]
        passWord = request.form["passWord"]
        token = get_token(userName, passWord)
        # get append function information
        sourceLayers = request.form["sourceLayers"].split(",")
        inspectionType = request.form["task"]
        # create return reponse for multiprocess appending.
        manager = mp.Manager()
        returnJson = manager.dict()
        returnJson["pointsAppended"] = []
        returnJson["bufferSuccess"] = []
        returnJson["bufferFail"] = []
        returnJson["peaksSuccess"] = []
        returnJson["peaksFail"] = []
        returnJson["invalidJson"] = []
        # prepare point and buffer feature list for appending, two inspections have both those two feature types.
        pointFeatures, bufferFeatures = [], []
        # a bool to check if there is at least one layer that is appendable
        appendedAtLeastOnce = False
        # append operation based on inspection type
        if inspectionType == "S":
            returnJson["task"] = "SnifferDrone"
            # extra peak features collector
            peaksFeatures = []
            # Dictionary to record the number of peaks for each peak layer. This is a work around for
            # retrieving the peaks appending response. Each row in AGOL layer is an appending
            # feature. Each peak layer created from one csv contains even number of rows (buffered twice)
            peaksDict = {}
            # get urls
            bufferUrl = request.form["bufferUrl"]
            peaksUrl = request.form["peaksUrl"]
            pointsUrl = request.form["pointsUrl"]

            for i in sourceLayers:
                sql = "Source_Name = '" + i.split("-")[0] + "'"
                if i[-1] == "r":
                    if not query_feature(token, sql, bufferUrl):
                        queryOpertaion = cursor.execute("SELECT Source_name, EsriGeometry FROM BuffersTable WHERE Source_name == '" + i + "'").fetchall()
                        if len(queryOpertaion) > 0:
                            query = queryOpertaion[0]
                            esriGeometry = json.loads(query[1].replace("'", '"'))
                            uploadStruct = {
                                "attributes" : {"Source_Name": query[0].split("-")[0]},
                                "geometry" : esriGeometry
                            }
                            bufferFeatures.append(uploadStruct)
                            appendedAtLeastOnce = True

                elif i[-1] == "s":
                    if not query_feature(token, sql, peaksUrl):
                        sourceName = i.replace("peaks", "path")
                        orig_id = 1
                        query= cursor.execute("SELECT Flight_date, Senselat, Senselong, CH4, Source_name, Utmlong, Utmlat FROM PointsTable WHERE Source_name == '" + sourceName + "' AND Peak == 1").fetchall()
                        if len(query) > 0:
                            # record the total number of features appendable to the paak layer.
                            peaksDict[i.split("-")[0]] = len(query)*2
                            for j in query:
                                # obtain center coordinate
                                peakCenter = Point(j[5], j[6])
                                # create outer buffer esri geometry json.
                                outerCircle = {"attributes" : {
                                    "Flight_Date": j[0],
                                    "SenseLat": j[1],
                                    "SenseLong": j[2],
                                    "CH4": j[3],
                                    "Source_Name" : j[4].split("-")[0],
                                    "BUFF_DIST": 13.57884,
                                    "ORIG_FID": orig_id
                                }}
                                outerBuffer = mapping(peakCenter.buffer(13.57884, resolution=bufferResolution))
                                esriOuterBuffer = json.loads(toEsriGeometry(outerBuffer))
                                outerCircle["geometry"] = esriOuterBuffer
                                peaksFeatures.append(outerCircle)
                                # create inner buffer esri geometry json.
                                innerCircle = {"attributes" : {
                                    "Flight_Date": j[0],
                                    "SenseLat": j[1],
                                    "SenseLong": j[2],
                                    "CH4": j[3],
                                    "Source_Name" : j[4].split("-")[0],
                                    "BUFF_DIST": 5.876544,
                                    "ORIG_FID": orig_id
                                }}
                                innerBuffer = mapping(peakCenter.buffer(5.876544, resolution=bufferResolution))
                                esriInnerBuffer = json.loads(toEsriGeometry(innerBuffer))
                                innerCircle["geometry"] = esriInnerBuffer
                                peaksFeatures.append(innerCircle)
                                orig_id += 1
                            appendedAtLeastOnce = True

                else:
                    if not query_feature(token, sql, pointsUrl):
                        query = cursor.execute("SELECT Flight_date, Senselat, Senselong, CH4, Source_name, Utmlong, Utmlat FROM PointsTable WHERE Source_name == '" + i + "'").fetchall()
                        if len(query) > 0:
                            for j in query:
                                esriPoint = {"attributes" : {
                                                "Flight_Date": j[0],
                                                "SenseLat": j[1],
                                                "SenseLong": j[2],
                                                "CH4": j[3],
                                                "Source_Name" : j[4].split("-")[0]
                                                },
                                                "geometry" :
                                                {
                                                    "x" : j[5],
                                                    "y" : j[6]
                                                }}
                                pointFeatures.append(esriPoint)
                            returnJson["pointsAppended"] += [i]
                            appendedAtLeastOnce = True

            if appendedAtLeastOnce:
                # Multiprocess add. parameters: token, features, targetUrl, urlCheckDict, returnJson, includeReponse
                # add_feature(token, bufferFeatures, bufferUrl)
                bufferAppend = mp.Process(target=add_buffer_features, args=[token, bufferFeatures, bufferUrl, returnJson])
                # add_feature(token, pointFeatures, pointsUrl)
                pointsAppend = mp.Process(target=add_point_features, args=[token, pointFeatures, pointsUrl])
                # add_feature(token, peaksFeatures, peaksUrl)
                peaksAppend = mp.Process(target=add_peak_features, args=[token, peaksFeatures, peaksUrl, returnJson, peaksDict])
                bufferAppend.start()
                pointsAppend.start()
                peaksAppend.start()
                bufferAppend.join()
                pointsAppend.join()
                peaksAppend.join()

        else:
            returnJson["task"] = "Inficon"
            inficonPointsUrl = request.form["inficonPoints"]
            inficonBufferUrl = request.form["inficonBuffer"]

            for i in sourceLayers:
                sql = "Source_Name = '" + i.split("-")[0] + "'"
                if i[-1] == "r":
                    if not query_feature(token, sql, inficonBufferUrl):
                        queryOperation = cursor.execute("SELECT Source_name, EsriGeometry FROM BuffersTable WHERE Source_name == '" + i + "'").fetchall()
                        if len(queryOperation) > 0:
                            query = queryOperation[0]
                            esriGeometry = json.loads(query[1].replace("'", '"'))
                            uploadStruct = {
                                "attributes" : {"Source_Name": query[0].split("-")[0]},
                                "geometry" : esriGeometry
                            }
                            bufferFeatures.append(uploadStruct)
                            appendedAtLeastOnce = True

                else:
                    if not query_feature(token, sql, inficonPointsUrl):
                        query = cursor.execute("SELECT Flight_date, Senselat, Senselong, CH4, Source_name, Utmlong, Utmlat FROM PointsTable WHERE Source_name == '" + i + "'").fetchall()
                        if len(query) > 0:
                            for j in query:
                                esriPoint = {"attributes" : {
                                                "Inspection_Date": j[0],
                                                "Inspection_Time": j[0].split(" ")[1],
                                                "Lat": j[1],
                                                "Long": j[2],
                                                "CH4": j[3],
                                                "Source_Name" : j[4].split("-")[0]
                                                },
                                                "geometry" :
                                                {
                                                    "x" : j[5],
                                                    "y" : j[6]
                                                }}
                                pointFeatures.append(esriPoint)
                            returnJson["pointsAppended"] += [i]
                            appendedAtLeastOnce = True

            if appendedAtLeastOnce:
                bufferInficonAppend = mp.Process(target=add_buffer_features, args=[token, bufferFeatures, inficonBufferUrl, returnJson])
                pointInficonAppend = mp.Process(target=add_point_features, args=[token, pointFeatures, inficonPointsUrl])
                bufferInficonAppend.start()
                pointInficonAppend.start()
                bufferInficonAppend.join()
                pointInficonAppend.join()

        return (returnJson.copy())

    return ("This is for appending function")

if __name__ == '__main__':
    app.run()