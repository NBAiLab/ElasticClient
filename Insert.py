import sys
import os
import xmlHandler
import elasticsearchHandler
import argparse
import json
import glob



from elasticsearchHandler import elasticSearchHandler
from sandboxLogger import SandboxLogger

from xmlHandler import xmlHandler
s=SandboxLogger("myLogger","logging_config.config")

globalElasticHandler = None
def insertMytest(server,port):
    global globalElasticHandler

    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler
    data = {}
    data['Filnavn'] = ['Testing1and2','Testing1and3']
    data['Dato'] = "01012011"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")

    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)

    data = {}
    data['Filnavn'] = ['Testing1and2','Testing1and3']
    data['Dato'] = "01022011"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)
    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01022011 184500"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)
    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01022011 194500"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)

    elasticHandler.commit("mytest")




def createDataStructure(server,port,idx, MappingsFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)

        #elasticHandler = elasticSearchHandler(server,port)
        settingsfile = open(MappingsFile, "r")
        settings = settingsfile.read()
        elasticHandler.createIndex(idx, settings)

def insertEmneData(server, port, CSVEmneFil):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
    cnt=0
    with open(CSVEmneFil) as fp:
        for line in fp:

            filnavn, emne1, emne2, emne3, emne4, emne5 = line.replace("'","").split(":")
            data = {}
            data['Filnavn'] = filnavn
            data['Emne1'] = emne1
            data['Emne2'] = emne2
            data['Emne3'] = emne3
            data['Emne4'] = emne4
            data['Emne5'] = emne5
            data['Konfidens'] = float("1.0")
            json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
            elasticHandler.insert("emner", "Emner", json_data)
            cnt+=1
            if cnt == 1000:
                sys.stdout.write('@')
                sys.stdout.flush()
                cnt=0
                elasticHandler.commit("emner")
            #print(json_data)
    elasticHandler.commit("emner")



def insertVideoShotsfile(server, port, medieType, videoshotsFile):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
        # print("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    handler = xmlHandler(inputXmlFile=videoshotsFile, rootNodeName="Scener")
    relativeFilename = videoshotsFile.split("/")[-1]
    relativeFilenameNoEnding = relativeFilename.split(".")[0]
    year = relativeFilename.split("_")[2]
    month = relativeFilename.split("_")[3]
    day = relativeFilename.split("_")[4]
    hour = relativeFilename.split("_")[5]
    min = relativeFilename.split("_")[6].split(".")[0]
    correctDate = day + month + year + " " + hour + min + "00"
    res = handler.findAllNodes("Shot")
    cnt=1
    for i in res:
        data = {}
        data['Filnavn'] = relativeFilenameNoEnding
        data['Dato'] = correctDate
        data['Shotnumber'] = int(cnt)
        data['Shotstart'] = float(i.attrib['Start_tid'].rstrip())
        data['Shotstop'] = float(i.attrib['Stopp_tid'].rstrip())
        cnt+=1
        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
        #print(json_data)
        elasticHandler.insert("videoshot", "Videoshot", json_data)
    elasticHandler.commit("videoshot")


def insertVideoExplicitfile(server, port, medieType, videoexplicitsFile):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
        # print("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    handler = xmlHandler(inputXmlFile=videoexplicitsFile, rootNodeName="Snusk")
    relativeFilename = videoexplicitsFile.split("/")[-1]
    relativeFilenameNoEnding = relativeFilename.split(".")[0]
    year = relativeFilename.split("_")[2]
    month = relativeFilename.split("_")[3]
    day = relativeFilename.split("_")[4]
    hour = relativeFilename.split("_")[5]
    min = relativeFilename.split("_")[6].split(".")[0]
    correctDate = day + month + year + " " + hour + min + "00"
    res = handler.findAllNodes("Bilde")

    for i in res:
        data = {}
        data['Filnavn'] = relativeFilenameNoEnding
        data['Dato'] = correctDate
        data['Frame'] = float(i.attrib['Bilde_tid'].rstrip())
        data['Sannsynlighet'] = i.text.strip()

        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
        #print(json_data)
        elasticHandler.insert("videoexplicit", "Videoexplicit", json_data)

    elasticHandler.commit("videoexplicit")


def insertLabelfile(server,port,medieType,labelsFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
            #print("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler=elasticHandler

        handler = xmlHandler(inputXmlFile=labelsFile, rootNodeName="Labels")

        relativeFilename = labelsFile.split("/")[-1]
        relativeFilenameNoEnding = relativeFilename.split(".")[0]
        year =relativeFilename.split("_")[2]
        month = relativeFilename.split("_")[3]
        day = relativeFilename.split("_")[4]
        hour = relativeFilename.split("_")[5]
        min = relativeFilename.split("_")[6].split(".")[0]
        correctDate = day + month + year + " " + hour + min + "00"
        res = handler.findAllNodes("Label")
        #print("correctDate:" + correctDate)

        for i in res:
            #print("Label: " + i.text.rstrip())
            label= i.text.rstrip()
            data = {}
            data['Filnavn'] = relativeFilenameNoEnding
            data['Dato'] = correctDate
            res2=handler.findInSub(i,"Kategori")
            kategori=""
            if len(res2) > 0:
                kategori= str(res2[0].text.rstrip())
            else:
                kategori="NULL"
            res2 = handler.findInSub(i, "Segment")
            segment = ""
            if len(res2) > 0:
                segment = str(res2[0].text.rstrip())
            else:
                segment = "NULL"

            segmentNumber = int(segment.split(" ")[0])
            segmentStart = float(segment.split(" ")[1][:-1])
            segmentStop = float(segment.split(" ")[3][:-1])
            #print("segmentstart:" + segmentStart + " segmentstop:" + segmentStop)
            res2 = handler.findInSub(i, "Konfidens")
            konfidens = 0
            if len(res2) > 0:
                konfidens = float(str(res2[0].text.rstrip()))
            else:
                konfidens = float("0")
            data['Videolabel'] = label
            data['Videolabelcategory'] = kategori
            data['Segmentnumber'] = segmentNumber
            data['Segmentstart'] = segmentStart
            data['Segmentstop'] = segmentStop
            data['Konfidens'] = konfidens
            json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
            elasticHandler.insert("videoannotation", "Videoannotation", json_data)
        elasticHandler.commit("videoannotation")
        res = handler.findAllNodes("shotAnnotations/shotLabel")

        for i in res:
            label = i.text.strip()

            shotlabelcategorynode = handler.findInSub(i, "shotLabelCategory")
            if len(shotlabelcategorynode) > 0:
                #print(str(shotlabelcategorynode[0].text.rstrip()))
                shotlabelkategori=shotlabelcategorynode[0].text.strip()
                for j in shotlabelcategorynode:
                    shotlabelkategoriSegmentNodes = handler.findInSub(j, "Segment")
                    shotlabelkategoriKonfidensNodes = handler.findInSub(j, "Konfidens")
                    cnt=0
                    while cnt < len(shotlabelkategoriSegmentNodes):
                        #print(shotlabelkategoriSegmentNodes[cnt].text.rstrip())
                        #print(shotlabelkategoriKonfidensNodes[cnt].text.rstrip())
                        data = {}
                        data['Filnavn'] = relativeFilenameNoEnding
                        data['Dato'] = correctDate
                        data['Shotlabel'] = label
                        data['Shotlabelcategory'] = shotlabelkategori
                        segmentInfo=shotlabelkategoriSegmentNodes[cnt].text.strip()
                        segmentNumber = segmentInfo.split(" ")[0]
                        segmentStart = segmentInfo.split(" ")[1][:-1]
                        segmentStop = segmentInfo.split(" ")[3][:-1]
                        data['Segmentinfo']=segmentInfo
                        data['Segmentnumber'] = int(segmentNumber)
                        data['Segmentstart'] = float(segmentStart)
                        data['Segmentstop'] = float(segmentStop)
                        data['Konfidens'] = float(shotlabelkategoriKonfidensNodes[cnt].text.rstrip())
                        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                        #print(json_data)
                        elasticHandler.insert("shotannotation", "Shotannotation", json_data)
                        cnt+=1
        elasticHandler.commit("shotannotation")
        res = handler.findAllNodes("frameAnnotations/frameLabel")

        for i in res:
            label = i.text.strip()

            framelabelcategorynode = handler.findInSub(i, "categoryLabel")
            if len(framelabelcategorynode) > 0:
                # print(str(shotlabelcategorynode[0].text.rstrip()))
                framelabel = framelabelcategorynode[0].text.strip()
                for j in framelabelcategorynode:
                    frameFirstOffset = handler.findInSub(j, "FirstFrameTimeOffset")
                    frameOffsetKonfidens = handler.findInSub(j, "FrameOffsetConfidence")
                    cnt = 0
                    while cnt < len(frameFirstOffset):
                        # print(shotlabelkategoriSegmentNodes[cnt].text.rstrip())
                        # print(shotlabelkategoriKonfidensNodes[cnt].text.rstrip())
                        data = {}
                        data['Filnavn'] = relativeFilenameNoEnding
                        data['Dato'] = correctDate
                        data['Framelabel'] = label
                        data['Framelabelcategory'] = framelabel
                        data['Firstframeoffset'] = float(frameFirstOffset[cnt].text.strip())
                        data['Konfidens'] = float(frameOffsetKonfidens[cnt].text.strip())


                        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                        #print(json_data)
                        elasticHandler.insert("frameannotation", "Frameannotation", json_data)
                        cnt += 1
        elasticHandler.commit("frameannotation")


def insertTimecodesFile(server,port,medieType,timecodesFile):
        global globalElasticHandler

        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        #elasticHandler = elasticSearchHandler(server, port)
        handler = xmlHandler(inputXmlFile=timecodesFile, rootNodeName="Ordene")
        relativeFilename = timecodesFile.split("/")[-1]
        relativeFilenameNoEnding = relativeFilename.split(".")[0]

        res = handler.findAllNodes("Ord")

        if (medieType == "Radio"):
            dato = relativeFilenameNoEnding.rstrip().split("_")[1].replace("r","R").replace("p","P").replace("nRk","nrk")
            year = dato[3:7]
            month = dato[7:9].zfill(2)
            day = dato[9:11].zfill(2)
            hour = dato[11:13].zfill(2)
            min = dato[13:15].zfill(2)
            sec = "00"
        elif (medieType == "Tv"):
            # print("tv" + dato)
            year = relativeFilenameNoEnding.split("_")[2]
            month = relativeFilenameNoEnding.split("_")[3].zfill(2)
            day = relativeFilenameNoEnding.split("_")[4].zfill(2)
            hour = relativeFilenameNoEnding.split("_")[5].zfill(2)
            min = relativeFilenameNoEnding.split("_")[6].zfill(2)
            sec = "00"
        else:
            dato = relativeFilenameNoEnding.rstrip().split("_")[3]
            year = dato[:4]
            month = dato[:6][-2:].zfill(2)
            day = dato[-2:].zfill(2)
            hour = min = sec = ""

        correctDate = str(day + month + year + " " + hour + min + sec).rstrip()




        #print(relativeFilenameNoEnding + " YYZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ " + correctDate)
        for i in res:
            data = {}
            data['Filnavn'] = relativeFilenameNoEnding
            data['Medietype'] = medieType
            data['Dato'] = correctDate
            data['Ord'] = i.text.rstrip()
            data['Start_tid'] = float(i.attrib['Start_tid'].rstrip())
            data['Stopp_tid'] = float(i.attrib['Stopp_tid'].rstrip())
            json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
            elasticHandler.insert("timecodes", "Timecodes", json_data)
        elasticHandler.commit("timecodes")

def insertLokasjonLokasjonFile(server,port,medieType,personPersonFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        #elasticHandler = elasticSearchHandler(server, port)
        relativeFilename = personPersonFile.split("/")[-1]
        relativeFilenameNoEnding = relativeFilename.split(".")[0]
        handler = xmlHandler(inputXmlFile=personPersonFile, rootNodeName="location2Location")

        res = handler.findAllNodes("locationLocation")
        for i in res:
                data = {}

                data['Medietype'] = medieType
                if (medieType == "Radio"):
                    data['Filnavn'] = relativeFilenameNoEnding.replace("r", "R").replace("p", "P").replace("nRk","nrk")
                    dato = i.attrib['fileName'].rstrip().split("_")[1]
                    year = dato[3:7]
                    month = dato[7:9].zfill(2)
                    day = dato[9:11].zfill(2)
                    hour = dato[11:13].zfill(2)
                    min = dato[13:15].zfill(2)
                    sec="00"
                elif (medieType == "Tv"):
                    data['Filnavn'] = relativeFilenameNoEnding
                    #print("tv" + dato)
                    year = i.attrib['fileName'].split("_")[2]
                    month = i.attrib['fileName'].split("_")[3].zfill(2)
                    day = i.attrib['fileName'].split("_")[4].zfill(2)
                    hour = i.attrib['fileName'].split("_")[5].zfill(2)
                    min = i.attrib['fileName'].split("_")[6].zfill(2)
                    sec = "00"
                else:
                    data['Filnavn'] = relativeFilenameNoEnding
                    dato = i.attrib['fileName'].rstrip().split("_")[3]
                    year = dato[:4]
                    month = dato[:6][-2:].zfill(2)
                    day = dato[-2:].zfill(2)
                    hour=min=sec=""

                correctDate = str(day + month + year + " " + hour + min + sec).rstrip()
                # print("zzzz"+ correctDate + "    " + i.attrib['fileName'].rstrip() + "    " + medieType)
                data["Dato"] = correctDate
                data['Navn'] = i.attrib['locationName'].rstrip()
                data['Relatert'] = i.text.rstrip()
                if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" or i.attrib['importance'] == "None":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == "" or i.attrib['lon'] == "" or i.attrib['importance'] == "":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == " " or i.attrib['lon'] == " " or i.attrib['importance'] == " ":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] is not None and i.attrib['lon'] is not None and i.attrib['importance'] is not None:
                    # print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
                    data["Lokasjon"] = str(i.attrib['lat'].rstrip()) + "," + str(i.attrib['lon'].rstrip())
                    data["Konfidens"] = float(i.attrib['importance'])
                else:
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                #print(data)
                json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                elasticHandler.insert("lokasjonlokasjon","LokasjonLokasjon",json_data)
        elasticHandler.commit("lokasjonlokasjon")


def insertOrganisasjonLokasjonFile(server,port,medieType,personPersonFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        # elasticHandler = elasticSearchHandler(server, port)

        handler = xmlHandler(inputXmlFile=personPersonFile, rootNodeName="organisation2Location")

        res = handler.findAllNodes("organisationLocation")
        for i in res:
                data = {}


                data['Medietype'] = medieType
                if (medieType == "Radio"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0] .replace("r", "R").replace("p", "P").replace("nRk","nrk")

                    dato = i.attrib['fileName'].rstrip().split("_")[1]
                    year = dato[3:7]
                    month = dato[7:9].zfill(2)
                    day = dato[9:11].zfill(2)
                    hour = dato[11:13].zfill(2)
                    min = dato[13:15].zfill(2)
                    sec = "00"
                elif (medieType == "Tv"):
                    #print("tv" + dato)
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    year = i.attrib['fileName'].split("_")[2]
                    month = i.attrib['fileName'].split("_")[3].zfill(2)
                    day = i.attrib['fileName'].split("_")[4].zfill(2)
                    hour = i.attrib['fileName'].split("_")[5].zfill(2)
                    min = i.attrib['fileName'].split("_")[6].zfill(2)
                    sec = "00"
                else:
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    dato = i.attrib['fileName'].rstrip().split("_")[3]
                    year = dato[:4]
                    month = dato[:6][-2:].zfill(2)
                    day = dato[-2:].zfill(2)
                    hour=min=sec=""

                correctDate = str(day + month + year + " " + hour + min + sec).rstrip()
                data["Dato"] = correctDate
                data['Navn'] = i.attrib['organisationName'].rstrip()
                data['Relatert'] = i.text.rstrip()

                if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" or i.attrib['importance'] == "None":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == "" or i.attrib['lon'] == "" or i.attrib['importance'] == "":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == " " or i.attrib['lon'] == " " or i.attrib['importance'] == " ":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] is not None and i.attrib['lon'] is not None and i.attrib['importance'] is not None:
                    # print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
                    data["Lokasjon"] = str(i.attrib['lat'].rstrip()) + "," + str(i.attrib['lon'].rstrip())
                    data["Konfidens"] = float(i.attrib['importance'])

                else:
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                #print(data)
                json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                elasticHandler.insert("organisasjonlokasjon","OrganisasjonLokasjon",json_data)
        elasticHandler.commit("organisasjonlokasjon")

def insertPersonLokasjonFile(server,port,medieType,personPersonFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        # elasticHandler = elasticSearchHandler(server, port)
        #elasticHandler = elasticSearchHandler(server, port)
        handler = xmlHandler(inputXmlFile=personPersonFile, rootNodeName="person2location")

        res = handler.findAllNodes("personLocation")
        for i in res:
                data = {}

                if (medieType == "Radio"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0].replace("r", "R").replace("p", "P").replace("nRk","nrk")
                    dato = i.attrib['fileName'].rstrip().split("_")[1]
                    year = dato[3:7]
                    month = dato[7:9].zfill(2)
                    day = dato[9:11].zfill(2)
                    hour = dato[11:13].zfill(2)
                    min = dato[13:15].zfill(2)
                    sec = "00"
                elif (medieType == "Tv"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    #print("tv" + dato)
                    year = i.attrib['fileName'].split("_")[2]
                    month = i.attrib['fileName'].split("_")[3].zfill(2)
                    day = i.attrib['fileName'].split("_")[4].zfill(2)
                    hour = i.attrib['fileName'].split("_")[5].zfill(2)
                    min = i.attrib['fileName'].split("_")[6].zfill(2)
                    sec = "00"
                else:
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    dato = i.attrib['fileName'].rstrip().split("_")[3]
                    year = dato[:4]
                    month = dato[:6][-2:].zfill(2)
                    day = dato[-2:].zfill(2)
                    hour=min=sec=""

                correctDate = str(day + month + year + " " + hour + min + sec).rstrip()

                data['Medietype'] = medieType


                data["Dato"] = correctDate
                data['Navn'] = i.attrib['personName'].rstrip()
                data['Relatert'] = i.text.rstrip()

                if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" or i.attrib['importance'] == "None":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == "" or i.attrib['lon'] == "" or i.attrib['importance'] == "":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] == " " or i.attrib['lon'] == " " or i.attrib['importance'] == " ":
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)
                elif i.attrib['lat'] is not None and i.attrib['lon'] is not None and i.attrib['importance'] is not None:
                    # print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
                    data["Lokasjon"] = str(i.attrib['lat'].rstrip()) + "," + str(i.attrib['lon'].rstrip())
                    data["Konfidens"] = float(i.attrib['importance'])
                else:
                    data["Lokasjon"] = "0,0"
                    data["Konfidens"] = float(0)

                json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                #print(json_data)
                elasticHandler.insert("personlokasjon","PersonLokasjon",json_data)
        elasticHandler.commit("personlokasjon")


def insertAvisLokasjonFile(server, port, medieType, alocFile):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

        # elasticHandler = elasticSearchHandler(server, port)
    handler = xmlHandler(inputXmlFile=alocFile, rootNodeName="entities")
    filenameNoEnding=alocFile.rstrip().split(".")[0]
    res = handler.findAllNodes("entity")
    for i in res:
        data = {}

        data['Medietype'] = medieType
        data['Filnavn'] =filenameNoEnding
        correctDate = "20110101 140000"
        data["Dato"] = correctDate
        data['Navn'] = i.text.rstrip()
        data['Relatert'] = i.attrib['lokasjon'].rstrip()

        if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] == "" or i.attrib['lon'] == "" :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] == " " or i.attrib['lon'] == " " :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] is not None and i.attrib['lon'] is not None :
            # print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
            data["Lokasjon"] = str(i.attrib['lat'].rstrip()) + "," + str(i.attrib['lon'].rstrip())
            data["Konfidens"] = float("1.0")
        else:
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
        elasticHandler.insert("organisasjonlokasjon", "OrganisasjonLokasjon", json_data)

        data = {}
        data['Medietype'] = medieType
        data['Filnavn'] = filenameNoEnding
        data['Navn'] = i.attrib['lokasjon'].rstrip()

        if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] == "" or i.attrib['lon'] == "" :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] == " " or i.attrib['lon'] == " " :
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)
        elif i.attrib['lat'] is not None and i.attrib['lon'] is not None :
            # print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
            data["Lokasjon"] = str(i.attrib['lat'].rstrip()) + "," + str(i.attrib['lon'].rstrip())
            data["Konfidens"] = float("1.0")
        else:
            data["Lokasjon"] = "0,0"
            data["Konfidens"] = float(0)

        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
        # sys.stdout.buffer.write(json_data)

        elasticHandler.insert("lokasjon", "Lokasjon", json_data)

    elasticHandler.commit("lokasjon")
    elasticHandler.commit("organisasjonlokasjon")


def insertPersonOrganisasjonFile(server,port,medieType,personPersonFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

            # elasticHandler = elasticSearchHandler(server, port)
        handler = xmlHandler(inputXmlFile=personPersonFile, rootNodeName="person2Organisation")

        res = handler.findAllNodes("personOrganisation")
        for i in res:
                data = {}

                data['Medietype'] = medieType

                if (medieType == "Radio"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0].replace("r", "R").replace("p", "P").replace("nRk","nrk")
                    dato = i.attrib['fileName'].rstrip().split("_")[1]
                    year = dato[3:7]
                    month = dato[7:9].zfill(2)
                    day = dato[9:11].zfill(2)
                    hour = dato[11:13].zfill(2)
                    min = dato[13:15].zfill(2)
                    sec = "00"
                elif (medieType == "Tv"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    #print("tv" + dato)
                    year = i.attrib['fileName'].split("_")[2]
                    month = i.attrib['fileName'].split("_")[3].zfill(2)
                    day = i.attrib['fileName'].split("_")[4].zfill(2)
                    hour = i.attrib['fileName'].split("_")[5].zfill(2)
                    min = i.attrib['fileName'].split("_")[6].zfill(2)
                    sec = "00"
                else:
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    dato = i.attrib['fileName'].rstrip().split("_")[3]
                    year = dato[:4]
                    month = dato[:6][-2:].zfill(2)
                    day = dato[-2:].zfill(2)
                    hour=min=sec=""

                correctDate = str(day + month + year + " " + hour + min + sec).rstrip()
                data["Dato"] = correctDate
                data['Navn'] = i.attrib['personName'].rstrip()
                data['Relatert'] = i.text.rstrip()
                json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                elasticHandler.insert("personorganisasjon","PersonOrganisasjon",json_data)
        elasticHandler.commit("personorganisasjon")


def insertPersonPersonFile(server,port,medieType,personPersonFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        relativeFilename = personPersonFile.split("/")[-1]

        # elasticHandler = elasticSearchHandler(server, port)
        handler = xmlHandler(inputXmlFile=personPersonFile, rootNodeName="person2Person")

        res = handler.findAllNodes("personPerson")
        for i in res:
                data = {}

                data['Medietype'] = medieType
                #print("ssss" + i.attrib['fileName'])
                dato=""
                if (medieType == "Radio"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0].replace("r", "R").replace("p", "P").replace("nRk","nrk")
                    dato = i.attrib['fileName'].rstrip().split("_")[1]
                    year = dato[3:7]
                    month = dato[7:9].zfill(2)
                    day = dato[9:11].zfill(2)
                    hour = dato[11:13].zfill(2)
                    min = dato[13:15].zfill(2)
                    sec = "00"
                elif (medieType == "Tv"):
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    #print("tv" + dato)
                    year = i.attrib['fileName'].split("_")[2]
                    month = i.attrib['fileName'].split("_")[3].zfill(2)
                    day = i.attrib['fileName'].split("_")[4].zfill(2)
                    hour = i.attrib['fileName'].split("_")[5].zfill(2)
                    min = i.attrib['fileName'].split("_")[6].zfill(2)
                    sec = "00"
                else:
                    data['Filnavn'] = i.attrib['fileName'].rstrip().split(".")[0]
                    dato = i.attrib['fileName'].rstrip().split("_")[3]
                    year = dato[:4]
                    month = dato[:6][-2:].zfill(2)
                    day = dato[-2:].zfill(2)
                    hour=min=sec=""



                correctDate = str(day + month + year + " " + hour + min + sec).rstrip()
                #print(correctDate)
                data["Dato"] = correctDate
                data['Navn'] = i.text.rstrip()
                data['Relatert'] = i.attrib['personName'].rstrip()
                json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                elasticHandler.insert("personperson","PersonPerson",json_data)
        elasticHandler.commit("personperson")

def insertEntityFile(server,port,medieType,dato,entityFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        #elasticHandler = elasticSearchHandler(server, port)
        relativeFilename = entityFile.split("/")[-1]
        relativeFilenameNoEnding = relativeFilename.split(".")[0]
        handler = xmlHandler(inputXmlFile=entityFile, rootNodeName="entities")

        res = handler.findAllNodes("entity")
        for i in res:
                data = {}
                entity_type = i.attrib['entity_type'].rstrip()
                #data['Filnavn'] = entityFile.rsplit('.', 1)[0] + ".txt"
                #print("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"+entityFile.rsplit('.', 1)[0] + ".txt")
                if medieType == "Radio":
                    data['Filnavn'] = relativeFilenameNoEnding.replace("r", "R").replace("p", "P").replace("nRk","nrk")
                else:
                    data['Filnavn'] = relativeFilenameNoEnding
                data['Medietype'] = medieType
                data["Dato"] = dato

                data["Lengde"] = int(i.attrib['entity_length'].rstrip())
                pos=i.attrib['entity_positions'].rstrip().replace(" ", "")
                pos=pos.replace("[", "")
                pos = pos.replace("]", "")

                noElements=len(pos.split(","))
                strArr=pos.split(",")
                #print(str(noElements) + str(strArr))
                x = [0] * noElements
                count=0
                while count < noElements:
                        if len(strArr[count]) > 0:
                            x[count]=int(strArr[count])
                        count+=1
                numbers = sum(c.isdigit() for c in pos)
                data["Posisjon"] = x
                toInsert=True
                if (numbers == 0):
                    data["Posisjon"] = ""
                    toInsert = False
                if i.text is None or i.text == "" or i.text == " ":
                    toInsert = False
                else:
                    data['Navn'] = i.text.rstrip()

                if entity_type == "Person":
                        s.debug("Inserting Person")

                        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                        # sys.stdout.buffer.write(json_data)
                        if (toInsert == True):
                            elasticHandler.insert("person", entity_type, json_data)

                if entity_type == "Lokasjon":
                        s.debug("Inserting Lokasjon")

                        if i.attrib['lat'] == "None" or i.attrib['lon'] == "None" or i.attrib['importance'] == "None":
                            data["Lokasjon"] = "0,0"
                            data["Konfidens"] = float(0)
                        elif i.attrib['lat'] == "" or i.attrib['lon'] == "" or i.attrib['importance'] == "":
                            data["Lokasjon"] = "0,0"
                            data["Konfidens"] = float(0)
                        elif i.attrib['lat'] == " " or i.attrib['lon'] == " " or i.attrib['importance'] == " ":
                            data["Lokasjon"] = "0,0"
                            data["Konfidens"] = float(0)
                        elif i.attrib['lat'] is not None and i.attrib['lon'] is not None and i.attrib['importance'] is not None:
                            #print(i.attrib['lat'] + ":" + i.attrib['lon'] + ":" + i.attrib['importance'])
                            data["Lokasjon"]=str(i.attrib['lat'].rstrip()) + ","+ str(i.attrib['lon'].rstrip())
                            data["Konfidens"] = float(i.attrib['importance'])

                        else:
                            data["Lokasjon"] = "0,0"
                            data["Konfidens"] = float(0)

                        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                        #sys.stdout.buffer.write(json_data)
                        if (toInsert == True):
                            elasticHandler.insert("lokasjon", entity_type, json_data)
                if entity_type == "Organisasjon":
                        s.debug("Inserting Organisasjon")

                        json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
                        # sys.stdout.buffer.write(json_data)
                        if (toInsert == True):
                            elasticHandler.insert("organisasjon", entity_type, json_data)
        elasticHandler.commit("organisasjon")
        elasticHandler.commit("lokasjon")
        elasticHandler.commit("person")



def createDataStructures(server,port,idx, MappingsFile,xmlFile):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        #elasticHandler = elasticSearchHandler(server,port)
        settingsfile = open(MappingsFile, "r")
        settings = settingsfile.read()

        elasticHandler.createIndex(idx, settings)


def dropIndex(server,port,idx):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler
        #print("drop")
        #elasticHandler = elasticSearchHandler(server, port)
        elasticHandler.dropIndex(idx)
        #print("drop")

def printIndexContent(server,port,idx):
        global globalElasticHandler
        elasticHandler = None
        if globalElasticHandler != None:
            elasticHandler = globalElasticHandler
        else:
            elasticHandler = elasticSearchHandler(server, port)
            globalElasticHandler = elasticHandler

        #elasticHandler = elasticSearchHandler(server, port)
        res=elasticHandler.search(idx, {"query": {"match_all": {}},"from":1,"size":10})
        if (res != None):
                for row in res["hits"]["hits"]:
                        print(row["_source"])


def printIndexContentKeyDate(server, port, idx,key,dato):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    # elasticHandler = elasticSearchHandler(server, port)
    res = elasticHandler.search(idx, {"query": {"bool": {"must": [{"match": {"Navn": key}}, {"match": {"Dato": dato}}]}}})
    if (res != None):
        for row in res["hits"]["hits"]:
            print(row["_source"])


def msearch(server, port, idx,field1,value1,field2,value2):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    # elasticHandler = elasticSearchHandler(server, port)
    res = elasticHandler.search(idx, {"query": {"bool": {"must": [{"match": {field1: value1}}, {"match": {field2: value2}}]}}})
    if (res != None):
        for row in res["hits"]["hits"]:
            print(row["_source"])


def search(server, port, idx,field,value):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    # elasticHandler = elasticSearchHandler(server, port)
    res = elasticHandler.search(idx, {"query": {"bool": {"must": [{"match": {field: value}}]}}})
    if (res != None):
        for row in res["hits"]["hits"]:
            print(row["_source"])


def printIndexContentKey(server, port, idx,key):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    # elasticHandler = elasticSearchHandler(server, port)
    res = elasticHandler.search(idx, {"query": {"bool": {"must": [{"match": {"Navn": key}}]}}})
    if (res != None):
        for row in res["hits"]["hits"]:
            print(row["_source"])


if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument('server', help='Ipaddress or logicalname for elastic server')
        parser.add_argument('port', help='portnr for server')

        parser.add_argument('masterfileName', help='File with all datafiles for insertion')
        args = parser.parse_args()
        insertMytest(args.server, args.port)
        exit()
        globalElasticHandler = elasticSearchHandler(args.server, args.port)

        with open(args.masterfileName) as fp:
                line = fp.readline()
                while line:
                        #print(line)
                        sys.stdout.write('#')
                        sys.stdout.flush()
                        #print("Processing file: " + line.strip('\n'))
                        s.info("Processing file: " + line.strip('\n'))
                        fileEnding=line.split(".")[1].strip('\n')
                        #print(fileEnding)
                        if fileEnding == "entity":
                                s.info("entity file insert")
                                relativeFilename = line.split("/")[-1]
                                relativeFilenameNoEnding=relativeFilename.split(".")[0]
                                dato=""
                                year=month=day=hour=min=""
                                correctDate = ""
                                if "/Tv/" in line or "/tv/" in line:
                                    dato = relativeFilename.split("_")[1].strip('\n')

                                    year = relativeFilename.split("_")[2].strip('\n')
                                    month =relativeFilename.split("_")[3].strip('\n')
                                    day = relativeFilename.split("_")[4].strip('\n')
                                    hour=relativeFilename.split("_")[5].strip('\n')
                                    min=relativeFilename.split("_")[6].split(".")[0].strip('\n')
                                    correctDate = day + month + year + " " +hour+min+"00"
                                    #print(correctDate)
                                elif "/radio/" in line or "/Radio/" in line:
                                    dato = relativeFilename.split("_")[1].strip('\n')

                                    year = dato[3:][:4]
                                    month = dato[7:][:2]
                                    day = dato[9:][:2]
                                    hour=dato[11:][:2]
                                    min=dato[13:][:2]
                                    correctDate = day + month + year + " " +hour+min+"00"
                                else:
                                    dato = relativeFilename.split("_")[3]
                                    year = dato[:4]
                                    month = dato[:6][-2:]
                                    day = dato[-2:]
                                    correctDate = day + month + year

                                #print("dato:" + dato + " correctdate: " + correctDate)
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                        insertEntityFile(args.server, args.port, "Avis", correctDate, line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                        insertEntityFile(args.server, args.port, "Tv", correctDate, line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertEntityFile(args.server, args.port, "Radio", correctDate, line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertEntityFile(args.server, args.port, "AvisBilde", correctDate, line.strip('\n'))
                        if fileEnding == "aloc":
                            s.debug("aloc insert")
                            insertAvisLokasjonFile(args.server, args.port, "Avis", line.strip('\n'))

                        if fileEnding == "emne":
                            s.debug("emne insert")
                            insertEmneData(args.server, args.port, line.strip('\n'))

                        if fileEnding == "p2p":
                                s.debug("p2p insert")
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                        insertPersonPersonFile(args.server, args.port, "Avis",line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertPersonPersonFile(args.server, args.port, "Tv", line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertPersonPersonFile(args.server, args.port, "Radio", line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertPersonPersonFile(args.server, args.port, "AvisBilde", line.strip('\n'))
                        if fileEnding == "p2o":
                                s.debug("p2o insert")
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                        insertPersonOrganisasjonFile(args.server, args.port, "Avis",line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertPersonOrganisasjonFile(args.server, args.port, "Tv", line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertPersonOrganisasjonFile(args.server, args.port, "Radio", line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertPersonOrganisasjonFile(args.server, args.port, "AvisBilde", line.strip('\n'))
                        if fileEnding == "p2l":
                                s.debug("p2l insert")
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                     insertPersonLokasjonFile(args.server, args.port, "Avis",line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertPersonLokasjonFile(args.server, args.port, "Tv", line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertPersonLokasjonFile(args.server, args.port, "Radio", line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertPersonLokasjonFile(args.server, args.port, "AvisBilde", line.strip('\n'))
                        if fileEnding == "l2l":
                                s.debug("l2l insert")
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                    insertLokasjonLokasjonFile(args.server, args.port, "Avis",line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertLokasjonLokasjonFile(args.server, args.port, "Tv", line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertLokasjonLokasjonFile(args.server, args.port, "Radio", line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertLokasjonLokasjonFile(args.server, args.port, "AvisBilde", line.strip('\n'))

                        if fileEnding == "o2l":
                                s.debug("o2l insert")
                                if "/avisArtikler/" in line or "/AvisArtikler/" in line:
                                        insertOrganisasjonLokasjonFile(args.server, args.port, "Avis",
                                                                       line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertOrganisasjonLokasjonFile(args.server, args.port, "Tv", line.strip('\n'))
                                if "/radio/" in line or "/Radio/" in line:
                                    insertOrganisasjonLokasjonFile(args.server, args.port, "Radio", line.strip('\n'))
                                if "/avisBilder/" in line or "/AvisBilder/" in line:
                                    insertOrganisasjonLokasjonFile(args.server, args.port, "AvisBilde", line.strip('\n'))

                        if fileEnding == "tcd":
                                s.debug("tcd insert")
                                if "/radio/" in line or "/Radio/" in line:
                                    insertTimecodesFile(args.server, args.port, "Radio",
                                                        line.strip('\n'))
                                if "/tv/" in line or "/Tv/" in line:
                                    insertTimecodesFile(args.server, args.port, "Tv",
                                                        line.strip('\n'))

                        if fileEnding == "LABELS":
                                s.debug("labels insert")
                                insertLabelfile(args.server, args.port, "Video",line.strip('\n'))
                        if fileEnding == "EXPLICIT":
                                s.debug("EXPLICITs insert")
                                insertVideoExplicitfile(args.server, args.port, "Video",line.strip('\n'))
                        if fileEnding == "SHOTS":
                                s.debug("SHOTS insert")
                                insertVideoShotsfile(args.server, args.port, "Video",line.strip('\n'))


                        line = fp.readline()




        # print("Content person")
        printIndexContent(args.server, args.port, "emne")
        #print("Content lokasjon")
        #printIndexContent(args.server, args.port, "lokasjon")
        # print("Content organisasjon")
        # printIndexContent(args.server, args.port, "organisasjon")
        # print("Content person")
        # printIndexContentKeyDate(args.server, args.port, "person","Matruh",'31012011')
        # printIndexContentKey(args.server, args.port, "person", "Matruh")
        # print("search person")
        # search(args.server, args.port,"person","Navn","Matruh")
        # msearch(args.server, args.port, "person", "Navn", "Matruh","Dato","31012011")


        # print("Content timecodes")
        # printIndexContent(args.server, args.port, "timecodes")
        # print("shot annotations")
        # printIndexContent(args.server, args.port, "shotannotation")
        # print("frame annotations")
        #printIndexContent(args.server, args.port, "frameannotation")

        # for filenow in glob.glob(mappingsString):
        #     indexName = filenow.split(".")[1]
        #     dropIndex(args.server, args.port, indexName)



        exit()

        insertPersonPersonFile(args.server, args.port, "Avis",
                               "/home/freddy/disk1/eksempler/avis/2011/01/REL/dagbladet_null_1_20110131_143_30_1-1_055_radiotv.p2p")
        printIndexContent('localhost', 9200, "personperson")
        dropIndex('localhost', 9200, "personperson")

        exit()

        createDataStructure('localhost', 9200, "person", "/home/freddy/elasticsearchMappings/mappings.person")
        createDataStructure('localhost', 9200, "lokasjon", "/home/freddy/elasticsearchMappings/mappings.lokasjon")
        createDataStructure('localhost', 9200, "organisasjon",
                            "/home/freddy/elasticsearchMappings/mappings.organisasjon")

        dato = entityFile.split("_")[3]
        year = dato[:4]
        month = dato[:6][-2:]
        day = dato[-2:]
        correctDate = dato
        # print(correctDate)


        with open(args.masterfileName) as fp:
                line = fp.readline()
                while line:
                        print(line)
                        line = fp.readline()
        exit()

        createDataStructure('localhost', 9200, "person", "/home/freddy/elasticsearchMappings/mappings.person")
        createDataStructure('localhost', 9200, "lokasjon", "/home/freddy/elasticsearchMappings/mappings.lokasjon")
        createDataStructure('localhost', 9200, "organisasjon", "/home/freddy/elasticsearchMappings/mappings.organisasjon")
        insertEntityFile('localhost', 9200, "Avis","/home/freddy/disk1/eksempler/avis/2011/01/ENT/dagbladet_null_1_20110131_143_30_1-1_010_nyheter.entity")
        print("Content person")
        printIndexContent('localhost', 9200, "person")
        print("Content lokasjon")
        printIndexContent('localhost', 9200, "lokasjon")
        print("Content organisasjon")
        printIndexContent('localhost', 9200, "organisasjon")
        dropIndex('localhost', 9200,"person")
        dropIndex('localhost', 9200,"lokasjon")
        dropIndex('localhost', 9200,"organisasjon")
