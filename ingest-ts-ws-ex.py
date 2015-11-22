import pandas
import websocket
import thread
import time
import calendar
import sys
import csv
import os
import argparse
import textwrap
import requests
import json
import yaml
from collections import defaultdict

# Parser for input arguments

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
                Script to ingest data from a csv file into Predix time-series service in
            Predix Cloud.

            The csv file must have the following columns:
              - Equipment name
                The default column index is 1
                You can specify a new column index using the option -ei
                Keep in mind the columns index start at 0
              - Tag name
                The default column index is 2
                You can specify a new column index using the option -ti
              - Timestamp in the format month / day / year hour:minute:seconds AM/PM
                The default column index is 3
                You can specify a new column index using the option -di
                You can also specify a new timestamp format by using the option -t
              - Value
                The default column index is 4
                You can specify a new column index using the option -vi
        '''),
    epilog=textwrap.dedent('''\
        -------------------------------------------------------------------------------
                           Developed by Ricardo Breder (200020939)
        '''))

parser.add_argument("data", help="csv file with time-series data")
parser.add_argument("--tss", dest='tss', default='', help="time-series wss url")
parser.add_argument("--zone", dest='zone', default='', help="time-series service instance id")
parser.add_argument("--uaa", dest='uaa', default='', help="predix UAA issuerId (Token URI)")
parser.add_argument("--client", dest='client', default='', help="predix UAA Client")
parser.add_argument("--secret", dest='secret', default='', help="predix UAA Client secret")
parser.add_argument("--username", dest='username', default='', help="username from Predix UAA with access to time-series")
parser.add_argument("--password", dest='password', default='', help="password from Predix UAA with access to time-series")
parser.add_argument("--token", dest='token', default='', help="specify the predix UAA token with access to time-series")
parser.add_argument("-d", "--delimiter", dest='delimiter', default=';', help="specify the delimiter character. Default is ;")
parser.add_argument("-t", "--timestamp", dest='timestamp', default="%m/%d/%Y %I:%M:%S %p", help="specify the timestamp format following strftime(3) documentation")
parser.add_argument("-s", "--datapoints", dest='dpsize', default='500', help="specify the number of points per message")
parser.add_argument("--ei", dest='eni', default='1', help="specify the index of the equipment name column in the csv")
parser.add_argument("--ti", dest='tni', default='2', help="specify the index of the tag name column in the csv")
parser.add_argument("--di", dest='tsi', default='3', help="specify the index of the timestamp column in the csv")
parser.add_argument("--vi", dest='vi', default='4', help="specify the index of the value column in the csv")
parser.add_argument("--solH", dest='solH', default='', help="specify the SOL high limit")
parser.add_argument("--solL", dest='solL', default='', help="specify the SOL low limit")
parser.add_argument("--dBand", dest='dBand', default='', help="specify the deadband")
parser.add_argument("--dSecs", dest='dSecs', default='', help="specify the deadband wait time in miliseconds")
parser.add_argument("--es", dest='excursionSvc', default='', help="specify the service URI to send the excursions")
parser.add_argument("-y", dest='yaml', default='', help="specify a yaml file with the configuration")
args = parser.parse_args()

data = args.data
tsUri = args.tss
tsZone = args.zone
uaaUri = args.uaa
uaaUsername = args.username
uaaSecret = args.secret
uaaClient = args.client
uaaPassword = args.password
uaaToken = args.token
delimiter = args.delimiter
timestamp = args.timestamp
dpsize = int(args.dpsize)
solH = float(args.solH)
solL = float(args.solL)
dBand = float(args.dBand)
dSecs = int(args.dSecs)
eni = int(args.eni)
tni = int(args.tni)
tsi = int(args.tsi)
vi = int(args.vi)
excursionSvc = args.excursionSvc
yamlFile = args.yaml

def on_message(ws, message):
    if json.JSONDecoder().decode(message)["statusCode"] != 202:
        print("Error sending packet to time-series service")
        print(message)
        sys.exit(1)
    else:
        print("Packet Sent")

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("--- Socket Closed ---")

def on_open(ws):
    if os.path.isfile(data):
        df = pandas.read_csv(data, sep=delimiter, header=None)
        df.sort_values(by=[tsi], ascending=True, inplace=True)

        i = 0
        m = 1
        datapoints = []
        tagname = ""
        equipname = ""
        meter = ""
        payloads = []
        nCount = 0
        excursion = {"start": 0, "end": 0, "duration": 0, "tag": "", "active": False, "type": "", "count": 0, "source": "OSIPI"}

        print("Generating packets with " + str(dpsize) + " data points...")

        for index, row in df.iterrows():
            # Create the packets to send it over WS
            # Define the tag name if none exists
            if meter == "":
                tagname = row[tni]
                equipname = row[eni]
                meter = equipname + "_" + tagname

            # If current tag name is different than the tag name from the file, define another tag name
            elif meter != equipname + "_" + tagname:
                payloads.append(payload(ws, meter, datapoints, m))
                tagname = row[tni]
                equipname = row[eni]
                meter = equipname + "_" + tagname
                m += 1
                i = 0
                datapoints = []

            # Add the last point in the packet and exit the loop
            if i >= dpsize:
                payloads.append(payload(ws, meter, datapoints, m))
                m += 1
                i = 0
                datapoints = []

            # Verifies if the value is a valid number or don't add the point
            try:
                value = float(row[vi])
            except:
                value = 0.0
                i += 1
                continue

            tstamp = calendar.timegm(time.strptime(row[tsi], timestamp)) * 1000
            datapoints.append([tstamp, value])

            # Verifies for SOL excursions on High limit
            if value > solH:

                # Verifies for active SOL High excursion
                if excursion['type'] == "SOL High":
                    excursion['duration'] += (tstamp - excursion['end'])
                    excursion['end'] = tstamp
                    excursion['count'] += 1
                else:
                    # Deactivates SOL Low excursion and send it
                    if excursion['type'] == "SOL Low":
                        excursion['active'] = False
                        # if nCount == 0:
                        excursion['duration'] += (tstamp - excursion['end'])
                        excursion['end'] = tstamp
                        excursion['count'] += 1

                        # Only add if duration time is greater than 60 seconds
                        if excursion['duration'] > 60000:
                            sendExcursions(excursion)

                    # Create new SOL High excursion
                    excursion['end'] = tstamp
                    excursion['start'] = tstamp
                    excursion['duration'] = 0
                    excursion['active'] = True
                    excursion['tag'] = meter
                    excursion['type'] = "SOL High"
                    excursion['count'] = 1

            # Verifies for SOL excursions on Low Limit
            elif value < solL:

                # Verifies for active SOL Low excursion
                if excursion['type'] == "SOL Low":
                    excursion['duration'] += (tstamp - excursion['end'])
                    excursion['end'] = tstamp
                    excursion['count'] += 1
                else:
                    # Deactivates SOL High excursion and send it
                    if excursion['type'] == "SOL High":
                        excursion['active'] = False
                        
                        excursion['duration'] += (tstamp - excursion['end'])
                        excursion['end'] = tstamp
                        excursion['count'] += 1

                        # Only add if duration time is greater than 60 seconds
                        if excursion['duration'] > 60000:
                            sendExcursions(excursion)

                    # Create new SOL Low excursion
                    excursion['end'] = tstamp
                    excursion['start'] = tstamp
                    excursion['duration'] = 0
                    excursion['active'] = True
                    excursion['tag'] = meter
                    excursion['type'] = "SOL Low"
                    excursion['count'] = 1

            # If not excursion
            else:
                # If there's an active excursion, de-activates it and send it
                if excursion['type'] != "" and (value > (solL + dBand) and (tstamp - excursion['end']) > dSecs):
                    excursion['active'] = False
                    # excursion['duration'] += (tstamp - excursion['end'])
                    # excursion['end'] = tstamp

                    # Only add if duration time is greater than 60 seconds
                    if excursion['duration'] > 60000:
                        sendExcursions(excursion)
                    
                    # Reset excursion object
                    excursion['end'] = 0
                    excursion['start'] = 0
                    excursion['duration'] = 0
                    excursion['tag'] = ""
                    excursion['type'] = ""
                    excursion['count'] = 0

                elif excursion['type'] != "" and (value < (solL + dBand) and (tstamp - excursion['end']) < dSecs):
                    excursion['duration'] += (tstamp - excursion['end'])
                    excursion['end'] = tstamp
                    excursion['count'] += 1
                    # nCount = 1

            i += 1

        # Append last packet to payload list
        if i > 0:
            payloads.append(payload(ws, meter, datapoints, m+1))

        # Send payloads
        sendPayload(ws, payloads)

        # Send last excursion
        if excursion['type'] != "" and excursion['duration'] > 60000:
            sendExcursions(excursion)

    else:
        print("Time-series csv data file not found")
        print("Please make sure the file " + data + "exists and you have access")
        print("Terminating...")
        sys.exit(1)


def payload(ws, meter, datapoints, m):
    datapointsstr = ""
    for d in datapoints:
        datapointsstr += "[" + str(d[0]) + "," + str(d[1]) + "],"

    datapointsstr = datapointsstr[:-1]

    payload = '''{  
                   "messageId": ''' + str(m) + ''',
                   "body":[  
                      {  
                         "name":"''' + meter + '''",
                         "datapoints": [''' + datapointsstr + '''],
                         "attributes":{  
                         }
                      }
                   ]
                }'''
    return payload

def sendPayload(ws, payloads):
    def run(*args):
        i = 0
        it = len(payloads)
        for p in payloads:
            i += 1
            ws.send(p)
            print("Sending packet " + str(i) + " of " + str(it))
            time.sleep(1)

        time.sleep(1)
        ws.close()
        print(str(i) + " packets sent.")
        print("Thread terminating...")

    thread.start_new_thread(run, ())

def sendExcursions(excursions):
    print("Sending Excursion...")
    exs = []
    exs.append(excursions)
    request = requests.post(excursionSvc, data = json.JSONEncoder().encode(exs))
    if request.status_code == requests.codes.ok:
        print("Excursion sent")
    else:
        print("Error sending excursion")
        request.raise_for_status()

def getToken():
    uri = uaaUri
    payload = {"grant_type": "password", "username": uaaUsername, "password": uaaPassword}
    auth = requests.auth.HTTPBasicAuth(uaaClient, uaaSecret)
    request = requests.post(uri, data=payload, auth=auth)
    if request.status_code == requests.codes.ok:
        return json.JSONDecoder().decode(request.text)["access_token"]
    else:
        print("Error requesting token")
        request.raise_for_status()

if __name__ == "__main__":
    if yamlFile != '':
        if os.path.isfile(yamlFile):
            with open(yamlFile, 'r') as stream:
                config = yaml.load(stream)
                tsUri = config["time-series"]["uri"]
                tsZone = config["time-series"]["zone"]
                uaaToken = config["uaa"]["token"]
                uaaUri = config["uaa"]["uri"]
                uaaClient = config["uaa"]["client"]
                uaaSecret = config["uaa"]["secret"]
                uaaUsername = config["uaa"]["username"]
                uaaPassword = config["uaa"]["password"]
                if "csv" in config.keys():
                    csvConfig = config["csv"]
                    if "delimiter" in csvConfig.keys():
                        delimiter = csvConfig["delimiter"]
                    if "timestamp" in csvConfig.keys():
                        timestamp = csvConfig["timestamp"]
                    if "packetsize" in csvConfig.keys():
                        dpsize = int(csvConfig["packetsize"])
                    if "indexes" in csvConfig.keys():
                        eni = int(csvConfig["indexes"]["equipment"])
                        tni = int(csvConfig["indexes"]["tag"])
                        tsi = int(csvConfig["indexes"]["timestamp"])
                        vi = int(csvConfig["indexes"]["value"])
                if "excursion" in config.keys():
                    exConfig = config["excursion"]
                    if "uri" in exConfig.keys():
                        excursionSvc = exConfig["uri"]
                print("Configuration file " + yamlFile + " loaded")
        else:
            print("The file " + yamlFile + " doesn't exist or you don't have permission to access it")
            print("Terminating...")
            sys.exit(1)
    elif tsUri == '' or tsZone == '' or uaaUri == '' or uaaClient == '' or uaaSecret == '' or uaaUsername == '' or uaaPassword == '':
        print("Please either use a yaml file with option -y")
        print("or specify the parements -tss -zone -uaa -client -secre -username -password")
        print("Terminating...")
        sys.exit(1)

    websocket.enableTrace(True)
    host = tsUri

    if uaaToken == '':
        uaaToken = getToken()

    headers = {
                'Authorization:bearer ' + uaaToken,
                'Predix-Zone-Id:' + tsZone,
                'Origin:http://localhost/'
    }
    ws = websocket.WebSocketApp(
                                host,
                                header = headers,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close
    )
    ws.on_open = on_open
    ws.run_forever()