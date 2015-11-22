import pandas
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


def detectExcursion(uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername):
    if os.path.isfile(data):
        df = pandas.read_csv(data, sep=delimiter, header=None)
        if metername == -1:
            df.sort_values(by=[eni,tni,tsi], ascending=True, inplace=True)
        else:
            df.sort_values(by=[metername,tsi], ascending=True, inplace=True)

        i = 0
        m = 1
        tagname = data.rsplit('/',1)[1].replace('.csv', '')
        equipname = ""
        meter = ""
        nExcursions = 0
        excursion = {"SOLL": {"start": 0, "end": 0, "duration": 0, "tag": "", "active": False, "type": "", "count": 0, "source": "OSIPI", "lowLimit": 0, "highLimit": 0, "timeclear": 0}, 
        "SOLH": {"start": 0, "end": 0, "duration": 0, "tag": "", "active": False, "type": "", "count": 0, "source": "OSIPI", "lowLimit": 0, "highLimit": 0, "timeclear": 0}}

        print("Detecting Excursions")

        for index, row in df.iterrows():
            # Create the packets to send it over WS
            # Define the tag name if none exists
            if metername == -1:
                if meter == "":
                    # tagname = row[tni]
                    equipname = row[eni]
                    meter = equipname + concat + tagname

                # If current tag name is different than the tag name from the file, define another tag name
                elif meter != equipname + concat + tagname:
                    tagname = row[tni]
                    equipname = row[eni]
                    meter = equipname + concat + tagname

            else:
                if meter == "":
                    meter = row[metername]

                # If current tag name is different than the tag name from the file, define another tag name
                elif meter != row[metername]:
                    meter = row[metername]

            # Verifies if the value is a valid number or don't compute it
            try:
                value = float(row[vi])
            except:
                value = 0.0
                i += 1
                continue

            # Try to convert from date stamp to epoch in ms
            try:
                tstamp = calendar.timegm(time.strptime(row[tsi], timestamp)) * 1000
            except:
                try:
                    tstamp = calendar.timegm(time.strptime(row[tsi], timestamp+".%f")) * 1000
                except:
                    print("Error converting date using the provided time stamp")
                    print("Terminating...")
                    sys.exit(1)


            # Helper to skip unnecessary ifs
            detected = 0

            # Verifies for SOL excursions on High limit
            if value > solH and solH != '':
                detected = 1
                excursion['SOLH']['timeclear'] = 0
                # Verifies for active SOL High excursion
                if excursion['SOLH']['count'] != 0:
                    excursion['SOLH']['duration'] += (tstamp - excursion['SOLH']['end'])
                    excursion['SOLH']['end'] = tstamp
                    excursion['SOLH']['count'] += 1
                else:
                    # Create new SOL High excursion
                    excursion['SOLH']['end'] = tstamp
                    excursion['SOLH']['start'] = tstamp
                    excursion['SOLH']['duration'] = 0
                    excursion['SOLH']['active'] = True
                    excursion['SOLH']['tag'] = meter
                    excursion['SOLH']['type'] = "SOL High"
                    excursion['SOLH']['count'] = 1
                    excursion['SOLH']['lowLimit'] = solL
                    excursion['SOLH']['highLimit'] = solH

            # Verifies for SOL excursions on Low Limit
            if value < solL and solL != '':
                detected = 1
                excursion['SOLL']['timeclear'] = 0
                # Verifies for active SOL Low excursion
                if excursion['SOLL']['count'] != 0:
                    excursion['SOLL']['duration'] += (tstamp - excursion['SOLL']['end'])
                    excursion['SOLL']['end'] = tstamp
                    excursion['SOLL']['count'] += 1
                else:
                    # Create new SOL Low excursion
                    excursion['SOLL']['end'] = tstamp
                    excursion['SOLL']['start'] = tstamp
                    excursion['SOLL']['duration'] = 0
                    excursion['SOLL']['active'] = True
                    excursion['SOLL']['tag'] = meter
                    excursion['SOLL']['type'] = "SOL Low"
                    excursion['SOLL']['count'] = 1
                    excursion['SOLL']['lowLimit'] = solL
                    excursion['SOLL']['highLimit'] = solH

            # If not excursion
            if detected == 0:
                # If there's an active excursion, check for dead band
                if excursion['SOLL']['start'] != 0 and value > (solL + dBand):
                    if excursion['SOLL']['timeclear'] == 0:
                        excursion['SOLL']['timeclear'] = tstamp
                    elif (tstamp - excursion['SOLL']['timeclear']) >= dSecs:
                        excursion['SOLL']['active'] = False

                        # Only add if duration time is greater than 60 seconds
                        # if excursion['duration'] > 60000:
                        sendExcursions(excursion['SOLL'])
                        nExcursions += 1
                        
                        # Reset excursion object
                        excursion['SOLL']['end'] = 0
                        excursion['SOLL']['start'] = 0
                        excursion['SOLL']['duration'] = 0
                        excursion['SOLL']['tag'] = ""
                        excursion['SOLL']['type'] = ""
                        excursion['SOLL']['count'] = 0
                        excursion['SOLL']['lowLimit'] = 0
                        excursion['SOLL']['highLimit'] = 0
                        excursion['SOLL']['timeclear'] = 0

                elif excursion['SOLH']['start'] != 0 and value < (solH - dBand):
                    if excursion['SOLH']['timeclear'] == 0:
                        excursion['SOLH']['timeclear'] = tstamp
                    elif (tstamp - excursion['SOLH']['timeclear']) >= dSecs:
                        excursion['SOLH']['active'] = False

                        # Only add if duration time is greater than 60 seconds
                        # if excursion['duration'] > 60000:
                        sendExcursions(excursion['SOLH'])
                        nExcursions += 1
                        
                        # Reset excursion object
                        excursion['SOLH']['end'] = 0
                        excursion['SOLH']['start'] = 0
                        excursion['SOLH']['duration'] = 0
                        excursion['SOLH']['tag'] = ""
                        excursion['SOLH']['type'] = ""
                        excursion['SOLH']['count'] = 0
                        excursion['SOLH']['lowLimit'] = 0
                        excursion['SOLH']['highLimit'] = 0
                        excursion['SOLH']['timeclear'] = 0


            i += 1

        # Send last excursion
        if excursion['SOLH']['start'] != 0:
            sendExcursions(excursion['SOLH'])
            nExcursions += 1
        if excursion['SOLL']['start'] != 0:
            sendExcursions(excursion['SOLL'])
            nExcursions += 1

    else:
        print("Time-series csv data file not found")
        print("Please make sure the file " + data + " exists and you have access")
        print("Terminating...")
        sys.exit(1)

    return nExcursions


def sendExcursions(excursions):
    print("Sending Excursion...")
    exs = []
    excursions.pop('timeclear', None)
    exs.append(excursions)
    # print json.dumps(exs, sort_keys=True, indent=2, separators=(',',': '))
    request = requests.post(excursionSvc, data = json.JSONEncoder().encode(exs))
    if request.status_code == requests.codes.ok:
        print("Excursion sent")
    else:
        print("Error sending excursion")
        request.raise_for_status()

def getToken(uaaUri, uaaUsername, uaaPassword, uaaClient, uaaSecret):
    uri = uaaUri
    payload = {"grant_type": "password", "username": uaaUsername, "password": uaaPassword}
    auth = requests.auth.HTTPBasicAuth(uaaClient, uaaSecret)
    request = requests.post(uri, data=payload, auth=auth)
    if request.status_code == requests.codes.ok:
        return json.JSONDecoder().decode(request.text)["access_token"]
    else:
        print("Error requesting token")
        request.raise_for_status()

def loadFromYaml(yamlFile, uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername):
    if yamlFile != '':
        if os.path.isfile(yamlFile):
            with open(yamlFile, 'r') as stream:
                config = yaml.load(stream)
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
                    if "indexes" in csvConfig.keys():
                        eni = int(csvConfig["indexes"]["equipment"])
                        tni = int(csvConfig["indexes"]["tag"])
                        tsi = int(csvConfig["indexes"]["timestamp"])
                        vi = int(csvConfig["indexes"]["value"])
                    if "concatchar" in csvConfig.keys():
                        concat = csvConfig["concatchar"]
                    if "metername" in csvConfig.keys():
                        metername = csvConfig["metername"]

                if "excursion" in config.keys():
                    exConfig = config["excursion"]
                    if "uri" in exConfig.keys():
                        excursionSvc = exConfig["uri"]

                if "excursions-config" in config.keys():
                    exsConfig = config["excursions-config"]
                    if data.rsplit('/',1)[1].replace('.csv', '') in exsConfig.keys():
                        config = exsConfig[data.rsplit('/',1)[1].replace('.csv', '')]
                        if "solL" in config.keys():
                            solL = float(config["solL"])
                        if "solH" in config.keys():
                            solH = float(config["solH"])
                        if "deadband" in config.keys():
                            dBand = float(config["deadband"])
                        if "deadms" in config.keys():
                            dSecs = int(config["deadms"])

                print("Configuration file " + yamlFile + " loaded")
        else:
            print("The file " + yamlFile + " doesn't exist or you don't have permission to access it")
            print("Terminating...")
            sys.exit(1)
    elif uaaUri == '' or uaaClient == '' or uaaSecret == '' or uaaUsername == '' or uaaPassword == '':
        print("Please either use a yaml file with option -y")
        print("or specify the parements -uaa -client -secret -username -password")
        print("Terminating...")
        sys.exit(1)



    if solL == '':
        print("Configuration for SOL Low excursion not found")
        print("Detection for SOL Low will be skipped")
    if solH == '':
        print("Configuration for SOL High excursion not found")
        print("Detection for SOL High will be skipped")

    return uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername


if __name__ == "__main__":
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
                               Developed by Ricardo Breder
            '''))

    parser.add_argument("data", nargs='*', help="csv file with time-series data")
    parser.add_argument("--uaa", dest='uaa', default='', help="predix UAA issuerId (Token URI)")
    parser.add_argument("--client", dest='client', default='', help="predix UAA Client")
    parser.add_argument("--secret", dest='secret', default='', help="predix UAA Client secret")
    parser.add_argument("--username", dest='username', default='', help="username from Predix UAA with access to excursion service")
    parser.add_argument("--password", dest='password', default='', help="password from Predix UAA with access to excursion service")
    parser.add_argument("--token", dest='token', default='', help="specify the predix UAA token with access to time-series")
    parser.add_argument("-d", "--delimiter", dest='delimiter', default=';', help="specify the delimiter character. Default is ;")
    parser.add_argument("-t", "--timestamp", dest='timestamp', default="%m/%d/%Y %I:%M:%S %p", help="specify the timestamp format following strftime(3) documentation")
    parser.add_argument("--ei", dest='eni', default='1', help="specify the index of the equipment name column in the csv")
    parser.add_argument("--ti", dest='tni', default='2', help="specify the index of the tag name column in the csv")
    parser.add_argument("--di", dest='tsi', default='3', help="specify the index of the timestamp column in the csv")
    parser.add_argument("--vi", dest='vi', default='4', help="specify the index of the value column in the csv")
    parser.add_argument("--solH", dest='solH', default='', help="specify the SOL high limit")
    parser.add_argument("--solL", dest='solL', default='', help="specify the SOL low limit")
    parser.add_argument("--dBand", dest='dBand', default=0, help="specify the deadband")
    parser.add_argument("--dSecs", dest='dSecs', default=0, help="specify the deadband wait time in miliseconds")
    parser.add_argument("--es", dest='excursionSvc', default='', help="specify the service URI to send the excursions")
    parser.add_argument("-c", dest='concat', default='_', help="specify the character used in the concatenation for the meter name")
    parser.add_argument("-k", dest='skipmeter', default='-1', help="specify the index for the meter name. (Don't need to specify equipment and tag columns)")
    parser.add_argument("-y", dest='yaml', default='', help="specify a yaml file with the configuration")
    args = parser.parse_args()

    files = args.data
    uaaUri = args.uaa
    uaaUsername = args.username
    uaaSecret = args.secret
    uaaClient = args.client
    uaaPassword = args.password
    uaaToken = args.token
    delimiter = args.delimiter
    timestamp = args.timestamp
    eni = int(args.eni)
    tni = int(args.tni)
    tsi = int(args.tsi)
    vi = int(args.vi)
    excursionSvc = args.excursionSvc
    yamlFile = args.yaml
    dBand = float(args.dBand)
    dSecs = int(args.dSecs)
    concat = args.concat
    metername = int(args.skipmeter)

    if args.solL != '':
        solL = float(args.solL)
    else:
        solL = ''

    if args.solH != '':
        solH = float(args.solH)
    else:
        solH = ''

    for data in files:
        print("Detecting excursions on file: " + data)
        uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername =  loadFromYaml(yamlFile, uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername)
        nDetected = detectExcursion(uaaToken, uaaUri, uaaClient, uaaSecret, uaaUsername, uaaPassword, delimiter, timestamp, eni, tni, tsi, vi, excursionSvc, solL, solH, dBand, dSecs, concat, metername)
        print("Finished processing file.")
        print(str(nDetected) + " excursions sent.")
    print("All " + str(len(files)) + " files processed.")
