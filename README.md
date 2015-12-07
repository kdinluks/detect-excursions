# detect-excursions
### Python script to detect excursions from a csv file, or a collection of files, and send them to a excursion service to be persisted.

            The csv file must have the following columns:
              - Equipment name
                The default column index is 1
                You can specify a new column index using the option --ei
                Keep in mind the columns index start at 0
              - Tag name
                The default column index is 2
                You can specify a new column index using the option --ti
              - Timestamp in the format month / day / year hour:minute:seconds AM/PM
                The default column index is 3
                You can specify a new column index using the option --di
                You can also specify a new timestamp format by using the option -t
              - Value
                The default column index is 4
                You can specify a new column index using the option --vi

## To run the script for 1 file
```bash
	python detect-excursions.py <csv-file> --token <token> --es <excursion service>
```
## To run the script for more than 1 file
```bash
    python detect-excursions.py <csv-file1> <csv-file2> <csv-file3> --token <token> --es <excursion service>
```
Where:

            - <csv-file> the csv file with the time-series data, please see the comments above.
            - <token> the Bearer token from the UAA instance used by the excursion service.
            - <excursion service> the REST endpoint to persist the excursion.

### Optionally you can pass the following arguments:

            -d or --delimiter : used to specify the delimiter used in the csv file. The default is ";".
            -t or --timestamp : used to specify the timestamp format following the python documentation for the function  strptime(). The default is '%m/%d/%Y %I:%M:%S %p'.
            -s or --datapoints : used to specify the number of points per message to be sent over the wss connection. The default is 500.
            --ei : used to specify the equipment name column index as explained above.
            --ti : used to specify the tag name column index as explained above.
            --di : used to specify the timestamp column index as explained above.
            --vi : used to specify the value column index as explained above.
            --solH : used to specify the SOL high limit
            --solL : used specify the SOL low limit
            --dBand : used to specify the deadband
            --dSecs : used to specify the deadband wait time in miliseconds
            -c : used to specify the character used in the concatenation of equipment name and tag name to create the meter name.
            The default is "_"
            -k : used to specify the index of the column that contains the meter name.
            Note, if you specify -k you don't need to specify -ei and -ti as they won't be used.

### Alternatively you can pass a Yaml configuration following the config.yml template
```bash
    python detect-excursions.py <csv-file> -y <yaml-file>
```

### Let the script fetch the token

        If you don't want to worry about fetching the token prior to running the script, you can pass the client, user and UAA information either in the yaml configuration file or by using the script arguments, and the script will fetch the token for you.
        In order for the script to be able to fetch the token, the client must have the "password" grant type authorized.

        To pass the client, user and UAA information using a yaml file just follow the config.yml template.

        To pass the information using script arguments:
            --uaa : used to specify the predix UAA issuerId (Token URI)
            --client : used to specify UAA Client
            --secret : used to specify UAA Secret
            --username : used to specify UAA username with access to the excursions service
            --password : used to specify UAA user password

## Output of the script

        The script sends a JSON for every excursion it finds to a REST endpoint using the POST method.
        The JSON structury is
        ```JSON
        [
          {
            "active": true,
            "count": 132480,
            "duration": 7948741000,
            "end": 1446335940000,
            "highLimit": 96.0,
            "lowLimit": 1624.0,
            "source": "OSIPI",
            "start": 1438387199000,
            "tag": "AT-HBG-2730_ATT_PXI27301.PV",
            "type": "SOL Low"
          }
        ]
        ```