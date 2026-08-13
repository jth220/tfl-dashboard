import requests
import json

url = "https://api.tfl.gov.uk/Line/Mode/tube/Status" #Endpoint of interest

response = requests.get(url)

print("Status Code:", response.status_code)

data = response.json()
data = data[0] # Looks only at Bakerloo Line

print('Recent Update at :', data["modified"])

#Station Name
print('Station Line:', data["name"])

if data["disruptions"]:
    print('Disruptions:', data["disruptions"]) #What are disruptions defined as in this case?, if there is a minor delay, surely there is a disruption aswell?
else:
    print('No Disruptions')

lineStatuses = data["lineStatuses"]

print("Line Status: ", lineStatuses[0]["statusSeverityDescription"])


#Printing all top level keys
print(data.keys())


data = response.json()
for line in data:
    print(line["name"])