import csv
import random
import datetime
import json
import uuid

from kafka import KafkaProducer
from datetime import date

medTemp=75
today=date.today()
todayDate=today.strftime("%m/%d/%Y")

def connectToAiven():

  producer = KafkaProducer(
  bootstrap_servers='chh-se-candidate-kafka-chh-se-candidate.a.aivencloud.com:20620',
  security_protocol="SSL",
  ssl_cafile="./ca.pem",
  ssl_certfile="./service.cert",
  ssl_keyfile="./service.key",
  value_serializer=lambda v: json.dumps(v).encode('ascii')
)


def sendMessage (message):
  producer.send(
   'chh_test',
   value=message)


def flush():
  producer.flush()



def genWeatherStations(city, postal, temp):
  current_time=datetime.datetime.now()
  print(city)
  hour=current_time.hour
  minute=current_time.minute
  pws=random.randrange(1,79,3)
  if(temp==""):
    delta=random.randrange(1,19,2)
  else:
    delta=temp

  count=0
  while count<pws:
    pwsname=postal + "-" + str(count)
    #print(pwsname + "-" + str(count) + "-" + str(delta))
    count+=1
    h=0
    while h<=hour:
      prh="{:02d}".format(h)
      #print(pwsname + "-" + str(count) + "-" + str(delta) + "-" + str(prh))
      m=0
      #while m<=minute:
      while m<=59:
        prm="{:02d}".format(m)
        rtime=str(prh) + ":" + str(prm)
        rtemp=medTemp + int(delta) + random.randrange(0,13)
        id=uuid.uuid1()
        #print(city + "," + pwsname + "," + rtime + "," + str(todayDate) + "," + str(rtemp))
        j='{ "uuid":"' + str(id) + '","city":"' + city + '", '
        j=j + '"pws":"' + pwsname + '", '
        j=j + '"readdate": "' + str(todayDate) + '", '
        j=j + '"readtime":"' + rtime + '", '
        j=j + '"readtemp":' + str(rtemp) 
        j=j + '}'
        jdoc=json.loads(j)
        sendMessage(jdoc)
        print(jdoc)
        m+=15
        if(h==hour and m>minute):
          break
      h+=1

producer = KafkaProducer(
  bootstrap_servers='chh-se-candidate-kafka-chh-se-candidate.a.aivencloud.com:20620',
  security_protocol="SSL",
  ssl_cafile="./ca.pem",
  ssl_certfile="./service.cert",
  ssl_keyfile="./service.key",
  value_serializer=lambda v: json.dumps(v).encode('ascii')
)
 
# opening the CSV file
#with open('flzips.csv', mode ='r')as file:
#uncomment below for testing smaller data set
with open('tmp.csv', mode ='r')as file:
   
  # reading the CSV file
  csvFile = csv.reader(file)
 
  # displaying the contents of the CSV file
  for lines in csvFile:
    print(lines[1] + " " + lines[2])
    genWeatherStations(lines[1], lines[0], lines[2])
    flush()
