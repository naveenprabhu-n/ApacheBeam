'''
The Codebase is a quickstart to analyse streaming data using Apache Beam.
It is best suited for Cloud Dataflow runner on Google Cloud

Sales Data arrives into Cloud PubSub as a comma delimited row of values, 
e.g. schema
StoreLocation, StoreId, ItemId, ItemName, CostPrice, Qty, SellingPrice, pointofsaletimestamp

The data is cleaned, processed and aggregated after windowing them over a particular period.

The question we're trying to answer : 
Every 10 Seconds, Give me the total profit figures across 2 locations Mumbai and Bangalore for the last 30 seconds.
'''

#Import necessary library to handle streaming data

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window

#Create necessary components on GCP and add the details below

projectid= <Google Cloud Project Id>
inputsubscriptionname= <Cloud PubSub Subscription>
outputtopic= <Cloud PubSub topic>

#Declare the PubSub services

inputsubscription="project/{projectid}/subscriptions/{inputsubscriptionname}".format(projectid,inputsubscriptionname)
outputtopic="project/{projectid}/topic/{outputtopic}".format(projectid,outputtopic)

#Set streaming data options

options=PipelineOptions()
options.view_as(StandardOptions).streaming=True

'''
Create ParDo classes

Creating a class to calculate profit from the row of data and append to the record. 
Inherit DoFn class to render it ParDo

profit= Quantity * (Sellingprice - CostPrice)

'''

class addprofitcolumn(beam.DoFn):
  def process(self, element):

    qty=int(element[5])
    sellprice=int(element[6])
    costprice=int(element[4])
    profit=qty*(sellprice-costprice)

    element.append(str(profit))
    return element

'''
If the exact Point of sale timestamp is to be used to analysis, we can assign it as the timestamp, 
instead of beam's conventional or default timestamp.

Note : While this is highly personalized, 
it is recommended to use beam's default timestamp attached to the record for downstream windowing tasks.
'''

class addcustomtimestamp(beam.DoFn):
  def process(self, element):
    unixtimestamp=int(element[7])
    return beam.window.TimestampedValue(element,unixtimestamp)


#initiate a apache beam pipeline with the pipeline configuration
pipeline=beam.Pipeline()

'''
Create the input pcollection by reading the data from pubsub
Remove trailing and leading whitespaces if any
The source considered is CSV, Split the row on the commas 
'''

input=(pipeline
       |beam.io.ReadFromPubSub(subscription=inputsubscription)
       |beam.Map(lambda data: data.lstrip().rstrip())
       |beam.Map(lambda row: row.split(','))
       )

'''
Filter the reocrd based on the store location Mumbai and Bangalore
Calculate the Profit figure and add the POS timestamp if needed 
The profit becomes the last entry in the record
'''

processed=(input
         |beam.Filter(lambda row: row[0]=='Mumbai' or row[0]=='Bangalore')
         |beam.ParDo(addprofitcolumn)
         |beam.ParDo(addcustomtimestamp)
         |beam.Map(lambda row: (row[0],row[8]))
         )

'''
Use 30 second windows over 10 second Sliding period and aggreagte the profits
Included a sample FixedWindow function - which calculates discrete profit figures without overlap 
Question demands sliding window
'''

windowaggregate=(processed
                 #|beam.WindowInto(window.FixedWindows(20))
                 |beam.WindowInto(window.SlidingWindows(30,10))
                 |beam.CombinePerKey(sum)
                 )

'''
Write the output to a CLoud PubSub Topic after encoding the rows as per requirement
'''
output=(windowaggregate
        |beam.Map(lambda row: row.encode('utf-8'))
        |beam.io.WriteToPubSub(topic=outputtopic)
        )

'''
run the beam pipeline and wait until streaming data encounter is interrupted
'''

result=pipeline.run()

result.wait_until_finish


