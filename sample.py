from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import NullType, LongType, StructType,StructField, StringType
from saxonche import *
from functools import partial


xml = '<?xml version="1.0" encoding="utf-8"?> <visitors> <visitor id="9615" age="68" sex="F" /> <visitor id="1882" age="34" sex="M" /> <visitor id="5987" age="23" sex="M" /> </visitors>'

df = spark.createDataFrame([('1',xml)],['id','visitors']) #Dummy Dataframe with xml as tring in a column
#Assuming you already have xslt loaded either into a file or a string
# This code is greatly inspired from https://stackoverflow.com/questions/71390160/unable-to-pass-class-object-to-pyspark-udf

with PySaxonProcessor(license=False) as proc:

    def transformXML(xmlString,xsltprocObj = None):
        document = proc.parse_xml(xml_text=xmlString)
        xsltprocObj.set_source(xdm_node=document)
        output2 = xsltprocObj.transform_to_string()
        return output2
    
    xsltproc = proc.new_xslt30_processor()
    xsltproc.compile_stylesheet(stylesheet_text=stringxslt)
    transformXmludf = udf(partial(transformXML,xsltprocObj=xsltproc),StringType())
    

    df = df.withColumn("transformedXml", transformXmludf(col("xml")))
