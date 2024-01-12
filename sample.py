from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import NullType, LongType, StructType,StructField, StringType
from saxonche import *
from functools import partial


with PySaxonProcessor(license=False) as proc:

    def transformXML(xmlString,xsltprocObj = None):
        document = proc.parse_xml(xml_text=xmlString)
        xsltprocObj.set_source(xdm_node=document)
        output2 = xsltprocObj.transform_to_string()
        return output2
    
    xsltproc = proc.new_xslt30_processor()
    xsltproc.compile_stylesheet(stylesheet_text=stringxslt)
    transformXmludf = udf(partial(transformXML,xsltprocObj=xsltproc),StringType())
    

    df = df.withColumn("xmlStringCol", transformXmludf(col("transformedXml")))
