package com.ojag.LogsTest

import java.sql.Timestamp
import java.util.TimeZone
import java.sql.Date
import scala.math.min
import java.text.{DateFormat, SimpleDateFormat}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j._

import vegas._
import vegas.sparkExt._






object processLogs {
  
  
  
  /*
   * The getDist function uses Haversines law to compute
   * the geodesic distance between two random point on the
   * Earth's surface.
   * */
  
  type coords = (Float, Float)  
  def getDist(ref:coords,dest:coords): Double = {
    val lat_r = math.Pi / 180.0 * ref._1
    val long_r = math.Pi / 180.0 * ref._2
    val lat_d = math.Pi / 180.0 * dest._1
    val long_d = math.Pi / 180.0 * dest._2
    // Now using haversine's method:
    val longDiff = long_r - long_d
    val latDiff = lat_r - lat_d
    val a = math.pow(math.sin(latDiff / 2), 2) + math.cos(lat_r) *
      math.cos(lat_d) * math.pow(math.sin(longDiff / 2), 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    val km = 6367 * c 
    km
}
                          
  /*since there is no direct way in spark, this function reads a string and
   * converts it to time-stamps format, and returns the result,using the
   * default option as follows:
   * */  
  
                          
 def getTimeStamp(timeStr: String): Timestamp = {   
    val df1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val df2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    
    //val rslt:Timestamp =new Timestamp (df1.parse(timeStr).getTime)  
    //return rslt or:
    
    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(df1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(df2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }
  
  
  type poiType = (Float,Float)
  type poiRType =(String,poiType)
  
  def poiMapper(line:String):poiRType={
    
    val field = line.split(",")
    val id = field(0)
    val lat = field(1).toFloat
    val long = field(2).toFloat    
    val poi = (id,(lat,long))
    return poi
  }
 
   
  type logType = (Timestamp,String,String,String,Float,Float)
  type logRType = (Int,logType)  
  
  def logMapper(line:String):logRType = {
    
    val field = line.split(",")
    val id = field(0).toInt
    val ts = getTimeStamp(field(1))
    val country = field(2)    
    val province = field(3)
    val city = field(4)
    val latt = field(5).toFloat
    val long = field(6).toFloat
    
    val log = (id,(ts,country,province,city,latt,long))
    return log
  }
  
  type lgType = (Int, (Long, Float, Float))
  type tagLgType = (Int, (Long, Float, Float,String))
  
  //This function computes the distance to any log-location to any poi
  def getDisttoPois(logLine:lgType,poiLine:poiRType):Double = {
    
    val coordsR:coords = (logLine._2._2,logLine._2._3)
    val coordsP:coords = (poiLine._2._1,poiLine._2._2)    
    val dist = getDist(coordsR,coordsP)
    
    return dist
  }
  
  def labelLogs(logs:RDD[(Int, (Long, Float, Float))], pois: RDD[processLogs.poiRType]):Map[Int,(String,Double,Float,Float)]={
    val logL = logs.count().toInt
    val poiL = pois.count().toInt
    println(f"unique logs found: $logL")
    //This loop calculates the distance from each record to its closest poi (in km)
    var taggedLog:Map[Int,(String,Double,Float,Float)]=Map()
    var logRow = logs.first()
    var lbl:String = ""
    var lat:Float =0.1f
    var long:Float =0.1f
    for(i<-0 until logL){ 
      if(i>0)logRow =  logs.zipWithIndex.filter(_._2==i).map(_._1).first()
      var dst:Double = 10000
      var poiRow = pois.first()
      for(j<-0 until poiL){
        if(j>0)poiRow = pois.zipWithIndex.filter(_._2==j).map(_._1).first()        
        val poiDst = getDisttoPois(logRow,poiRow)
        if(poiDst<dst){    
          dst = poiDst
          lbl = poiRow._1
          lat = poiRow._2._1
          long = poiRow._2._2
         }        
      }
      taggedLog += (logRow._1->(lbl,dst,lat,long))
      
      if(i%50==0){
          var prog = (i.toFloat/logL.toFloat)*100
          println(f"processing... $prog%.1f %%")
        }
    }    
    return taggedLog  		
  }
  
  
  
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    /*The next group of lines, creates spark session to ease input's dataset processing 
    * CAUTION!!!! CHANGE MASTER => master("local[*]") to your current ENVIRONM. CONDS.
    * FOR OPTIMIZATION PURPOSES 
    */
    
    val logsChart = SparkSession
    .builder
    .appName("processLogs")
    .master("local[*]")
    .getOrCreate()
      
    
    //Reading and creating POIs sc
    //val POIData = logsChart.sparkContext.textFile("../EQ_data/data/POIList.csv")
    val POIData = logsChart.sparkContext.textFile("POIList.csv")
    val poiHeaders = POIData.first()    
    val POIs  = POIData.filter(row => row != poiHeaders).map(poiMapper).cache()  
    
    
    //feeding the session with the csv data and mapping the content for each input row
    val logs = logsChart.sparkContext.textFile("DataSample.csv")
    //val logs = logsChart.sparkContext.textFile("../EQ_data/data/DataSample2.csv")
    
    
    //These two lines drop the headers (first row), keeping only logs info in the dataset
    val inputHeaders = logs.first()
    val LOGs  = logs.filter(row => row != inputHeaders).map(logMapper)
   
        
     //these three lines produced the cleaned RDD of logs 
    val sortedLogs = LOGs.map(x=> ((x._2._1).getTime, (x._1,x._2._5,x._2._6))).sortByKey(false)
    val tempLogs = sortedLogs.reduceByKey((x, y) =>(min(x._1,y._1),min(x._2,y._2),min(x._3,y._3)))
    val uniqueLogs = tempLogs.map(x=>(x._2._1,(x._1,x._2._2,x._2._3)))
    
    /* USING LINES OF INTEREST FOR sql spark statistical analysis*/
    val sc = logsChart.sparkContext
    
    //DISTANCE computation from logs to pois, with further results labeling
    val taggedLogs =  labelLogs(uniqueLogs,POIs).toSeq
    val logsRDD = sc.parallelize(taggedLogs,100).cache
    val distRDD =logsRDD.map(x=>(x._2._1,x._2._2,x._2._3,x._2._4))
   
    
    
    /* importing essential libs to handle datasets */
    import logsChart.implicits._
    val distDF = distRDD.toDF("POI", "Dist","lat","long")    
    distDF.createOrReplaceTempView("distTable")
    //distDF.printSchema()
        
    //running basics statistics on poi
    val poisStats = logsChart.sql("""SELECT POI, mean(Dist),stddev(Dist),count(Dist), mean(lat),mean(long)
                  FROM distTable
                  GROUP BY POI""").
                  toDF("POI","meanDist(km)","stdDist(km)","dCount","poiLatt","poiLong").
                  select($"POI",$"poiLatt",$"poiLong", $"meanDist(km)",$"stdDist(km)",$"dCount")
   
   poisStats.printSchema()
   poisStats.show()
   
   
   //java complained about vegas libs missing,and
   //despite this compiled, was not able tu run
   
   /* Vegas("MeanDist to Poi vs Poi_Locat").
           withDataFrame(poisStats).
           mark(Point).
           encodeX("poiLatt",Quant).
           encodeY("poiLong",Quant).
           encodeColor("POI",Nom).
           encodeSize("meanDist(km)",Quant).
           encodeShape("stdDist(km)",Quant).show   */
    
   
    
    logsChart.stop()
    
    
  }
  
}