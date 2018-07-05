# Landmark labeling using (Java) Scala-Spark
Cleaning, labeling and statistical description of data

In this example, the location and time stamp of various thousand of people is given using longitude and Lattitude. The task consists of cleaning the data to avoid duplicates, and then measure the distance to each of the provided  (POIs) points of interest location. Each label is set based on the closeness of each individual to any of the POIs.
Finally, using SQL-like commands organize the results and  run basic statistics on the outputs, looking at the location of each POI and the radius of the circle in which relatively close subjects are located, for this purpose the location and the standard deviation of the distance is used.

The distance is calculated using Haversines law

Please, run the code included in this folder, and make sure that the csv data, (POIList.csv and DataSample.csv),
are located along the Jar file.

 To run the code type >>spark-submit  processlogs_2.11-1.0.jar 
