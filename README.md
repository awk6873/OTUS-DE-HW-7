# boston_crimes_map
Simple Scala/Spark applicaton for learning Spark purposes. Reads two CSV files, joins them and calculates some aggregates.

# Running
spark-submit --master local[*] --class org.awk.BostonCrimesMap boston_crimes_map-assembly-0.0.1.jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}

# Result
parquet file at {path/to/output_folder} directory 
