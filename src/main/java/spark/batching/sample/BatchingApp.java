package spark.batching.sample;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class BatchingApp {

    public static void main(String[] args) {

        // Prepare Spark Session
        SparkConf conf = new SparkConf()
                .setAppName("SparkBatchingProject")
                .setMaster("local[*]");
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkBatchingProject")
                .config(conf)
                .getOrCreate();

        // Read avro from HDFS
        Dataset<Row> expDS = spark.read().format("avro")
                .option("recursiveFileLookup","true")
                .load("hdfs://localhost:9000/input");

        // Read Hotel csv from Kafka topic
        Dataset<Row> kafkaDS = spark.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "hotels")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();
        StructType hotelSchema = Encoders.bean(Hotel.class).schema();
        Dataset<Row> hotelDS = kafkaDS.withColumn("data", functions.from_json(kafkaDS.col("value").cast("string"), hotelSchema))
                .select("data.*");

        // Calculate date differences
        Dataset<Row> sortedDS= expDS.sort("hotel_id","date_time");
        WindowSpec window = Window.orderBy("hotel_id","date_time");
        Column date_time = lag("date_time", 1).over(window);
        Dataset<Row> dateDiffDS = sortedDS
                .withColumn("previous_date", date_time)
                .withColumn("date_diff", datediff(col("date_time"), col("previous_date")));

        // Select Invalid hotels
        Dataset<Row> invalidHotelsDS = dateDiffDS
                .filter(dateDiffDS.col("date_diff").$greater$eq(2))
                .filter(dateDiffDS.col("date_diff").$less(30));
        invalidHotelsDS
                .join(hotelDS, invalidHotelsDS.col("hotel_id").equalTo(hotelDS.col(("Id"))))
                .dropDuplicates("Name", "City", "Address")
                .select("Name", "City", "Address")
                .show();

        // Select Valid hotels
        Dataset<Row> validHotelsLowDS = dateDiffDS
                .filter(dateDiffDS.col("date_diff").$less(2));
        Dataset<Row> validHotelsHighDS = dateDiffDS
                .filter(dateDiffDS.col("date_diff").$greater(30));
        Dataset<Row> validHotelsDS = validHotelsLowDS.union(validHotelsHighDS);
        Dataset<Row> validJoinDS = validHotelsDS.join(hotelDS, validHotelsDS.col("hotel_id").equalTo(hotelDS.col(("Id"))));

        // Print booking counts
        validJoinDS.select(countDistinct("Country"))
                .show();
        validJoinDS.select(countDistinct("City"))
                .show();

        // Store in HDFS
        validJoinDS
                .drop("Id") // remove duplicate column
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("srch_ci")
                .parquet("hdfs://localhost:9000/output");

        // Close Session
        spark.close();
    }

}
