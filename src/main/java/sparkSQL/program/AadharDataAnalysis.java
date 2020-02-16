package sparkSQL.program;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class AadharDataAnalysis {
	public static void main(String[] args) {
		
		String inputPath = args[0];
		String outputPath = args[1];
		SparkSession spark = SparkSession.builder().appName("AadharDataAnalysis").master("local").getOrCreate();

		Dataset<Row> df = spark.read().format("csv").option("header", "true").option("inferschema", "true")
				.option("nullValue", "").option("mode", "DROPMALFORMED")
				.load(inputPath);

		df.createOrReplaceTempView("temp");

		spark.sql(
				"SELECT EnrolmentAgency , SUM(AadhaarGenerated) as aadharSum FROM temp GROUP BY EnrolmentAgency ORDER BY aadharSum DESC LIMIT 5")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Top5EnrollmentAgencies/");

		spark.sql(
				"SELECT District , SUM(AadhaarGenerated) as aadharSum FROM temp GROUP BY District ORDER BY aadharSum DESC LIMIT 10")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Top10IdentityGeneratedDistricts/");

		spark.sql(
				"SELECT District , SUM(AadhaarGenerated) as aadharSum FROM temp GROUP BY District ORDER BY aadharSum LIMIT 10")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Bottom10IdentityGeneratedDistricts/");

		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender NOT IN ('T') GROUP BY State ORDER BY aadharSum DESC LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Top3IdentityGeneratedStates/");

		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender NOT IN ('T') GROUP BY State ORDER BY aadharSum LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Bottom3IdentityGeneratedStates/");

		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender = 'M' GROUP BY State ORDER BY aadharSum DESC LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Top3IdentityGeneratedStatesForMale/");
		
		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender = 'M' GROUP BY State ORDER BY aadharSum LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Bottom3IdentityGeneratedStatesForMale/");
		
		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender = 'F' GROUP BY State ORDER BY aadharSum DESC LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Top3IdentityGeneratedStatesForFemale/");
		
		spark.sql(
				"SELECT State , COUNT(AadhaarGenerated) as aadharSum FROM temp WHERE Gender = 'F' GROUP BY State ORDER BY aadharSum LIMIT 3")
				.write().format("com.databricks.spark.csv").option("delimiter", "|")
				.option("timestampFormat", "yyyy-MM-dd")
				.save(outputPath+"/Bottom3IdentityGeneratedStatesForFemale/");

		// df.show();

	}
}
