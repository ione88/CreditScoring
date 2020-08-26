package main.java.com.credit_scoring.CreditScoring;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class BureauScoreCalculator {
	
	private SparkSession spark;
	
	public BureauScoreCalculator() {
		this.spark = SparkSession.builder().appName("Credit Scoring").config("spark.master", "local").getOrCreate();
		StructType schema = new StructType()
			    .add("id", "int")
			    .add("delinq_2yrs", "int")
			    .add("inq_last_6mths", "int")
			    .add("mths_since_last_delinq", "int")
			    .add("mths_since_last_record", "int")
			    .add("open_acc", "int")
		        .add("pub_rec", "int")
		        .add("revol_bal", "int")
		        .add("revol_util", "float")
		        .add("total_acc", "int")
		        .add("earliest_cr_line", "string");
		
		Dataset<Row> df = spark.read()
			    .option("mode", "DROPMALFORMED")
			    .schema(schema)
			    .csv("./BureauData.csv");
		
		df.createOrReplaceTempView("bureau");
	}
		
	public int generateBureauScore(int id) {
		Dataset<Row> sqlResult = spark.sql("SELECT * FROM bureau where id=" + id + "");
		List<Row> rows = sqlResult.collectAsList();
		Row row = rows.get(0);
		return this.getBureauScore((Integer)row.get(1), (Integer)row.get(2), (Integer)row.get(3), 
									(Integer)row.get(5), (Integer)row.get(6), (Float)row.get(8));
	}
	
	public int getBureauScore(int delinq_2yrs, int inq_last_6mths, int mths_since_last_delinq, int open_acc,
								int pub_rec, double revol_util) {
		int bureau_score = 0;
		
		if (delinq_2yrs == 0)
			bureau_score += 53;
		else if (delinq_2yrs == 1)
			bureau_score += 21;
		
		if (inq_last_6mths == 0)
			bureau_score += 60;
		else if (inq_last_6mths == 1 || inq_last_6mths == 2)
			bureau_score += 35;
		
		if (mths_since_last_delinq > 18 && mths_since_last_delinq <= 36)
			bureau_score += 12;
		else if (mths_since_last_delinq > 36 && mths_since_last_delinq <= 54)
			bureau_score += -33;
		else if (mths_since_last_delinq > 54 && mths_since_last_delinq < 120)
			bureau_score += -18;
		else if (mths_since_last_delinq >= 120)
			bureau_score += -4;
		
		if (open_acc <= 6)
			bureau_score += -19;
		else if (open_acc > 6 && open_acc <=10)
			bureau_score += -29;
		else if (open_acc > 10 && open_acc <= 14)
			bureau_score += -19;
		else if (open_acc > 14 && open_acc <= 20)
			bureau_score += -34;
		else if (open_acc > 20 && open_acc <= 30)
			bureau_score += -35;
		
		if (pub_rec == 0)
			bureau_score += 39;
		
		if (revol_util <= 0.1)
			bureau_score += 50;
		else if (revol_util > 0.1 && revol_util <= 0.2)
			bureau_score += 22;
		else if (revol_util > 0.2 && revol_util <= 0.3)
			bureau_score += 26;
		else if (revol_util > 0.3 && revol_util <= 0.4)
			bureau_score += 16;
		else if (revol_util > 0.4 && revol_util <= 0.5)
			bureau_score += -4;
		else if (revol_util > 0.5 && revol_util <= 0.6)
			bureau_score += 8;
		else if (revol_util > 0.6 && revol_util <= 0.7)
			bureau_score += -19;
		else if (revol_util > 0.7 && revol_util <= 0.8)
			bureau_score += -30;
		else if (revol_util > 0.8 && revol_util <= 0.9)
			bureau_score += -31;
		else if (revol_util > 0.9 && revol_util <= 1.0)
			bureau_score += -47;
		
		return bureau_score;
	}
	
}
