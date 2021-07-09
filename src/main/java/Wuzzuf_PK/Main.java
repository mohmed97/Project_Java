package Wuzzuf_PK;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;



@SpringBootApplication

public class Main extends SpringBootServletInitializer {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
        
        final SparkSession sparkSession = SparkSession.builder().appName("Wuzzuf Spark Demo")
                .master("local[2]").getOrCreate();
        
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        DataController s = new DataController(sparkSession); 
        
        s.Counter_Job();

        s.Counter_Job_per_Area();

        s.Counter_Job_per_Company();

        
    }

 
}
