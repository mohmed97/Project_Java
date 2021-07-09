package Wuzzuf_PK;


import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

//@RestController
@Controller
public class DataController {
    @Autowired
    private SparkSession sparkSession;
    
    // Get DataFrameReader using SparkSession and set header option to true
    // to specify that first row in file contains name of columns
    
    // Read_File Function to read and clean data

    public DataController(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @RequestMapping("src\\main\\resources\\public\\index.html")
    public Dataset<Row> Read_File() {
        
        DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
        
        Dataset<Row> trainingData = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");
        trainingData = trainingData.na().drop().distinct();
        return trainingData;
    }
    
    @GetMapping("/Show")
    public ResponseEntity<String> Show_traningData(){
        
        Dataset<Row> rowDataset = this.Read_File();
        Dataset<String> stringDataset = rowDataset.toJSON();
        String s = stringDataset.showString(100, 5000, false);
        String[] x = s.split("\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < x.length; i += 2)
            try {
                sb.append(x[i].split("\\|")[1] + "<br>");
            } catch (Exception e) {

            }
        return ResponseEntity.ok(sb.toString());
    }
   
    @GetMapping("src\\main\\resources\\public\\pieChart.html")
    public void Counter_Job_per_Company (){
        Dataset<Row> trainingData = this.Read_File();
        trainingData.createOrReplaceTempView("TRAINING_DATA");
        Dataset<Row> CounterperJob = sparkSession.sql("SELECT Company , count(Title) as Counter From TRAINING_DATA "+
                "GROUP BY Company ORDER by Counter desc");
        
        List<String> CompanyList = CounterperJob.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> CounterList = CounterperJob.select("Counter").limit(10).as(Encoders.LONG()).collectAsList();
        try {
            // Create Chart
            PieChart chart = new PieChartBuilder ().width (1024).height (768).title ("Counter JobsperCompany").build ();
            // Customize Chart
            chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
            chart.getStyler ().setHasAnnotations (true);
            // Series
            for(int i = 0 ; i<CompanyList.size() ; i++){
                chart.addSeries (CompanyList.get(i), CounterList.get(i));
            }
             // Show it
            BitmapEncoder.saveBitmap(chart, "./src/main/resources/public/Graphs/" + "Counter JobsperCompany", BitmapEncoder.BitmapFormat.JPG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    public void Draw_Chart(List<String> x ,List<Long> y,String t){
        try {
            CategoryChart Cachart = new CategoryChartBuilder ().width (1024).height (768).title (t).xAxisTitle ("Names").yAxisTitle ("Counter").build ();
            // Customize Chart
            Cachart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
            Cachart.getStyler ().setHasAnnotations (true);
            Cachart.getStyler ().setStacked (true);
            // Series

            Cachart.addSeries ("Jobs Counter",x, y);

             // Show it
            BitmapEncoder.saveBitmap(Cachart, "./src/main/resources/public/Graphs/" + t, BitmapEncoder.BitmapFormat.JPG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    @GetMapping("src\\main\\resources\\public\\HistogramChart (1).html")
    public void Counter_Job (){

        Dataset<Row> trainingData = this.Read_File();
        trainingData.createOrReplaceTempView("TRAINING_DATA");
        
        Dataset<Row> PopularJob = sparkSession.sql("SELECT Title , count(Title) as Counter From TRAINING_DATA "+
                "GROUP BY Title ORDER by Counter desc");
        
        PopularJob.show(1);
        
        List<String> JobList = PopularJob.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> CounterList = PopularJob.select("Counter").limit(10).as(Encoders.LONG()).collectAsList();
        
        
        Draw_Chart(JobList , CounterList ,"Jobs Counter");

    }

    @GetMapping("src\\main\\resources\\public\\HistogramChart (2).html")
    public void Counter_Job_per_Area (){
        

        Dataset<Row> trainingData = this.Read_File();
        trainingData.createOrReplaceTempView("TRAINING_DATA");
        
         Dataset<Row> PopularArea = sparkSession.sql("SELECT Location , count(Title) as Counter From TRAINING_DATA "+
                "GROUP BY Location ORDER by Counter desc");
        
        PopularArea.show(1);
        
        List<String> AreaList = PopularArea.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> CounterArea = PopularArea.select("Counter").limit(10).as(Encoders.LONG()).collectAsList();
        
        
        
        Draw_Chart(AreaList , CounterArea ,"PopularArea");
    }
    
    @RequestMapping("r")
    public ResponseEntity<String> Counter_Skill(){
        Dataset<Row> trainingData = this.Read_File();
         Dataset<Job_Title> jobsDataset = trainingData.as(Encoders.bean(Job_Title.class));
        Dataset<Row> skillsData = jobsDataset.select(col("Skills"));
        List<String> skillsDataStr = skillsData.as(Encoders.STRING()).collectAsList();

        Map<String, Integer> skillCount = new HashMap<String, Integer>();
        for (String sk : skillsDataStr)
        {
            for(String s: sk.split(","))
            {
                String key = s.trim().toLowerCase();
                skillCount.put(key, skillCount.getOrDefault(key,1) + 1);
            }
        }

        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();

        skillCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .forEachOrdered(x -> sortedMap.put(x.getKey() , x.getValue()) );
        String html = String.format("<h3>%s</h3>", "Display The Counter skills") +
                skillCount.toString();

        return ResponseEntity.ok(html); 
    }
      
    
}
