package cs523.kafka;

import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

/*Author:Sujiv Shrestha
 * Date Created:2019-09-19
 */
public class MyKafkaProducer
{
    static File logFile = new File("MyLogger");
    static OutputStreamWriter bfWrite;
    static String output = "/output";
    static Long time = System.nanoTime();
    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    static Queue<String> pricesQ = new LinkedList();
    static Double MAL = 0.0;
    static Double MAS = 0.0;

    public MyKafkaProducer() {
    }

    public static void main( String[] args ) throws InterruptedException, ExecutionException, IOException, SQLException, ClassNotFoundException {
//        try {
//            StockUtils.webApi("AAPL");
//            return;
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
        String jobName = args[0];
        String port = args[1];
        System.out.println("Arguments:"+Arrays.toString(args));
        bfWrite = new OutputStreamWriter(new FileOutputStream(logFile));
        //MyTable.create("");
        if(jobName.contains("s")) {//for stream from file input
            sparkStream(port);
            System.out.println("spark streaming");
        }
        if(jobName.contains("c")) {
            sparkStream2(port);
            System.out.println("spark streaming for online stream");
        }
        if(jobName.contains("k")) {
            String file = args[2];
            startKafka(port,file);//"/input/AAPL.csv"
            System.out.println("kafka stream out");
        }
        if(jobName.contains("o")) {
            startKafkaOnline(args[3],args[1],args[2]);
            System.out.println("kafka stream from online");
        }
    }
    public static void sparkStream(String PORT){
        Map<String,String> kProp = new HashMap<String,String>();

        {
            kProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:"+PORT);
            kProp.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer");
            kProp.put(ProducerConfig.ACKS_CONFIG, "all");
            kProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }
        JavaStreamingContext sparkSC = new JavaStreamingContext(new SparkConf().setAppName("test").setMaster("local[*]"), new Duration(1000));

        final String topic = "kafka-topic1";
        Collection<String> topics = Collections.singleton(topic);
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
                sparkSC,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kProp, (Set<String>) topics
        );

//        JavaPairDStream<Date,Candle> dStream = stream.map(f->StockUtils.parseCandle(f._2))
//                                            .mapToPair(c->new Tuple2<Date,Candle>(c.date,c))
//                                            .reduceByKeyAndWindow((a,b)->b.setSum(a.getSum()+b.getAvg(), a.getCnt()+1),new Duration(2000),new Duration(1000));
//                                            //.map(c->c._2);//._2.setMA_L(c._2.getSum()/c._2.getCnt()));
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
//        dStream.foreachRDD(
//                (VoidFunction<JavaPairRDD<Date, Candle>>) rdd->rdd.foreach(
//                        r-> {
//                            if (rdd.count() == r._2.count)
//                                r._2.setMA_L(r._2.getSum() / r._2.count);
//                            System.out.println("MA on " + sdf.format(r._2.date) + ":" + r._2.getMA_L());
//                            update(r);
//                        })
//        );
        JavaDStream<Candle> dStream = stream.map(f->StockUtils.parseCandle(f._2))
                .map(f->f.setCurrent(f.getAvg()));
        dStream.window(new Duration(5000),new Duration(1000))
                .foreachRDD(
                        (VoidFunction<JavaRDD<Candle>>)rdd->{
                            Tuple3<Candle,Double,Integer> t2 = rdd.map(a->new Tuple3<Candle,Double,Integer>(a,a.current,1))
                                    .reduce((a,b)->new Tuple3<Candle,Double,Integer>(b._1(),a._2()+b._2(),a._3()+b._3()));
                            logger(Double.toString(t2._1().open)
                                    +","+Double.toString(t2._1().high)
                                    +","+Double.toString(t2._1().low)
                                    +","+Double.toString(t2._1().close)
                                    +","+Double.toString(t2._1().current)
                                    +",,"+Double.toString(t2._2()/t2._3())+",data="+t2._3());
                        }
                );

        dStream.window(new Duration(20000),new Duration(1000))
                .foreachRDD(
                        (VoidFunction<JavaRDD<Candle>>)rdd->{
                            Tuple3<Candle,Double,Integer> t2 = rdd.map(a->new Tuple3<Candle,Double,Integer>(a,a.current,1))
                                    .reduce((a,b)->new Tuple3<Candle,Double,Integer>(b._1(),a._2()+b._2(),a._3()+b._3()));
                            logger(Double.toString(t2._1().open)
                                    +","+Double.toString(t2._1().high)
                                    +","+Double.toString(t2._1().low)
                                    +","+Double.toString(t2._1().close)
                                    +","+Double.toString(t2._1().current)
                                    +","+Double.toString(t2._2()/t2._3())+",,data="+t2._3());
                        }
                );
        sparkSC.start();
        sparkSC.awaitTermination();
        System.out.println("Spark Stream Starts..");
    }

    private static void update(Tuple2<Date, Candle> r) {
        System.out.println("From update:"+sdf.format(r._1)+":"+r._2);
    }

    private static void logger(String str) throws IOException {
        String[] data = str.split(",");
        if(!data[5].isEmpty()){
            MAL = Double.parseDouble(data[5]);
        }
        if(!data[6].isEmpty()){
            MAS = Double.parseDouble(data[6]);
        }
        String dataLine = StockUtils.getSMM()
                +"," +data[0]
                +"," +data[1]
                +"," +data[2]
                +"," +data[3]
                +"," +data[4]
                +","+StockUtils.formatPrice(MAL)
                +","+StockUtils.formatPrice(MAS)
                +","+data[7];
        String action = ((MAL*1.1>MAS)&&(MAL<MAS))?"Recommend: BUY":((MAS<MAL)&&(MAS*1.1>MAL))?"Recommend: SELL":"Recommend: WAIT";
        bfWrite.append(dataLine+" "+action+"\r\n");
        System.out.println(dataLine+" "+action);
        //SparkSQLUtil.addRow(dataLine, SparkSQLUtil.TName1);
        if((System.nanoTime()-time)/1000>1000){
            bfWrite.flush();
            time = System.nanoTime();
        }
    }

    private static void logger2(String str) throws IOException {
        String[] data = str.split(",");
        if(!data[1].isEmpty()){
            MAL = Double.parseDouble(data[1]);
        }
        if(!data[2].isEmpty()){
            MAS = Double.parseDouble(data[2]);
        }
        String dataLine = StockUtils.getSMM()
                +"," +data[0]
                +","+StockUtils.formatPrice(MAL)
                +","+StockUtils.formatPrice(MAS)
                +","+data[3];
        String action = (MAS>MAL*1.05)?"Recommend: BUY":(MAS<MAL)?"Recommend: SELL":"Recommend: WAIT";
        bfWrite.append(dataLine+" "+action+"\r\n");
        System.out.println(dataLine+" "+action);
        //SparkSQLUtil.addRow(dataLine, SparkSQLUtil.TName1);
        if((System.nanoTime()-time)/1000>1000){
            bfWrite.flush();
            time = System.nanoTime();
        }
    }

    public static void sparkStream2(String PORT){
        Map<String,String> kProp = new HashMap<String,String>();

        {
            kProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:"+PORT);
            kProp.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer");
            kProp.put(ProducerConfig.ACKS_CONFIG, "all");
            kProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local[*]"));
        JavaStreamingContext sparkSC = new JavaStreamingContext(sparkContext, new Duration(200));

        final String topic2 = "kafka-topic2";
        Collection<String> topics = Collections.singleton(topic2);
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
                sparkSC,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kProp, (Set<String>) topics
        );

        JavaDStream<Candle> dStream = stream.map(f->new Candle(Double.parseDouble(f._2)));
        dStream.window(new Duration(5000),new Duration(1000))
                .foreachRDD(
                        (VoidFunction<JavaRDD<Candle>>)rdd->{
                            Tuple3<Candle,Double,Integer> t2 = rdd.map(a->new Tuple3<Candle,Double,Integer>(a,a.current,1))
                                    .reduce((a,b)->new Tuple3<Candle,Double,Integer>(b._1(),a._2()+b._2(),a._3()+b._3()));
                            logger2(Double.toString(t2._1().current)+",,"+Double.toString(t2._2()/t2._3())+",data="+t2._3());
                        }
                );

        dStream.window(new Duration(20000),new Duration(1000))
                .foreachRDD(
                        (VoidFunction<JavaRDD<Candle>>)rdd->{
                            Tuple3<Candle,Double,Integer> t2 = rdd.map(a->new Tuple3<Candle,Double,Integer>(a,a.current,1))
                                    .reduce((a,b)->new Tuple3<Candle,Double,Integer>(b._1(),a._2()+b._2(),a._3()+b._3()));
                            logger2(Double.toString(t2._1().current)+","+Double.toString(t2._2()/t2._3())+",,data="+t2._3());
                        }
                );

        //dStream.print();
        sparkSC.start();
        sparkSC.awaitTermination();
        System.out.println("Spark Stream Starts..");
    }

    public static void startKafka(String PORT, String file) throws ExecutionException, InterruptedException, FileNotFoundException {
        String TOPIC = "kafka-topic1";
        Properties kProp = new Properties();
        kProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:"+PORT);
        kProp.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer");
        kProp.put(ProducerConfig.ACKS_CONFIG, "all");
        kProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
        kProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Long, String> streamProducer = new KafkaProducer<Long, String>(kProp);
        System.out.println("kafka Stream Starts..");

        Runnable kafkaFileReader = new Runnable(){
            String filename = new File("").getAbsolutePath()+file;
            {
                System.out.println(filename);
            }
            BufferedReader br = new BufferedReader(new FileReader(filename));
            @Override
            public void run() {
                String line = "";
                try {
                    br.readLine();
                    while((line = br.readLine())!=null) {
                        if(line.split(",").length==7)
                        streamProducer.send(new ProducerRecord<>(TOPIC, line));
                        System.out.println(line);
                        Thread.sleep(50);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread kafkaTh = new Thread(kafkaFileReader);
        kafkaTh.start();
    }

    public static void startKafkaOnline(String sym, String PORT, String mode) throws InterruptedException {
        System.out.println("kafka Online Stream Starts..");
        String TOPIC = "kafka-topic2";

        Properties kProp = new Properties();
        kProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:"+PORT);
        kProp.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer2");
        kProp.put(ProducerConfig.ACKS_CONFIG, "all");
        kProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        kProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Long, Double> streamProducer = new KafkaProducer<Long, Double>(kProp);

        Runnable kafkaReader = new Runnable(){

            @Override
            public void run() {
                boolean flag = true;
                Integer strt = 0;
                Double price = 0.0;//(mode.equalsIgnoreCase("mock")) ? 180 + 50 * Math.sin(strt * Math.PI / 180 / 2) + Math.random() * 15 : StockUtils.webApi(sym);
                String priceStr = "0.00";
                while (flag) {
                    try {
                        strt++;
                        strt = strt % (360 * 2);
                        price = (mode.equalsIgnoreCase("mock")) ? 180 + 50 * Math.sin(strt * Math.PI / 180 / 2) + Math.random() * 15 : StockUtils.webApi(sym);
                        priceStr = StockUtils.formatPrice(price);//.substring(0, priceStr.indexOf(".") + 3);
//                            streamProducer.send(new ProducerRecord(TOPIC, Long.toString(System.nanoTime()), priceStr));
                        pricesQ.add(priceStr);
                        System.out.println(priceStr+"-->updated");
                        Thread.sleep(500);
                    } catch (Exception e) {
                        e.getStackTrace();
                    }
                }
            }
        };
        Thread kafkaTh = new Thread(kafkaReader);
        kafkaTh.start();
        boolean flag = true;
        String priceString = "0.00";
        String priceString2= "0.00";
        while (flag) {
            priceString2 = pricesQ.poll();
            if(pricesQ.size()>1000){
                pricesQ = new LinkedList<String>();
            }
            if(priceString2==null)
                priceString2 = priceString;
            streamProducer.send(new ProducerRecord(TOPIC, Long.toString(System.nanoTime()), priceString2));
            String myDateString = StockUtils.getSMM();
            System.out.println(myDateString+": current price:"+priceString2);
            priceString = priceString2;
            Thread.sleep(1000);
        }
    }
}
