package cs523.kafka;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.FastDateFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;

public class StockUtils {
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd:HH-mm-ss");

    public static Candle parseCandle(String str) throws ParseException {
        String[] sArr = str.split(",");
        try {
            return new Candle(
                    "AAPL",
                    Double.parseDouble(sArr[1]),
                    Double.parseDouble(sArr[2]),
                    Double.parseDouble(sArr[3]),
                    Double.parseDouble(sArr[4]),
                    Double.parseDouble(sArr[5]),
                    Integer.parseInt(sArr[6]),
                    simpleDateFormat.parse(sArr[0])
            );
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Error for :::: "+str);
        }
        return null;
    }

    public static Double webApi(String sym) throws IOException, ParseException, IOException {
        try {
            URL url = new URL("https://finance.yahoo.com/quote/AAPL?p=AAPL");//"https://finance.yahoo.com/quote/" + sym + "?p=" + sym);
            URLConnection con = url.openConnection();
            InputStream in = con.getInputStream();
            String encoding = con.getContentEncoding();
            encoding = encoding == null ? "UTF-8" : encoding;
            String body = IOUtils.toString(in, encoding);
            //System.out.println(body);
            File f = new File("body");
            FileWriter fw = new FileWriter(f);
            String json = body.substring(body.indexOf("root.App.main = ") + 16, body.indexOf("}(this));") - 2);
            fw.write(body);
            fw.write(json);
            fw.close();
            System.out.println(json);
            String pStr = body.substring(
                    (body.indexOf("\"currentPrice\":{\"raw\":") + "\"currentPrice\":{\"raw\":".length())
                    , body.indexOf(",\"fmt\"", body.indexOf("\"currentPrice\":{\"raw\":") + "\"currentPrice\":{\"raw\":".length()));
            //        JSONObject jsn = (JSONObject)new JSONParser().parse(json);
            //System.out.println(json.substring(json.indexOf("\"currentPrice\":{\"raw\":"), json.indexOf("\"currentPrice\":{\"raw\":")+60) + "...");
            Thread.sleep(100);
            return Double.parseDouble(pStr);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return 0.00;
    }

    public static String getSMM() {
        Date myDate = new Date();
        String str = fdf.format(myDate);
        return str;
    }

    public static String formatPrice(Double d){
        String str = Double.toString(d);
        return str.substring(0,Math.min(str.indexOf(".")+3,str.length()-1));
    }
}
