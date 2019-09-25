package cs523.kafka;

import java.io.Serializable;
import java.util.Date;

public class Candle implements Serializable {
    String symbol;
    Double open;
    Double high;
    Double low;
    Double close;
    Double adjClose;
    Double current=0.0;
    Integer vol;
    Date date;
    Long time=0l;
    //indicators
    Double MA_L=0.0;
    Double MA_S=0.0;
    Double sum=0.0;
    Integer count=0;

    public Candle(){}

    public Candle(Double curr){
        this.current = curr;
        this.MA_L = curr;
        this.MA_S = curr;
        time = System.nanoTime();
    }

    public Candle(String sym, Double op, Double hi, Double lw, Double cl, Double adj, Integer vl, Date dt){
        symbol = sym;
        open = op;
        high = hi;
        low = lw;
        close = cl;
        adjClose = adj;
        vol = vl;
        date = dt;
    }

    public Double getAvg(){
        return (open+close)/2;
    }

    public Candle setMA_L(Double ma){
        this.MA_L= ma;
        return this;
    }

    public Double getMA_L(){
        return MA_L;
    }

    public Candle setSum(Double sm, int cnt){
        this.sum = sm;
        count = cnt;
        return this;
    }

    public Candle setCurrent(Double curr){
        this.current = curr;
        return this;
    }

    public Double getSum(){
        return sum;
    }

    public Integer getCnt(){
        return count;
    }

    @Override
    public String toString() {
        return "Candle{" +
                "symbol='" + symbol + '\'' +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", adjClose=" + adjClose +
                ", vol=" + vol +
                ", date=" + date +
                ", MA_L=" + MA_L +
                ", MA_S=" + MA_S +
                ", sum=" + sum +
                ", count=" + count +
                '}';
    }
}
