import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

/** Real persistent RocksDB hopping windows; input/output are UTF-8 JSON. */
public class HydroStreams {
  static final ObjectMapper JSON=new ObjectMapper();
  public static class Event {
    public String id,station; public long ts; public boolean tick;
    public double upper,lower,flow,power,gate,efficiency;
  }
  public static class Ref { public double target,minFlow,maxFlow,weight; }
  public static class Station {
    public long n,firstTs=Long.MAX_VALUE,lastTs=Long.MIN_VALUE;
    public double flow,upper,power,efficiency,firstUpper,lastUpper,target,minFlow,maxFlow,weight;
    void add(Event e,Ref r) {
      n++;flow+=e.flow;upper+=e.upper;power+=e.power;efficiency+=e.efficiency;
      if(e.ts<firstTs){firstTs=e.ts;firstUpper=e.upper;}
      if(e.ts>=lastTs){lastTs=e.ts;lastUpper=e.upper;}
      target=r.target;minFlow=r.minFlow;maxFlow=r.maxFlow;weight=r.weight;
    }
  }
  public static class Enriched { public Event event; public Ref ref; }
  public static class Aggregate {
    public long n; public Map<String,Station> stations=new TreeMap<>();
    public Aggregate add(Enriched value) {
      if(!value.event.tick){n++;stations.computeIfAbsent(value.event.station,k->new Station()).add(value.event,value.ref);}
      return this;
    }
  }
  static <T> T parse(String value,Class<T> type) {try{return JSON.readValue(value,type);}catch(Exception e){throw new RuntimeException(e);}}
  static String json(Object value){try{return JSON.writeValueAsString(value);}catch(Exception e){throw new RuntimeException(e);}}
  static <T> Serde<T> serde(Class<T> type){return Serdes.serdeFrom((topic,value)->value==null?null:json(value).getBytes(java.nio.charset.StandardCharsets.UTF_8),
    (topic,bytes)->bytes==null?null:parse(new String(bytes,java.nio.charset.StandardCharsets.UTF_8),type));}
  static String output(Windowed<String> key,Aggregate a){
    Map<String,Object> result=new TreeMap<>();result.put("start",key.window().start());result.put("end",key.window().end());result.put("count",a.n);
    Map<String,Object> stations=new TreeMap<>();double weighted=0,weights=0,lo=0,hi=Double.MAX_VALUE;
    for(var entry:a.stations.entrySet()){
      Station s=entry.getValue();double slope=s.lastTs==s.firstTs?0:(s.lastUpper-s.firstUpper)/(s.lastTs-s.firstTs);
      double prediction=s.lastUpper+slope*600000;
      double desired=s.flow/s.n+20*(prediction-s.target);
      weighted+=s.weight*desired;weights+=s.weight;lo=Math.max(lo,s.minFlow);hi=Math.min(hi,s.maxFlow);
      Map<String,Object> m=new TreeMap<>();m.put("count",s.n);m.put("mean_flow",s.flow/s.n);m.put("mean_upper",s.upper/s.n);
      m.put("mean_power",s.power/s.n);m.put("mean_efficiency",s.efficiency/s.n);m.put("prediction_10m",prediction);m.put("desired_flow",desired);
      m.put("weight",s.weight);stations.put(entry.getKey(),m);
    }
    double q=Math.max(lo,Math.min(hi,weighted/weights)),objective=0;
    for(Object obj:stations.values()){Map<?,?> s=(Map<?,?>)obj;objective+=(double)s.get("weight")*Math.pow(q-(double)s.get("desired_flow"),2);}
    result.put("stations",stations);result.put("coordinated_flow",q);result.put("objective",objective);
    return json(result);
  }
  public static void main(String[] args)throws Exception{
    // bootstrap, prefix, appId, guarantee, stateDir, readyFile, faultFile|none
    Properties p=new Properties();p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,args[0]);p.put(StreamsConfig.APPLICATION_ID_CONFIG,args[2]);
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,args[3]);p.put(StreamsConfig.STATE_DIR_CONFIG,args[4]);
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class);p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class);
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,100);p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,1);
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,1);p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG,0);
    p.put("consumer.auto.offset.reset","earliest");p.put("producer.transaction.timeout.ms",60000);
    StreamsBuilder b=new StreamsBuilder();String prefix=args[1];
    GlobalKTable<String,Ref> refs=b.globalTable(prefix+"-reference",Consumed.with(Serdes.String(),serde(Ref.class)));
    KStream<String,String> raw=b.stream(prefix+"-input",Consumed.with(Serdes.String(),Serdes.String()).withTimestampExtractor((record,prior)->parse((String)record.value(),Event.class).ts));
    KStream<String,Event> events=raw.mapValues(value->parse(value,Event.class));
    events.filter((key,e)->!e.tick&&(e.flow<=0||e.upper<=e.lower||e.efficiency<=0||e.efficiency>1)).mapValues(HydroStreams::json).to(prefix+"-invalid");
    KStream<String,Event> valid=events.filter((key,e)->e.tick||(e.flow>0&&e.upper>e.lower&&e.efficiency>0&&e.efficiency<=1));
    valid.filter((key,e)->!e.tick&&(e.upper>95||e.efficiency<.88)).mapValues(HydroStreams::json).to(prefix+"-anomaly");
    KStream<String,Enriched> joined=valid.join(refs,(key,e)->e.station,(e,r)->{Enriched x=new Enriched();x.event=e;x.ref=r;return x;});
    KTable<Windowed<String>,Aggregate> windows=joined.groupByKey(Grouped.with(Serdes.String(),serde(Enriched.class)))
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(30),Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(10)))
      .aggregate(Aggregate::new,(key,value,a)->a.add(value),Materialized.<String,Aggregate,org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes,byte[]>>as("hydro-history")
        .withKeySerde(Serdes.String()).withValueSerde(serde(Aggregate.class)))
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
    final int[] emitted={0};
    windows.toStream().filter((key,a)->a.n>0).peek((key,a)->{
      if(!args[6].equals("none")&&++emitted[0]==3&&!Files.exists(Path.of(args[6]))) {
        try{Files.writeString(Path.of(args[6]),"halt after state update, before output/transaction commit");}catch(Exception ex){throw new RuntimeException(ex);}
        Runtime.getRuntime().halt(17);
      }
    }).map((key,a)->KeyValue.pair("DNIPRO|"+key.window().start(),output(key,a))).to(prefix+"-output",Produced.with(Serdes.String(),Serdes.String()));
    KafkaStreams streams=new KafkaStreams(b.build(),p);
    streams.setStateListener((state,old)->{System.out.println("STATE "+state);if(state==KafkaStreams.State.RUNNING)try{Files.writeString(Path.of(args[5]),"RUNNING");}catch(Exception e){throw new RuntimeException(e);}});
    Runtime.getRuntime().addShutdownHook(new Thread(()->streams.close(Duration.ofSeconds(20))));streams.start();
  }
}
