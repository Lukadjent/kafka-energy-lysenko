import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
/** At-most-once is implemented with plain clients, not an unsupported Streams option. */
public class DeliveryProbe {
  public static void main(String[] a)throws Exception{
    // bootstrap,prefix,group,mode,faultFile|none,resultFile
    Properties c=new Properties();c.put("bootstrap.servers",a[0]);c.put("group.id",a[2]);c.put("enable.auto.commit",false);c.put("auto.offset.reset","earliest");
    c.put("isolation.level","read_committed");c.put("max.poll.records",100);c.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");c.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    Properties p=new Properties();p.put("bootstrap.servers",a[0]);p.put("acks","all");p.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");p.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    boolean eos=a[3].equals("exactly_once");if(eos)p.put("transactional.id",a[2]+"-txn");
    long began=System.nanoTime(),rows=0;List<Double> samples=new ArrayList<>();
    try(KafkaConsumer<String,String> consumer=new KafkaConsumer<>(c);KafkaProducer<String,String> producer=new KafkaProducer<>(p)){
      if(eos)producer.initTransactions();consumer.subscribe(List.of(a[1]+"-input"));boolean assigned=false;
      while(true){ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(200));
        if(!consumer.assignment().isEmpty())assigned=true;
        if(records.isEmpty()){if(assigned){boolean done=true;var ends=consumer.endOffsets(consumer.assignment());for(var tp:consumer.assignment())if(consumer.position(tp)<ends.get(tp))done=false;if(done)break;}continue;}
        long start=System.nanoTime();Map<TopicPartition,OffsetAndMetadata> offsets=new HashMap<>();for(var r:records)offsets.put(new TopicPartition(r.topic(),r.partition()),new OffsetAndMetadata(r.offset()+1));
        if(eos)producer.beginTransaction();
        if(a[3].equals("at_most_once"))consumer.commitSync(offsets);
        boolean fault=!a[4].equals("none")&&!Files.exists(Path.of(a[4]));
        if(fault&&a[3].equals("at_most_once")){Files.writeString(Path.of(a[4]),"after offset commit, before send");Runtime.getRuntime().halt(17);}
        for(var r:records)producer.send(new ProducerRecord<>(a[1]+"-output",r.key(),r.value()));producer.flush();
        if(fault){Files.writeString(Path.of(a[4]),"after flush, before offset/transaction commit");Runtime.getRuntime().halt(17);}
        if(eos){producer.sendOffsetsToTransaction(offsets,consumer.groupMetadata());producer.commitTransaction();}else if(!a[3].equals("at_most_once"))consumer.commitSync(offsets);
        rows+=records.count();samples.add((System.nanoTime()-start)/1e6);
      }
    }
    new com.fasterxml.jackson.databind.ObjectMapper().writeValue(Path.of(a[5]).toFile(),Map.of("rows",rows,"seconds",(System.nanoTime()-began)/1e9,"batch_ms",samples,"mode",a[3]));
  }
}
