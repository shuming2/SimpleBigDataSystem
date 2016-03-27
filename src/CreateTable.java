
import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster;


import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.*;

public class CreateTable {
    public static void main(String[] args) {
        String create = "";

        //Cluster cluster = Cluster.builder().withPort(9101).addContactPoint("10.2.2.143").build();
        Cluster cluster = Cluster.builder().withPort(9101).addContactPoint("162.246.157.170").build();

        Session session = cluster.connect();


        session.execute("CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        Session kpSession = cluster.connect("mykeyspace");


        // transfer the sql to string
        try {
            InputStream r = new FileInputStream("src/bigdata_setup1.sql");
            ByteArrayOutputStream byteout = new ByteArrayOutputStream();
            byte tmp[] = new byte[1];
            byte context[];
            int i = 0;
            while ((i = r.read(tmp)) != -1) {
                byteout.write(tmp);
            }
            context = byteout.toByteArray();
            create = new String(context, "UTF-8");


            kpSession.execute(create);
        } catch (Exception e) {
            System.out.println("Error: Creating table fails");
        }


        kpSession.close();
        session.close();
        cluster.close();

    }


}
