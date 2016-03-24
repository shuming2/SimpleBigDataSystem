import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;

public class InsertTable {
    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoint("10.2.3.12").build();
        Session session = cluster.connect();


        Session kpSession = cluster.connect("mykeyspace");


        RegularStatement insert = QueryBuilder.insertInto("mykeyspace", "cdr").values(new String[]{"a", "b", "c"}, new Object[]{1, 2, 3});
        kpSession.execute(insert);


        kpSession.close();
        session.close();
        cluster.close();
    }
}
