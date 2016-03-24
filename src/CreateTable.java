
import com.datastax.driver.core.*;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;

public class CreateTable {
    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoint("10.2.3.12").build();
        Session session = cluster.connect();


        session.execute("CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        Session kpSession = cluster.connect("mykeyspace");


        try {
            InputStream r = new FileInputStream("bigdata_setup1.sql");
            ByteArrayOutputStream byteout = new ByteArrayOutputStream();
            byte tmp[] = new byte[256];
            byte context[];
            int i = 0;
            while ((i = r.read(tmp)) != -1) {
                byteout.write(tmp);
            }
            context = byteout.toByteArray();
            String str = new String(context, "UTF-8");

            kpSession.execute(str);
        } catch (Exception e) {
            System.out.println("Error: Creating table fails");
        }

        kpSession.close();
        session.close();
        cluster.close();
    }




    /*
    public static void main(String[] args) {
        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.QUORUM);

        Cluster cluster = Cluster.builder()
                .addContactPoint("10.2.3.12")   //seed node
                .withCredentials("cassandra", "cassandra")
                .withQueryOptions(options)
                .build();

        Session session = cluster.connect();
        session.execute("CREATE KEYSPACE kp WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

        Session kpSession = cluster.connect("kp");




        kpSession.execute("CREATE TABLE tbl(a INT,  b INT, c INT, PRIMARY KEY(a));");

        RegularStatement insert = QueryBuilder.insertInto("kp", "tbl").values(new String[]{"a", "b", "c"}, new Object[]{1, 2, 3});
        kpSession.execute(insert);

        RegularStatement insert2 = QueryBuilder.insertInto("kp", "tbl").values(new String[]{"a", "b", "c"}, new Object[]{3, 2, 1});
        kpSession.execute(insert2);

        RegularStatement delete = QueryBuilder.delete().from("kp", "tbl").where(QueryBuilder.eq("a", 1));
        kpSession.execute(delete);

        RegularStatement update = QueryBuilder.update("kp", "tbl").with(QueryBuilder.set("b", 6)).where(QueryBuilder.eq("a", 3));
        kpSession.execute(update);

        RegularStatement select = QueryBuilder.select().from("kp", "tbl").where(QueryBuilder.eq("a", 3));
        ResultSet rs = kpSession.execute(select);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println("a=" + row.getInt("a"));
            System.out.println("b=" + row.getInt("b"));
            System.out.println("c=" + row.getInt("c"));
        }


        //~~~~~~~~~~
        ResultSet results = session.execute("SELECT * FROM simplex.playlists "
                + "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        // + "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"),
                    row.getString("artist")));
        }
        System.out.println();
        //~~~~~~~~~~~~

        kpSession.close();
        session.close();
        cluster.close();
    }


    */

}
