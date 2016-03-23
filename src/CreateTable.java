
import java.util.Iterator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CreateTable {
    public static void main(String[] args) {

        Cluster cluster = Cluster.builder().addContactPoint("10.2.3.12").build();
        Session session = cluster.connect();

        session.execute("CREATE KEYSPACE mykeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE mykeyspace;");

        session.execute("CREATE TABLE simplex.songs (id uuid PRIMARY KEY,title text,album text,"
                + "artist text,tags set<text>,data blob);");
        session.execute("CREATE TABLE simplex.playlists (id uuid,title text,album text, "
                + "artist text,song_id uuid,PRIMARY KEY (id, title, album, artist));");

        session.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,'La Petite Tonkinoise','Bye Bye Blackbird',"
                + "'Joséphine Baker',{'jazz', '2013'});");
        session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES ("
                + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'La Petite Tonkinoise','Bye Bye Blackbird','Joséphine Baker');");

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
        session.execute("DROP KEYSPACE simplex;");
        cluster.close();
    }


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

        // 针对keyspace的session，后面表名前面不用加keyspace
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
        kpSession.close();
        session.close();
        cluster.close();
    }

    public String createTableStatement(){
        
    }

}
