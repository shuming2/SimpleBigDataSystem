import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class InsertTable {
    public static void main(String[] args) {
        String str ="";
        Cluster cluster = Cluster.builder().addContactPoint("10.2.3.12").build();
        Session session = cluster.connect();


        Session kpSession = cluster.connect("mykeyspace");


        RegularStatement insert = QueryBuilder.insertInto("mykeyspace", "cdr").values(new String[]{"a", "b", "c"}, new Object[]{1, 2, 3});
        kpSession.execute(insert);

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
            str = new String(context, "UTF-8");
        } catch (Exception e) {
            System.out.println("Error: File does not exist");
        }


        Map<String, String> columns = getColumns(str);


        kpSession.close();
        session.close();
        cluster.close();
    }


    private static Map<String, String> getColumns (String str) {
        Map<String, String> map = new LinkedHashMap<>();


        str = str.substring(str.indexOf("(") + 1, str.lastIndexOf(")"));
        ArrayList<String> list = new ArrayList<>(Arrays.asList(str.split(",")));
        list.remove(list.size()-1);

        String[] items;
        int i;
        for (String item: list) {
            items = item.split(" ");
            for (i = 1; i < items.length; i++) {
                if (!items[i].equals(" ") && !items[i].equals("")) {
                    break;
                }
            }
            map.put(items[0],items[i]);
        }

        return map;
    }

    //insert
    private String populate(Map<String, String> columns) {
        for(String key: columns.keySet()) {
            if (columns.get(key).equals("int")) {

            }
        }



        return null;
    }
}
