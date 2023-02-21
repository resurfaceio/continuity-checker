// © 2016-2023 Resurface Labs Inc.

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {

    public static void main(String[] args) throws Exception {

        String url = "jdbc:trino://localhost:7700?user=continuity";  // todo add command-line params
        Connection c = DriverManager.getConnection(url);
        Statement s = c.createStatement();

        long total = 0;

        while (true) {

            System.out.println("---------------------------------------------------------------------------------------------------------");

            long start = System.currentTimeMillis();
            try (ResultSet rs = s.executeQuery("select shard_file, length_bytes, open_for_writes, pushed_to_iceberg from resurface.data.shards order by last_modified")) {
                while (rs.next()) {
                    String shard_file = rs.getString(1);
                    long length_bytes = rs.getLong(2);
                    boolean open_for_writes = rs.getBoolean(3);
                    boolean pushed_to_iceberg = rs.getBoolean(4);
                    String status = open_for_writes ? "🎤" : (pushed_to_iceberg ? "🧊" : "👉");
                    System.out.println(status + " " + shard_file + (open_for_writes ? (" (" + length_bytes + ")") : ""));
                }
            }
            System.out.println("   shards loaded in " + (System.currentTimeMillis() - start) + " ms");

            start = System.currentTimeMillis();
            try (ResultSet rs = s.executeQuery("select count(*) as total, current_time as total from resurface.data.message_union_index")){
                while (rs.next()) {
                    long current_total = rs.getLong(1);
                    String current_time = rs.getString(2);
                    if (current_total == total) {
                        // nothing to do
                    } else if (current_total > total) {
                        // ok, moving monotonically
                        total = current_total;
                        System.out.println("🐋 " + total);
                    } else {
                        // total went backwards
                        System.out.println("DISCONTINUITY DETECTED AT " + current_time + ", " + total + " 👉 " + current_total);
                        System.exit(-1);
                    }
                }
            }
            System.out.println("   count loaded in " + (System.currentTimeMillis() - start) + " ms");

            Thread.sleep(1000); // todo add command-line params
        }
    }

}
