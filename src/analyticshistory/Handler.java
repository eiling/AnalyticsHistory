package analyticshistory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

class Handler{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss.S");

    private boolean active;

    private String url;
    private Connection connection;

    Handler(){
        active = true;
    }
    void setUrl(String hostName, String dbName, String user, String password){
        url = String.format("jdbc:sqlserver://%s:1433;database=%s;user=%s;password=%s;" +
                        "encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=5;",
                hostName, dbName, user, password);
    }

    boolean connect(){
        try {
            connection = DriverManager.getConnection(url);
            connection.setNetworkTimeout(null, 5000);
            return true;
        } catch (SQLException e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    void start(){
        while(active){
            try{
                if(connection.isClosed()){
                    System.out.println("Reconnecting.");
                    connect();
                }
            } catch(SQLException e){
                System.out.println("Connection.isClosed() error.");
                System.out.println(e.getMessage());
            }

            //for each 5 minutes
            try{
                Statement st = connection.createStatement();
                st.setQueryTimeout(1);

                ResultSet resultSet =
                        st.executeQuery("SELECT id,min(datetime),max(datetime) FROM reading GROUP BY id");

                while(resultSet.next()){
                    String id = resultSet.getString(1);
                    LocalDateTime start = LocalDateTime.parse(resultSet.getString(2), FORMATTER);
                    LocalDateTime end = LocalDateTime.parse(resultSet.getString(3), FORMATTER);

                    start = start.truncatedTo(ChronoUnit.MINUTES).minusMinutes(start.getMinute() % 5);

                    end = end.truncatedTo(ChronoUnit.MINUTES); //39
                    end = end.minusMinutes(end.getMinute() % 5); //35
                    end = end.minusSeconds(5); //34:55

                    Statement statement = connection.createStatement();
                    statement.setQueryTimeout(5);

                    while(start.isBefore(end)){
                        String startTime = start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        start = start.plusMinutes(5);

                        String endTime = start.minusSeconds(5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        ResultSet count = statement.executeQuery(
                                "SELECT count(reference) FROM reading WHERE datetime " +
                                        "BETWEEN '" + startTime + "' AND '" + endTime + "'"
                        );

                        count.next(); //count() always returns a single row
                        if(count.getInt(1) > 54){ //90% of the total
                            statement.executeUpdate(
                                    "INSERT INTO five_minutes_temperature_analytics\n" +
                                            "SELECT DISTINCT\n" +
                                            "    min(datetime) OVER (PARTITION BY id) AS datetime,\n" +
                                            "    id,\n" +
                                            "    round(avg(temperature) OVER (PARTITION BY id), 1) AS average,\n" +
                                            "    round(var(temperature) OVER (PARTITION BY id), 1) AS variance,\n" +
                                            "    round(stdev(temperature) OVER (PARTITION BY id), 1) AS stdev,\n" +
                                            "    min(temperature) OVER (PARTITION BY id) AS min,\n" +
                                            "    round(percentile_cont(0.25) WITHIN GROUP (ORDER BY temperature) " +
                                            "       OVER (PARTITION BY id), 1) AS twentyfive,\n" +
                                            "    round(percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature) " +
                                            "       OVER (PARTITION BY id), 1) AS fifty,\n" +
                                            "    round(percentile_cont(0.75) WITHIN GROUP (ORDER BY temperature) " +
                                            "       OVER (PARTITION BY id), 1) AS seventyfive,\n" +
                                            "    max(temperature) OVER (PARTITION BY id) AS max\n" +
                                            "FROM reading\n" +
                                            "WHERE " +
                                            "   id = '" + id + "' AND " +
                                            "   datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                            );

                            System.out.println(
                                    "Inserted analytics from " + id + " for [ " + startTime + "; " + endTime + " ]"
                            );
                        } else{
                            System.out.println(
                                    "Not enough data from " + id + " for [ " + startTime + "; " + endTime + " ]"
                            );
                        }

                        int x = statement.executeUpdate(
                                "DELETE FROM reading WHERE " +
                                        "id = '" + id + "' AND " +
                                        "datetime<='" + endTime + "'"
                        );

                        System.out.println(
                                "Deleted " + x + " readings from " + id + " for [ " + startTime + "; " + endTime + " ]"
                        );

                        if(!active) break;
                    }

                    if(!active) break;

                    //for each hour
                }

            } catch(SQLException e){
                e.printStackTrace();
            }

            System.out.println("waiting");

            try{
                Thread.sleep(1000);
            } catch(InterruptedException ignored){
            }
        }

        if(connection == null) return;

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void deactivate(){
        active = false;
    }
}
