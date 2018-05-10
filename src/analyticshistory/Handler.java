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


            try{
                Statement st = connection.createStatement();
                st.setQueryTimeout(1);

                //for each 5 minutes
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
                                "SELECT count(reference) FROM reading " +
                                        "WHERE id='" + id + "' AND " +
                                        "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                        );

                        count.next(); //count() always returns a single row
                        if(count.getInt(1) >= 54){ //~90% of the total
                            statement.executeUpdate(
                                    "INSERT INTO five_minutes_temperature_analytics " +
                                            "SELECT DISTINCT " +
                                                "min(datetime) OVER (PARTITION BY id)," +
                                                "id," +
                                                "round(avg(temperature) OVER (PARTITION BY id), 1)," +
                                                "round(var(temperature) OVER (PARTITION BY id), 1)," +
                                                "round(stdev(temperature) OVER (PARTITION BY id), 1)," +
                                                "min(temperature) OVER (PARTITION BY id)," +
                                                "round(percentile_cont(0.25) WITHIN GROUP (ORDER BY temperature) " +
                                                    "OVER (PARTITION BY id), 1)," +
                                                "round(percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature) " +
                                                    "OVER (PARTITION BY id), 1)," +
                                                "round(percentile_cont(0.75) WITHIN GROUP (ORDER BY temperature) " +
                                                    "OVER (PARTITION BY id), 1)," +
                                                "max(temperature) OVER (PARTITION BY id)" +
                                            "FROM reading " +
                                            "WHERE " +
                                                "id = '" + id + "' AND " +
                                                "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                            );

                            System.out.println(
                                    "Inserted analytics from " + id + " for [ " + startTime + "; " + endTime + " ] (5 min)"
                            );
                        } else{
                            System.out.println(
                                    "Not enough data from " + id + " for [ " + startTime + "; " + endTime + " ] (5 min)"
                            );
                        }

                        int x = statement.executeUpdate(
                                "DELETE FROM reading WHERE " +
                                        "id = '" + id + "' AND " +
                                        "datetime<='" + endTime + "'"
                        );

                        System.out.println(
                                "Deleted " + x + " readings from " + id + " for [ " + startTime + "; " + endTime + " ] (5 min)"
                        );

                        if(!active) break;
                    }

                    if(!active) break;
                }

                if(!active) break;

                //for each hour
                resultSet =
                        st.executeQuery(
                                "SELECT id,min(datetime),max(datetime) " +
                                        "FROM five_minutes_temperature_analytics " +
                                        "GROUP BY id"
                        );

                while(resultSet.next()){
                    String id = resultSet.getString(1);
                    LocalDateTime start = LocalDateTime.parse(resultSet.getString(2), FORMATTER);
                    LocalDateTime end = LocalDateTime.parse(resultSet.getString(3), FORMATTER);

                    start = start.truncatedTo(ChronoUnit.HOURS);

                    end = end.truncatedTo(ChronoUnit.HOURS).minusMinutes(5);

                    Statement statement = connection.createStatement();
                    statement.setQueryTimeout(5);

                    while(start.isBefore(end)){
                        String startTime = start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        start = start.plusHours(1);

                        String endTime = start.minusMinutes(5).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        ResultSet count = statement.executeQuery(
                                "SELECT count(reference) FROM five_minutes_temperature_analytics " +
                                        "WHERE id='" + id + "' AND " +
                                        "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                        );

                        count.next(); //count() always returns a single row
                        if(count.getInt(1) >= 11){ //~90% of the total
                            statement.executeUpdate(
                                    "INSERT INTO hour_temperature_analytics " +
                                            "SELECT DISTINCT " +
                                                "min(datetime) OVER (PARTITION BY id)," +
                                                "id," +
                                                "round(avg(average) OVER (PARTITION BY id), 1)," +
                                                "round(avg(variance) OVER (PARTITION BY id), 1)," +
                                                "round(avg(stdev) OVER (PARTITION BY id), 1)," +
                                                "min(minimum) OVER (PARTITION BY id)," +
                                                "round(avg(first_quartile) OVER (PARTITION BY id), 1)," +
                                                "round(avg(median) OVER (PARTITION BY id), 1)," +
                                                "round(avg(third_quartile) OVER (PARTITION BY id), 1)," +
                                                "max(maximum) OVER (PARTITION BY id)" +
                                            "FROM five_minutes_temperature_analytics " +
                                            "WHERE " +
                                                "id = '" + id + "' AND " +
                                                "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                            );

                            System.out.println(
                                    "Inserted analytics from " + id + " for [ " + startTime + "; " + endTime + " ] (hour)"
                            );
                        } else{
                            System.out.println(
                                    "Not enough data from " + id + " for [ " + startTime + "; " + endTime + " ] (hour)"
                            );
                        }

                        int x = statement.executeUpdate(
                                "DELETE FROM five_minutes_temperature_analytics WHERE " +
                                        "id = '" + id + "' AND " +
                                        "datetime<='" + endTime + "'"
                        );

                        System.out.println(
                                "Deleted " + x + " readings from " + id + " for [ " + startTime + "; " + endTime + " ] (hour)"
                        );

                        if(!active) break;
                    }

                    if(!active) break;
                }

                if(!active) break;

                //for each day
                resultSet =
                        st.executeQuery(
                                "SELECT id,min(datetime),max(datetime) " +
                                        "FROM hour_temperature_analytics " +
                                        "GROUP BY id"
                        );

                while(resultSet.next()){
                    String id = resultSet.getString(1);
                    LocalDateTime start = LocalDateTime.parse(resultSet.getString(2), FORMATTER);
                    LocalDateTime end = LocalDateTime.parse(resultSet.getString(3), FORMATTER);

                    start = start.truncatedTo(ChronoUnit.DAYS);

                    end = end.truncatedTo(ChronoUnit.DAYS).minusHours(1);

                    Statement statement = connection.createStatement();
                    statement.setQueryTimeout(5);

                    while(start.isBefore(end)){
                        String startTime = start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        start = start.plusDays(1);

                        String endTime = start.minusHours(1).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                        ResultSet count = statement.executeQuery(
                                "SELECT count(reference) FROM five_minutes_temperature_analytics " +
                                        "WHERE id='" + id + "' AND " +
                                        "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                        );

                        count.next(); //count() always returns a single row
                        if(count.getInt(1) >= 22){ //~90% of the total
                            statement.executeUpdate(
                                    "INSERT INTO day_temperature_analytics " +
                                            "SELECT DISTINCT " +
                                            "min(datetime) OVER (PARTITION BY id)," +
                                            "id," +
                                            "round(avg(average) OVER (PARTITION BY id), 1)," +
                                            "round(avg(variance) OVER (PARTITION BY id), 1)," +
                                            "round(avg(stdev) OVER (PARTITION BY id), 1)," +
                                            "min(minimum) OVER (PARTITION BY id)," +
                                            "round(avg(first_quartile) OVER (PARTITION BY id), 1)," +
                                            "round(avg(median) OVER (PARTITION BY id), 1)," +
                                            "round(avg(third_quartile) OVER (PARTITION BY id), 1)," +
                                            "max(maximum) OVER (PARTITION BY id)" +
                                            "FROM hour_temperature_analytics " +
                                            "WHERE " +
                                            "id = '" + id + "' AND " +
                                            "datetime BETWEEN '" + startTime + "' AND '" + endTime + "'"
                            );

                            System.out.println(
                                    "Inserted analytics from " + id + " for [ " + startTime + "; " + endTime + " ] (day)"
                            );
                        } else{
                            System.out.println(
                                    "Not enough data from " + id + " for [ " + startTime + "; " + endTime + " ] (day)"
                            );
                        }

                        int x = statement.executeUpdate(
                                "DELETE FROM hour_temperature_analytics WHERE " +
                                        "id = '" + id + "' AND " +
                                        "datetime<='" + endTime + "'"
                        );

                        System.out.println(
                                "Deleted " + x + " readings from " + id + " for [ " + startTime + "; " + endTime + " ] (day)"
                        );

                        if(!active) break;
                    }

                    if(!active) break;
                }

                if(!active) break;
            } catch(SQLException e){
                e.printStackTrace();
            }

            System.out.println("waiting");

            try{
                Thread.sleep(1000);
            } catch(InterruptedException ignored){
            }
        }

        //disconnect
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
