package analyticshistory;

import java.util.Scanner;

public class Main{
    public static void main(String[] args){
        Handler handler = new Handler();

        Scanner in = new Scanner(System.in);

        System.out.print("Host Name: ");
        String hostName = in.nextLine();
        System.out.print("Database Name: ");
        String dbName = in.nextLine();
        System.out.print("User: ");
        String user = in.nextLine();
        System.out.print("Password: ");
        String password = in.nextLine();

        handler.setUrl(hostName, dbName, user, password);
        while(!handler.connect()){
            System.out.println("Connection failed");
            System.out.print("Host Name: ");
            hostName = in.nextLine();
            System.out.print("Database Name: ");
            dbName = in.nextLine();
            System.out.print("User: ");
            user = in.nextLine();
            System.out.print("Password: ");
            password = in.nextLine();

            handler.setUrl(hostName, dbName, user, password);
        }

        new Thread(handler::start).start();

        in.nextLine();

        in.close();

        handler.deactivate();
    }
}
