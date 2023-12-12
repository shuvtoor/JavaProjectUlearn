import java.sql.Connection;
import java.sql.SQLException;
import java.io.PrintWriter;
import java.io.FileNotFoundException;


public class Parser {
    public static void run(Connection connection) throws SQLException, FileNotFoundException {
        var statement = connection.createStatement();
        var printWriter = new PrintWriter("Data.txt");
        var resultSet = statement.executeQuery(
                "SELECT printf(\"%.2f\", sum(networth)) as net, country "
                        + "FROM forbes "
                        + "GROUP BY country;"
        );

        while (resultSet.next()) {
            printWriter.println(
                    resultSet.getString("country") + ";="
                            + resultSet.getString("net").replace('.', ',')
            );
        }

        printWriter.close();

        System.out.println("Cамый молодой миллиардер из Франции, капитал которого превышает 10 млрд.:");
        System.out.println(statement.executeQuery(
                "SELECT *, min(age) "
                        + "FROM forbes "
                        + "WHERE country ='France' AND networth > 10;").getString("name")
        );

        resultSet = statement.executeQuery(
                "SELECT name,source, max(networth) "
                        + "FROM forbes "
                        + "WHERE country ='United States' and industry='Energy ';"
        );
        System.out.println("\n");
        System.out.println("Имя и компания бизнесмена из США, имеющего самый большой капитал в сфере Energy:");
        System.out.println(resultSet.getString("name") + " "
                + resultSet.getString("source")
        );
        statement.close();
    }
}
