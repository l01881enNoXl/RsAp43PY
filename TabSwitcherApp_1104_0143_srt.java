// 代码生成时间: 2025-11-04 01:43:54
import static spark.Spark.*;

public class TabSwitcherApp {

    // Define the number of tabs
    private static final int NUM_TABS = 3;

    // Define the port number
    private static final int PORT = 4567;

    /**
     * The main method where the application starts.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        // Initialize Spark
        port(PORT);

        // Home page that displays tabs
        get("/", (req, res) -> {
            return "<html><body>
" +
                    "<h2>Tab Switcher</h2>
" +
                    "<ul>
";
            for (int i = 1; i <= NUM_TABS; i++) {
                String activeClass = (req.queryParams("tab") != null && req.queryParams("tab").equals(String.valueOf(i))) ? "active" : "";
                String tabLink = String.format("<li><a href='/?tab=%d' class='%s'>Tab %d</a></li>", i, activeClass, i);
                return res.body().append(tabLink);
            }
            return res.body().append("</ul>
");
        }, "text/html");

        // Define each tab's content
        for (int i = 1; i <= NUM_TABS; i++) {
            get(String.format("/tab%d", i), (req, res) -> {
                String tabContent = "Content of tab " + i;
                return "<html><body>
" + tabContent + "</body></html>
";
            }, "text/html");
        }
    }
}
