// 代码生成时间: 2025-10-05 01:52:24
import spark.Spark;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class AntiCheatSystem {
    // In-memory storage for suspicious activities
    private static final Map<String, List<String>> suspiciousActivities = new HashMap<>();

    // Initialize the anti-cheat system
    public static void main(String[] args) {
        // Define the routes
        initRoutes();
    }

    private static void initRoutes() {
        // Route to report suspicious activity
        Spark.post("/report", AntiCheatSystem.class, (req, res) -> {
            try {
                String activity = req.queryParams("activity");
                String userId = req.queryParams("userId");

                // Check if the activity report is valid
                if (activity == null || userId == null) {
                    throw new IllegalArgumentException("Missing required parameters");
                }

                // Add the suspicious activity to the in-memory storage
                suspiciousActivities.computeIfAbsent(userId, k -> new ArrayList<>()).add(activity);

                return "Suspicious activity reported";
            } catch (Exception e) {
                // Handle any exceptions and return an error message
                return "Error: " + e.getMessage();
            }
        });

        // Route to check and review suspicious activities
        Spark.get("/check", AntiCheatSystem.class, (req, res) -> {
            try {
                String userId = req.queryParams("userId");
                if (userId == null) {
                    throw new IllegalArgumentException("Missing userId parameter");
                }

                // Retrieve the suspicious activities for the given user
                List<String> activities = suspiciousActivities.getOrDefault(userId, new ArrayList<>());

                // Return the suspicious activities as a JSON string
                return activities.stream()
                        .map(activity -> "{"activity":"" + activity + """}
                        )
                        .collect(Collectors.joining(", "));
            } catch (Exception e) {
                // Handle any exceptions and return an error message
                return "Error: " + e.getMessage();
            }
        });
    }

    // Method to flag a user as a cheater based on their suspicious activities
    private static boolean isCheater(String userId) {
        // For simplicity, if a user has more than 3 suspicious activities, flag them as a cheater
        List<String> activities = suspiciousActivities.getOrDefault(userId, new ArrayList<>());
        return activities.size() > 3;
    }
}
