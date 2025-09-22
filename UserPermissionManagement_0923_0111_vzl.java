// 代码生成时间: 2025-09-23 01:11:49
import static spark.Spark.*;

public class UserPermissionManagement {

    // A simple in-memory store to hold user permissions
    private static final HashMap<String, HashSet<String>> userPermissions = new HashMap<>();

    /**
     * Initializes the Spark routes for the user permission management system.
     */
    public static void init() {
        // Start the Spark server
        port(4567); // you can choose any available port

        // Route to add a new user with initial permissions
        post("/users/:username/permissions", (req, res) -> {
            String username = req.queryParams("username");
            String permissions = req.queryParams("permissions");
            try {
                if (userPermissions.containsKey(username)) {
                    return "User already exists";
                }
                userPermissions.put(username, new HashSet<>(Arrays.asList(permissions.split(", 