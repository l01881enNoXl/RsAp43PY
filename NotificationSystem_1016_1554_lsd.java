// 代码生成时间: 2025-10-16 15:54:53
import spark.ModelAndView;
import spark.template.mustache.MustacheTemplateEngine;
import java.util.HashMap;
import java.util.Map;
import static spark.Spark.*;

public class NotificationSystem {

    // Map to store user notifications
    private static final Map<String, String> userNotifications = new HashMap<>();

    public static void main(String[] args) {
        // Set the port to listen on
        port(4567);

        // Set the template engine to render Mustache templates
        templateEngine(new MustacheTemplateEngine());

        // Define routes
        get("/notifications", (request, response) -> {
            // Return a ModelAndView with the current user notifications
            ModelAndView modelAndView = new ModelAndView(new HashMap<>(), "notifications.mustache");
            modelAndView.model("notifications", userNotifications.get(request.queryParams("username")));
            return modelAndView;
        }, new MustacheTemplateEngine());

        post("/receiveNotification", (request, response) -> {
            try {
                // Extract the username and notification message from the request body
                String username = request.queryParams("username");
                String message = request.queryParams("message");

                // Add the notification to the user's list
                String currentNotifications = userNotifications.getOrDefault(username, "");
                userNotifications.put(username, currentNotifications + "
" + message);

                // Respond with a success message
                return "Notification received and added for user: " + username;
            } catch (Exception e) {
                // Handle any errors that occur
                return "An error occurred: " + e.getMessage();
            }
        });
    }

    // Method to get the current notifications for a user
    public static String getUserNotifications(String username) {
        return userNotifications.getOrDefault(username, "No notifications for this user.");
    }

    // Method to add a notification to a user
    public static void addNotification(String username, String message) {
        String currentNotifications = userNotifications.getOrDefault(username, "");
        userNotifications.put(username, currentNotifications + "
" + message);
    }
}
