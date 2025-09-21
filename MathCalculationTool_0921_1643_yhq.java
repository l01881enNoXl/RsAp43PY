// 代码生成时间: 2025-09-21 16:43:31
 * It provides basic arithmetic operations and is designed to be easily extendable and maintainable.
 */

import static spark.Spark.*;
import spark.template.freemarker.FreeMarkerEngine;
import spark.ModelAndView;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

public class MathCalculationTool {
    private static final Gson gson = new Gson();

    /*
     * Main method to start the Spark web server.
     */
    public static void main(String[] args) {
        port(4567); // Set the port number for the Spark server
        staticFiles.location("/public"); // Serve static files from the public directory

        // Set up FreeMarker template engine
        get("/", (req, res) -> new ModelAndView(new HashMap<>(), "index.ftl"), new FreeMarkerEngine());

        // Define routes for basic arithmetic operations
        get("/add", "application/json", (req, res) -> {
            try {
                double a = Double.parseDouble(req.queryParams("a"));
                double b = Double.parseDouble(req.queryParams("b"));
                return new Gson().toJson(new OperationResult("add", a + b));
            } catch (NumberFormatException e) {
                return new Gson().toJson(new OperationResult("error", "Invalid input"));
            }
        });

        get("/subtract", "application/json", (req, res) -> {
            try {
                double a = Double.parseDouble(req.queryParams("a"));
                double b = Double.parseDouble(req.queryParams("b"));
                return new Gson().toJson(new OperationResult("subtract", a - b));
            } catch (NumberFormatException e) {
                return new Gson().toJson(new OperationResult("error", "Invalid input"));
            }
        });

        get("/multiply", "application/json", (req, res) -> {
            try {
                double a = Double.parseDouble(req.queryParams("a"));
                double b = Double.parseDouble(req.queryParams("b"));
                return new Gson().toJson(new OperationResult("multiply", a * b));
            } catch (NumberFormatException e) {
                return new Gson().toJson(new OperationResult("error", "Invalid input"));
            }
        });

        get("/divide", "application/json", (req, res) -> {
            try {
                double a = Double.parseDouble(req.queryParams("a"));
                double b = Double.parseDouble(req.queryParams("b"));
                if (b == 0) {
                    return new Gson().toJson(new OperationResult("error", "Cannot divide by zero"));
                }
                return new Gson().toJson(new OperationResult("divide", a / b));
            } catch (NumberFormatException e) {
                return new Gson().toJson(new OperationResult("error", "Invalid input"));
            }
        });
    }

    /*
     * Helper class to represent the result of an operation.
     */
    private static class OperationResult {
        private String operation;
        private double result;
        private String error;

        public OperationResult(String operation, double result) {
            this.operation = operation;
            this.result = result;
            this.error = null;
        }

        public OperationResult(String error) {
            this.operation = null;
            this.result = Double.NaN;
            this.error = error;
        }
    }
}