// 代码生成时间: 2025-10-26 05:11:24
import spark.Spark;
import spark.template.freemarker.FreeMarkerEngine;
import spark.ModelAndView;
import static spark.Spark.get;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class WealthManagementTool {
    // Main method to start the application
    public static void main(String[] args) {
        // Set the template engine to FreeMarker
        Spark.templateEngine(new FreeMarkerEngine());

        // Define routes
        get("/", (request, response) -> new ModelAndView(new HashMap<String, Object>(), "index.ftl"));
        get("/calculate", (request, response) -> calculateWealth(request));
    }

    // Method to calculate wealth
    private static ModelAndView calculateWealth(spark.Request request) {
        try {
            // Extract input parameters from the request
            String initialInvestment = request.queryParams("initialInvestment");
            String annualReturnRate = request.queryParams("annualReturnRate");
            String numberOfYears = request.queryParams("numberOfYears");

            if (initialInvestment == null || annualReturnRate == null || numberOfYears == null) {
                // Handle missing input parameters
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("error", "Please provide all input parameters.");
                return new ModelAndView(attributes, "error.ftl");
            }

            // Convert input parameters to double
            double initialInvestmentAmount = Double.parseDouble(initialInvestment);
            double annualReturnRateDouble = Double.parseDouble(annualReturnRate);
            int numberOfYearsInt = Integer.parseInt(numberOfYears);

            // Calculate final wealth using the compound interest formula
            double finalWealth = calculateFinalWealth(initialInvestmentAmount, annualReturnRateDouble, numberOfYearsInt);

            // Create a model with the calculated final wealth
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("finalWealth", finalWealth);
            return new ModelAndView(attributes, "result.ftl");
        } catch (Exception e) {
            // Handle any exceptions that occur during the calculation
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("error", "An error occurred during the calculation: " + e.getMessage());
            return new ModelAndView(attributes, "error.ftl");
        }
    }

    // Helper method to calculate final wealth using the compound interest formula
    private static double calculateFinalWealth(double initialInvestment, double annualReturnRate, int numberOfYears) {
        // Compound interest formula: A = P(1 + r/n)^(nt)
        // In this case, we assume n = 1 (annually)
        double finalWealth = initialInvestment * Math.pow(1 + annualReturnRate, numberOfYears);
        return finalWealth;
    }
}