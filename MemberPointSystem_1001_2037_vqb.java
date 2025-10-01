// 代码生成时间: 2025-10-01 20:37:50
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

// 会员积分系统
public class MemberPointSystem {

    private SparkSession spark;

    // 构造方法，初始化SparkSession
    public MemberPointSystem(SparkSession spark) {
        this.spark = spark;
    }

    // 添加积分
    public Dataset<Row> addPoints(String memberId, int points) {
        try {
            // 检查积分是否有效
            if (points < 0) {
                throw new IllegalArgumentException("积分不能为负数");
            }

            // 获取当前会员积分
            Dataset<Row> currentPoints = getMemberPoints(memberId);
            int currentPointsValue = currentPoints.agg(functions.sum("points")).first().getInt(0);

            // 计算新的积分
            int newPoints = currentPointsValue + points;

            // 更新会员积分
            currentPoints = currentPoints.withColumn("points", functions.lit(newPoints));

            // 保存更新后的积分数据
            currentPoints.write().format("parquet").save("member_points.parquet");

            return currentPoints;
        } catch (Exception e) {
            // 错误处理
            System.err.println("添加积分失败：" + e.getMessage());
            return null;
        }
    }

    // 获取会员积分
    public Dataset<Row> getMemberPoints(String memberId) {
        try {
            // 读取会员积分数据
            Dataset<Row> memberPoints = spark.read().format("parquet").load("member_points.parquet");

            // 过滤出指定会员的积分数据
            return memberPoints.filter(memberPoints.col("member_id").equalTo(memberId));
        } catch (Exception e) {
            // 错误处理
            System.err.println("获取积分失败：" + e.getMessage());
            return null;
        }
    }

    // 主方法，用于测试
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("MemberPointSystem")
                .master("local")
                .getOrCreate();

        MemberPointSystem memberPointSystem = new MemberPointSystem(spark);

        // 测试添加积分
        Dataset<Row> updatedPoints = memberPointSystem.addPoints("member1", 100);
        if (updatedPoints != null) {
            updatedPoints.show();
        }

        // 测试获取积分
        Dataset<Row> memberPoints = memberPointSystem.getMemberPoints("member1");
        if (memberPoints != null) {
            memberPoints.show();
        }

        spark.stop();
    }
}
