# 使用包含 Java 17 的基础镜像
FROM openjdk:17-jdk-slim

# 将 Maven 构建出的 JAR 包复制到容器中
# 注意：路径取决于您的 Maven build
ARG JAR_FILE=target/tdm-core-service-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar

# 暴露端口
EXPOSE 8080

# 启动应用
ENTRYPOINT ["java", "-jar", "/app.jar"]