FROM eclipse-temurin:21-jdk-alpine
WORKDIR /app
COPY src/ /app/src/
RUN mkdir -p /app/out
CMD ["sh", "-c", "find /app/src -name '*.java' > sources.txt && javac -d /app/out @sources.txt && java -Xmx400m -cp /app/out lsm.benchmark.BenchmarkRunner"]