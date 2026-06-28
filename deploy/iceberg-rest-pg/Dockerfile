# Iceberg REST catalog (apache fixture) + Postgres JDBC driver.
# The stock fixture launches `java -jar iceberg-rest-adapter.jar` which ignores
# -cp, so we relaunch via the Main-Class with an explicit classpath that adds
# the Postgres driver. This gives the JdbcCatalog a real, concurrency-safe
# backend (sqlite is single-writer and corrupts under multi-table commits).
FROM apache/iceberg-rest-fixture:1.10.1
USER root
COPY postgresql.jar /usr/lib/iceberg-rest/postgresql.jar
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
