#FROM trinodb/trino:476
# 删除目录下所有 jar 文件
#RUN rm -f /usr/lib/trino/plugin/mongodb/*.jar
# 复制你本地 lib 目录下的所有 jar 文件
#COPY lib/*.jar /usr/lib/trino/plugin/mongodb/
FROM redhat/ubi10-minimal:latest as tools
FROM trinodb/trino:476-4.0.24
USER root
COPY --from=tools /usr/bin/xargs /usr/bin/
COPY --from=tools /usr/bin/sed /usr/bin/
COPY --from=tools /usr/bin/jq /usr/bin/
COPY --from=tools /usr/bin/curl /usr/bin/
COPY --from=tools /usr/bin/find /usr/bin/
COPY --from=tools /lib64/ /lib64/
COPY --from=tools /usr/lib64/ /usr/lib64/
RUN mkdir -p /app/catalog
RUN chown -R 777 /app/catalog
RUN chown -R trino:trino /app/catalog
COPY --chown=trino:trino mongodb.properties /app/catalog/mongodb.properties
COPY --chown=trino:trino variable_replace.sh /usr/lib/trino/
COPY   --chown=trino:trino postgresql.properties /app/catalog/postgresql.properties
COPY  --chown=trino:trino  start.sh /usr/lib/trino/start.sh
RUN sed -i 's/\r$//' /usr/lib/trino/start.sh && \
    sed -i 's/\r$//' /usr/lib/trino/variable_replace.sh && \
    chmod +x /usr/lib/trino/start.sh && \
    chmod +x /usr/lib/trino/variable_replace.sh
EXPOSE 8080
USER trino:trino
CMD ["/usr/lib/trino/start.sh"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /usr/lib/trino/bin/health-check
