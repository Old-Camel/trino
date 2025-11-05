FROM trinodb/trino:476
# 删除目录下所有 jar 文件
RUN rm -f /usr/lib/trino/plugin/mongodb/*.jar
# 复制你本地 lib 目录下的所有 jar 文件
COPY lib/*.jar /usr/lib/trino/plugin/mongodb/
