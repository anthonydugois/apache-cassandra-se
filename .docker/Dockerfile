FROM eclipse-temurin:11-jre-focal

RUN groupadd -r cassandra --gid=999 \
 && useradd -r -g cassandra --uid=999 cassandra

RUN apt-get update \
 && apt-get install -y --no-install-recommends wget libjemalloc2 procps iproute2 numactl python3 gosu \
 && rm -rf /var/lib/apt/lists/* \
 && gosu nobody true \
 && libjemalloc=$(readlink -e /usr/lib/*/libjemalloc.so.2) \
 && ln -sT $libjemalloc /usr/local/lib/libjemalloc.so \
 && ldconfig

ENV CASSANDRA_HOME /opt/cassandra
ENV CASSANDRA_CONF /etc/cassandra
ENV CASSANDRA_DATA /var/lib/cassandra
ENV CASSANDRA_LOGS /var/log/cassandra

ENV PATH $CASSANDRA_HOME/bin:$PATH

RUN mkdir -p $CASSANDRA_HOME $CASSANDRA_CONF $CASSANDRA_DATA $CASSANDRA_LOGS

COPY ./build/dist $CASSANDRA_HOME

RUN chmod +x $CASSANDRA_HOME/bin/cassandra $CASSANDRA_HOME/bin/nodetool $CASSANDRA_HOME/bin/cqlsh \
 && chown -R cassandra:cassandra $CASSANDRA_CONF $CASSANDRA_DATA $CASSANDRA_LOGS \
 && chmod 777 $CASSANDRA_CONF $CASSANDRA_DATA $CASSANDRA_LOGS \
 && chmod -R a+rwX $CASSANDRA_CONF \
 && mv $CASSANDRA_HOME/conf/* $CASSANDRA_CONF \
 && rm -rf $CASSANDRA_HOME/conf \
 && ln -sT $CASSANDRA_CONF $CASSANDRA_HOME/conf \
 && ln -sT $CASSANDRA_DATA $CASSANDRA_HOME/data \
 && ln -sT $CASSANDRA_LOGS $CASSANDRA_HOME/logs

COPY ./.docker/entrypoint.sh /usr/local/bin/
ENTRYPOINT ["entrypoint.sh"]

EXPOSE 7000 7001 7199 9042 9160
CMD ["cassandra", "-f"]
