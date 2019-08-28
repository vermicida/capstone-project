FROM cassandra:3.11.4

COPY entrypoint-wrap.sh /entrypoint-wrap.sh

ENTRYPOINT ["/entrypoint-wrap.sh"]

CMD ["cassandra", "-f"]
