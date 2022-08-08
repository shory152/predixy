#
# docker build -t registry.woqutech.com/predixy:$(git describe --long) .
#

# build predixy
FROM redis:7.0 as builder
RUN sed -i "s|deb.debian.org|mirrors.aliyun.com|g" /etc/apt/sources.list
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y gcc g++ make git
COPY Makefile /predixy/Makefile
COPY src      /predixy/src
COPY .git     /predixy/.git
RUN cd /predixy && make clean && make

# build predixy docker image
FROM redis:7.0
COPY --from=builder /predixy/src/predixy /usr/bin/predixy
WORKDIR /
ENTRYPOINT ["predixy"]
