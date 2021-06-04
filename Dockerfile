#FROM --platform=${TARGETPLATFORM:-linux/amd64} node:12.13.0-alpine as ship
FROM --platform=${TARGETPLATFORM:-linux/amd64} node:12-alpine as ship

ENV NPM_CONFIG_LOGLEVEL warn
ENV APIPORT=4444

WORKDIR /home/app

COPY package.json ./
# COPY apk/kafkacat-1.6.0-r0.apk ./
COPY . .

RUN \
    apk --no-cache add ca-certificates busybox-extras kafkacat && \
    addgroup -S app && adduser -S -g app app && \
    mkdir -p /home/app && \
    npm i && \
    chown app:app -R /home/app && \
    chmod 777 /tmp

USER app

CMD ["/usr/local/bin/node", "index.js"]

# apk add --allow-untrusted kafkacat-1.6.0-r0.apk &&\
