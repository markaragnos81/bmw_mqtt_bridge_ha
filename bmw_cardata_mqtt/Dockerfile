ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

LABEL \
  io.hass.name="BMW CarData MQTT Bridge" \
  io.hass.description="BMW CarData -> HA MQTT Discovery mit OAuth2 Setup-Wizard, Streaming-Status und Ingress-Streaming-Fix" \
  io.hass.type="addon" \
  io.hass.version="3.2.21"

RUN apk add --no-cache \
    ca-certificates \
    openssl \
    py3-pip \
    python3

RUN python3 -m pip install --no-cache-dir --break-system-packages \
    flask==3.0.3 \
    paho-mqtt==2.1.0 \
    requests==2.31.0

COPY rootfs/usr/bin/ /usr/bin/
RUN chmod +x /usr/bin/webapp.py /usr/bin/bmw_client.py

RUN mkdir -p /data

EXPOSE 8099
CMD ["python3", "/usr/bin/webapp.py"]
