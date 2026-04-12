ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.11
FROM $BUILD_FROM

LABEL \
  io.hass.name="BMW CarData MQTT Bridge" \
  io.hass.description="BMW CarData → HA MQTT Discovery mit OAuth2 Setup-Wizard und auto. Fahrzeugerkennung" \
  io.hass.type="addon" \
  io.hass.version="3.0.0"

RUN apk add --no-cache openssl ca-certificates

RUN pip3 install --no-cache-dir \
    flask==3.0.3 \
    paho-mqtt==2.1.0 \
    requests==2.31.0

COPY rootfs/usr/bin/ /usr/bin/
RUN chmod +x /usr/bin/webapp.py /usr/bin/bmw_client.py

RUN mkdir -p /data

EXPOSE 8099
CMD ["python3", "/usr/bin/webapp.py"]
