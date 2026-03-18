FROM dxflrs/garage:v2.2.0 AS garagebin

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=garagebin /garage /garage
COPY docker/garage/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh /garage

ENTRYPOINT ["/entrypoint.sh"]