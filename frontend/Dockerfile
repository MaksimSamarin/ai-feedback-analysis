ARG NODE_BASE_IMAGE=node:20-alpine
ARG NGINX_BASE_IMAGE=nginx:1.27-alpine
FROM ${NODE_BASE_IMAGE} AS build

WORKDIR /app

COPY package*.json /app/
COPY certs/ /tmp/certs/
ARG NPM_REGISTRY_URL=https://registry.npmjs.org/
ARG NPM_STRICT_SSL=true
ARG APK_REPOSITORY_MAIN_URL=
ARG APK_REPOSITORY_COMMUNITY_URL=
ARG APK_TLS_NO_VERIFY=false
RUN if [ -n "${APK_REPOSITORY_MAIN_URL}" ] && [ -n "${APK_REPOSITORY_COMMUNITY_URL}" ]; then \
         printf '%s\n%s\n' "${APK_REPOSITORY_MAIN_URL}" "${APK_REPOSITORY_COMMUNITY_URL}" > /etc/apk/repositories; \
       fi \
    && find /tmp/certs -type f \( -name '*.crt' -o -name '*.pem' \) -exec sh -c 'cat "$1" >> /etc/ssl/certs/ca-certificates.crt' _ {} \; \
    && if [ "${APK_TLS_NO_VERIFY}" = "true" ]; then \
         apk add --no-cache --no-check-certificate ca-certificates; \
       else \
         apk add --no-cache ca-certificates; \
       fi \
    && find /tmp/certs -type f \( -name '*.crt' -o -name '*.pem' \) -exec cp {} /usr/local/share/ca-certificates/ \; \
    && update-ca-certificates \
    && npm config set strict-ssl "${NPM_STRICT_SSL}" \
    && npm config set cafile /etc/ssl/certs/ca-certificates.crt \
    && npm ci --registry "${NPM_REGISTRY_URL}"

COPY . /app

ARG VITE_API_BASE=/api
ENV VITE_API_BASE=${VITE_API_BASE}

RUN npm run build

FROM ${NGINX_BASE_IMAGE}

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
