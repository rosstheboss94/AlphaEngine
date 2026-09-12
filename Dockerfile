# syntax=docker/dockerfile:1

FROM node:22-alpine AS frontend-build

WORKDIR /workspace/app

COPY app/package.json app/package-lock.json ./
RUN npm ci

COPY app/ ./
RUN npm run build

FROM nginx:1.29-alpine

COPY --from=frontend-build /workspace/app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://127.0.0.1/ || exit 1
