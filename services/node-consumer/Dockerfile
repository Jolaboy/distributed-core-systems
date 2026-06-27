# syntax=docker/dockerfile:1

FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev

FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production \
    PORT=3000 \
    HOST=0.0.0.0
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY proto ./proto
COPY src ./src
EXPOSE 3000
USER node
CMD ["node", "src/index.js"]
