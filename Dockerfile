FROM node:20

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY index.js ./

ENV PORT=10000
EXPOSE 10000

CMD ["node", "index.js"]
