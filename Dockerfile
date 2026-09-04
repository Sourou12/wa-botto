FROM node:20

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY index.js ./

ENV PORT=10000
EXPOSE 10000

CMD ["node", "index.js"]
