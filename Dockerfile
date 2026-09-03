FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY index.js ./

ENV PORT=10000
EXPOSE 10000

CMD ["node", "index.js"]
