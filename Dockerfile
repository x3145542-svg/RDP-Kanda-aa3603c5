FROM node:20-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY bridge.js .
ENV PORT=3000
CMD ["node", "bridge.js"]
