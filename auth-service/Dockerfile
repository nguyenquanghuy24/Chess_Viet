FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

RUN npm install

COPY src/ ./src/

RUN npm run build

# Expose port
EXPOSE 3001

# Start the application
CMD ["npm", "start"]
