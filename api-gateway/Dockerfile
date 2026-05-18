FROM nginx:alpine

# Copy nginx configuration
COPY nginx/conf/default.conf /etc/nginx/conf.d/default.conf

# Copy SSL certificates (if available)
COPY nginx/ssl /etc/nginx/ssl

# Copy static files
COPY nginx/html /usr/share/nginx/html

EXPOSE 80 443

CMD ["nginx", "-g", "daemon off;"]