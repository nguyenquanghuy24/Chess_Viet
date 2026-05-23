FROM nginx:alpine

# Copy nginx configuration
COPY nginx/conf/default.conf /etc/nginx/conf.d/default.conf

# Copy static files
COPY nginx/html /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
