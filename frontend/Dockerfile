# Use an official lightweight Nginx image as a parent image
FROM nginx:stable-alpine

# Copy the static content (HTML, CSS, JS, etc.) into the container
COPY . /usr/share/nginx/html

# Expose port 80
EXPOSE 80

# Start Nginx and keep it running
CMD ["nginx", "-g", "daemon off;"]
