# Use the official image as a parent image.
FROM python:alpine3.7

# Copy all files into the working directory.
COPY . /app

# Set the working directory.
WORKDIR /app

# Run the command inside your image filesystem to install Flask.
RUN pip install flask
RUN pip install requests

# Inform Docker that the container is listening on the specified port at runtime.
EXPOSE 8085

# Run the specified executable
ENTRYPOINT [ "python" ]
CMD [ "server.py" ]
