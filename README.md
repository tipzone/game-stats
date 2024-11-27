# game-stats
Turn board game statistics into meaningful insights.

Logs are generated locally in an app.log file.



# Docker commands
BULD IMAGE:  docker build -t google-drive-extractor -f google-drive.Dockerfile .
-t tags the image
-f specifies the name of the Dockerfile (since the name is not just Dockerfile)


RUN CONTAINER: docker run --rm --env-file ../../.env -v .:/app google-drive-extractor
In terminal you need to be navigated to the folder where the Docker files reside.
--rm auto removes the container to keep it clean
--env specifies where .env file is. The .env lives in the root folder so it can be reused. If it were in the google-drive folder this wouldn't be necessary. 
-v mounts the volume, so that Docker can read your local files. Docker cannot look at your local files without this. Technically you COULD use the COPY command in the Dockerfile to embed embed the Python in the image itself. But the tradeoff is people would have to rebuild the image whenever a pull request updates the .py file.

