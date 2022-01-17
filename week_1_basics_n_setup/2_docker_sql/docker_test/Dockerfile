# base image
FROM python:3.9
# run commands on the base image (python shell opens by default in this image)
RUN pip install pandas
# create an app folder, cd into it and copy a local file into it
WORKDIR /app
COPY pipeline.py pipeline.py
# command to run when the image starts. 
ENTRYPOINT [ "python", "pipeline.py" ]

# build an image: docker build -t test_image:0.0 .
# run an image: docker run -it test_image:0.0 2022-02-17