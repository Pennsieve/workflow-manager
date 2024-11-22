FROM golang:bullseye

WORKDIR /service

# next flow dependencies
RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add -
RUN echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt-get update
RUN apt-get -y install temurin-18-jdk

# install nextflow
RUN wget -qO- https://get.nextflow.io | bash && chmod +x nextflow && cp ./nextflow /usr/local

ENV PATH="${PATH}:/usr/local/"

# cleanup
RUN rm -f /service/nextflow

# set desired nextflow version
RUN export NXF_VER=24.10.0

RUN apt-get clean
RUN apt-get -y install software-properties-common && add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update & apt-get -y install python3.9
RUN python3.9 --version

# install AWS CLI
RUN apt-get install -y unzip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update

COPY . .

RUN ls /service
RUN ls /service/workflows
RUN ls /service/taskRunner
RUN ls /service/scripts


RUN go build -o /service/main main.go

RUN mkdir -p workflows

RUN apt-get -y install python3-pip
RUN pip install -r /service/taskRunner/requirements.txt

ENTRYPOINT [ "/service/main" ]