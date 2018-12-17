FROM fedora:28

WORKDIR /opt/heater

RUN dnf -y update && \
    dnf -y install python3-pip python3-scikit-learn && \
    dnf clean all

COPY . ./

RUN python3 -m pip install --no-cache-dir -r requirements.txt && \
    python3 -m unittest discover

ENTRYPOINT ["python3", "-u", "./heater.py"]
