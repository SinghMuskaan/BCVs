FROM python

WORKDIR /app

RUN /usr/local/bin/python -m pip install --upgrade pip

COPY Requirements.txt .
RUN pip install -r Requirements.txt


COPY . .

ENTRYPOINT [ "python3" ]
CMD ["app.py"]