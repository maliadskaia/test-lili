FROM python:3.7

COPY . .

# RUN pip install -r requirements.txt

# ENTRYPOINT ["python", "/main.py"]

# RUN pip install -r requirements.txt

# RUN apt update && apt install -y build-essential cmake
# RUN pip install -r requirements-ops.txt
RUN pip install -r requirements-test.txt

WORKDIR /tests/integration/overdraft_predication/
# ENTRYPOINT ["python", "/test_overdraft_predication_ops.py"]
# ENTRYPOINT ["python", "/main.py"]
ENTRYPOINT ["python", "test_01.py"]