FROM public.ecr.aws/lambda/python:3.11

WORKDIR ${LAMBDA_TASK_ROOT}
COPY venv ./venv

RUN source ./venv/bin/activate

COPY get_postings_list.py ${LAMBDA_TASK_ROOT}
COPY chromedriver ${LAMBDA_TASK_ROOT}
COPY chrome/ ${LAMBDA_TASK_ROOT}

CMD ["get_postings_list.handler"]

