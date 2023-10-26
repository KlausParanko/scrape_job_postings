FROM public.ecr.aws/lambda/python:3.11

COPY venv/ ${LAMBDA_TASK_ROOT}

RUN pwd
RUN ls ${LAMBDA_TASK_ROOT}/venv

RUN . ${LAMBDA_TASK_ROOT}/venv/bin/activate

COPY get_postings_list.py ${LAMBDA_TASK_ROOT}
COPY chromedriver ${LAMBDA_TASK_ROOT}
COPY chrome/ ${LAMBDA_TASK_ROOT}

CMD ["get_postings_list.handler"]

