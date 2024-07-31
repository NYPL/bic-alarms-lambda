FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip

COPY ./chrome_installer.sh ./chrome_installer.sh
RUN ./chrome_installer.sh
RUN rm ./chrome_installer.sh

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --upgrade pip && \
	pip install -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY alarm_controller.py ${LAMBDA_TASK_ROOT}
COPY alarms ${LAMBDA_TASK_ROOT}/alarms
COPY helpers ${LAMBDA_TASK_ROOT}/helpers

CMD [ "lambda_function.lambda_handler" ]
