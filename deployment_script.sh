#!/bin/zsh

rm -f -r ./package
rm -f deployment-package.zip
pip3.11 install --target ./package -r requirements.txt
pip3.11 install \
    --platform manylinux2014_x86_64 \
    --target=./package \
    --implementation cp \
    --only-binary=:all: --upgrade \
    'psycopg[binary]'
cd package
zip -r ../deployment-package.zip .
cd ..
zip deployment-package.zip lambda_function.py
zip deployment-package.zip alarm_controller.py
zip deployment-package.zip query_helper.py