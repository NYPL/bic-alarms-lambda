#!/bin/zsh

rm -f -r ./package
rm -f deployment-package.zip
python -m pip install --upgrade pip
pip install --target ./package -r requirements.txt
pip install \
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
zip deployment-package.zip alarms/alarm.py
zip deployment-package.zip alarms/models/circ_trans_alarms.py
zip deployment-package.zip alarms/models/holds_alarms.py
zip deployment-package.zip alarms/models/location_visits_alarms.py
zip deployment-package.zip alarms/models/overdrive_checkouts_alarms.py
zip deployment-package.zip alarms/models/patron_info_alarms.py
zip deployment-package.zip alarms/models/pc_reserve_alarms.py
zip deployment-package.zip alarms/models/sierra_codes/sierra_itype_codes_alarms.py
zip deployment-package.zip alarms/models/sierra_codes/sierra_location_codes_alarms.py
zip deployment-package.zip alarms/models/sierra_codes/sierra_stat_group_codes_alarms.py
zip deployment-package.zip helpers/alarm_helper.py
zip deployment-package.zip helpers/overdrive_web_scraper.py
zip deployment-package.zip helpers/query_helper.py