#需自行安裝部分python modules:
# pip install beautifulsoup4
# pip install schedule
# pip install requests
# pip install win11toast

#只檢測_分鐘後賽事 預設46分鐘(包括半場), 整數
TOCHECKMINUTES = 65

#每隔幾多分鐘check 1次, 分鐘為單位, 整數
CHECKINTERVAL_MINUTES = 30
CHECKINTERVAL_SECOND = 30

#大波賠率要求細過幾多, 可以有小數.
#不想提示可直接設為1
DESIREDHIGHODD = 1.8

SERVER_NAME = 'ANGUS'
DATABASE_NAME = 'HKJC_Odds'
USER_NAME = 'sa'
PASSWORD = '2036'