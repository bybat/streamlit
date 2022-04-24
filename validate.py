#!/usr/bin/env python
import snowflake.connector
import json

# Get the credentials
config_location = 'C:/Users/berta/streamlitapp'

config = json.loads(open(str(config_location+'/creds.json')).read())

username = config['user']
account = config['account']
print("Hallo dit is een test")
# Gets the version
ctx = snowflake.connector.connect(
    user        = username,
    #password    = password,
    authenticator='externalbrowser',
    account     = account
    #insecure_mode=True
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version();")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()