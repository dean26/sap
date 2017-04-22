from pyrfc import Connection
from slackclient import SlackClient
import datetime

user = 'TO31'
password = ''
saprouter = '/H/saprouter.hcc.in.tum.de/S/3297/H/'

conn = Connection(user=user, passwd=password,
                  mshost='i78z',
                  msserv='3678',
                  sysid='1',
                  group="SPACE",
                  saprouter=saprouter, 
                  client='902')

'''
b_result = conn.call('BAPI_USER_GET_DETAIL',
                     USERNAME = 'TO31',
                     CACHE_RESULTS  = ' ')

print (b_result['ADDRESS']['LASTNAME'].encode('utf8'))
'''

def qry(self, Fields, SQLTable, Where = '', MaxRows=50, FromRow=0):
        """A function to query SAP with RFC_READ_TABLE"""

        # By default, if you send a blank value for fields, you get all of them
        # Therefore, we add a select all option, to better mimic SQL.
        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME':x} for x in Fields] # Notice the format

        # the WHERE part of the query is called "options"
        options = [{'TEXT': Where}] # again, notice the format
        
        # we set a maximum number of rows to return, because it's easy to do and
        # greatly speeds up testing queries.
        rowcount = MaxRows

        # Here is the call to SAP's RFC_READ_TABLE
        tables = self.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS = Fields, 
                                OPTIONS=options, ROWCOUNT = MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []
        fields_name = []

        data_fields = tables["DATA"] # pull the data part of the result set
        data_names = tables["FIELDS"] # pull the field name part of the result set

        #headers = [x['FIELDNAME'] for x in data_names] # headers extraction
        long_fields = len(data_fields) # data extraction
        long_names = len(data_names) # full headers extraction if you want it

        # now parse the data fields into a list
        for line in range(0, long_fields):
            fields.append(data_fields[line]["WA"].strip())

        # for each line, split the list by the '|' separator
        #fields = [x.strip().split('|') for x in fields ]

        # return the 2D list and the headers
        return fields

slack_client = SlackClient('')

channel_id = 'C4YJJBYU8'

cron_interval = 30
teraz = datetime.datetime.now()
nowa = teraz + datetime.timedelta(minutes = -cron_interval)

erdat = nowa.strftime('%Y%m%d')
erzet = nowa.strftime('%H%M%S')

where = "ERDAT >= '" + erdat + "' AND ERZET >= '" + erzet + "'"
fields = ['VBELN']
table = 'VBAK'

results = qry(conn, fields, table, "ERDAT >= '20170412' AND ERZET >= '163000'")

#results = qry(conn, fields, table, where)

#print (results)

for i in results:
      message = 'Otrzymano nowy dokument sprzedaży #'+i
      slack_client.api_call(
         "chat.postMessage",
         channel = channel_id,
         text = message,
         username = 'SAP',
         icon_emoji = ':robot_face:'
      )
