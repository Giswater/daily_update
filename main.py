#!/usr/bin/python
# -*- coding: UTF-8 -*-
import ast
from datetime import datetime

import psycopg2, psycopg2.extras
import configparser
import datetime

import os
import smtplib

from psycopg2._psycopg import ProgrammingError


class DailyUpdate():

    def __init__(self):

        self.cursor = None
        self.mails_to = []
        self.result = None
        time_start = datetime.datetime.now()
        time_start = time_start.strftime('%d/%m/%y %H:%M:%S')

        status = read_config_file()
        if status:
            if self.set_db_conection():
                self.call_function()
                self.mails_to = self.get_mails_to()
                if self.mails_to:
                    self.create_body_mail(self.result, time_start)


    def read_config_file(self):

        status = True
        try:
            # Read the config file
            config = configparser.ConfigParser()
            ruta = os.getcwd()
            ruta = ruta + "/config.conf"
            config.read(ruta)

            # Get mail configuration parameter
            self.domain_port = config.get("mail_config", "domain_port")
            self.domain_host = config.get("mail_config", "domain_host")
            self.sender_mail = config.get("mail_config", "sender_mail")
            self.sender_pwd = config.get("mail_config", "sender_pwd")
            
            # Get database configuration parameter        
            self.host = config.get("db_config", "host")
            self.db = config.get("db_config", "db")
            self.schema = config.get("db_config", "schema")
            self.user = config.get("db_config", "user")
            self.password = config.get("db_config", "password")    
        except Exception, e:
            print('read_config_file error %s' % e)
            status = False
            
        return status
        
                    
    def set_db_conection(self):
    
        # Connect to database
        status = True
        try:
            self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.password, host=self.host)
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError, e:
            print('set_db_conection error %s' % e)
            status = False
            
        return status


    def call_function(self):
    
        try:
            sql = "SELECT " + self.schema + ".gw_fct_utils_daily_update()"
            self.cursor.execute("begin")
            self.cursor.execute(sql)
            self.result = self.cursor.fetchone()
            self.cursor.execute("commit")
            return result
        except ProgrammingError as e:
            return ["An exception has occurred: {e} \n".format(e=e)]
        except Exception as e:
            print(type(e).__name__)
            return ["An exception has occurred: {e}".format(e=e)]


    def create_body_mail(self, result, time_start):

        # Get date from datetime string    
        time_end = datetime.datetime.now()
        time_end = time_end.strftime('%d/%m/%y %H:%M:%S')
        datetime_obj = datetime.datetime.strptime(time_start, '%d/%m/%y %H:%M:%S').date()
        
        if result[0] == 0:
            res = "Proceso realizado correctamente"
        else:
            res = "El proceso no se ha realizado correctamente, consulta log."
            
        for x in range(0, self.mails_to.__len__()):
            msg_header = 'From: Daily update <' + self.sender_mail + '>\n' \
                         'To: Receiver Name <' + self.mails_to[x] + '>\n' \
                         'MIME-Version: 1.0\n' \
                         'Content-type: text/html\n' \
                         'Subject: PostgreSql Daily update rapport. Result: <'+str(res)+'>\n' \
                         + str("Rapport for date "+str(datetime_obj))
            body = ' Hora inicio: ' + str(time_start) + '<br>Hora final: ' + str(time_end) + '<br>'

            if result[0] == 0:
                msg_content = '<h5>{body}<font color="green">Proceso realizado correctamente</font></h2>\n'.format(body=body)
            elif "An exception has occurred" in result[0]:
                msg_content = '<h5>{body}<font color="red">El proceso no se ha realizado correctamente, consulta log de postgre para mas informacion</font></h2>\n' \
                              '{result}'.format(body=body, result=result[0])
            else:
                msg_content = '<h5>{body}<font color="red">El proceso no se ha realizado correctamente, consulta log de postgre para mas informacion</font></h2>\n'.format(body=body)

            msg_full = (''.join([msg_header, msg_content])).encode()
            self.send_mail(self.mails_to[x], msg_full)


    def send_mail(self, mail_address, msg_content):
        """ Send mail to """
        
        server = smtplib.SMTP(self.domain_host, self.domain_port)
        server.starttls()
        server.login(self.sender_mail, self.sender_pwd)
        server.sendmail(self.sender_mail, mail_address, msg_content)
        server.quit()


    def get_mails_to(self):
        """ Return the list of mails from configuration table """
        
        # Get the datetime from Gui
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("begin")
        sql = ("SELECT value FROM " + self.schema + ".config_param_system "
            " WHERE parameter = 'daily_update_mails'")
        cursor.execute(sql)
        mails = cursor.fetchone()
        cursor.execute("commit")
        if mails is None:
            print("Any mail found. Check parameter 'daily_update_mails'")
            return False
            
        # Example:
        # {'mails': [{'mail':'info@bgeo.es'}, {'mail':'nestor@bgeo.es'}]}
        
        # Convert str to dict
        result = ast.literal_eval(mails[0])
        mails_to = []
        for mail in result['mails']:
            mails_to.append(mail['mail'])

        return mails_to



if __name__ == '__main__':
    DailyUpdate()
    
