#!/usr/bin/python
# -*- coding: UTF-8 -*-
from datetime import datetime
from psycopg2._psycopg import ProgrammingError

import ast
import psycopg2, psycopg2.extras
import configparser
import datetime
import os
import smtplib
import inspect


class DailyUpdate():

    def __init__(self):
        """ Constructor """
        
        self.cursor = None
        self.mails_to = []
        self.result = None
        self.smtp_server = None
        self.time_start = datetime.datetime.now().strftime('%d/%m/%y %H:%M:%S')

        
    def main(self):
        """ Main function """
        
        if not self.read_config_file():
            return
            
        if not self.set_db_conection():
            return
        
        self.call_function()
        self.mails_to = self.get_mails_from_db()
        if self.mails_to:
            self.create_body_mail(self.result, self.time_start)

    
    def test(self):
        """ Test mail """
        
        # Read config file
        if not self.read_config_file():
            return

        # Connect to SMTP server
        if not self.connect_smtp_server():
            return
            
        # Get list of mails
        self.mails_to = self.get_mails_from_db()
        if self.mails_to is None:
            self.mails_to = self.get_mails_from_file()
            
        # Send test mail
        self.test_mail(self.time_start)
                

                    
    def read_config_file(self):

        status = True
        try:
        
            # Read the config file
            config = configparser.ConfigParser()
            folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
            config_path = folder + os.sep + "config.conf"
            if not os.path.exists(config_path):
                print("Config file not found: " + str(config_path))
                return
                
            config.read(config_path)

            # Get mail configuration parameter
            self.domain_port = config.get("mail_config", "domain_port")
            self.domain_host = config.get("mail_config", "domain_host")
            self.sender_mail = config.get("mail_config", "sender_mail")
            self.sender_pwd = config.get("mail_config", "sender_pwd")
            self.mail_to = config.get("mail_config", "mail_to")
            
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
        
                    
    def set_db_connection(self):
    
        # Connect to database
        status = True
        try:
            self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.password, host=self.host)
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError, e:
            print('set_db_connection error %s' % e)
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
                         'Subject: PostgreSql daily update report. Result: <'+str(res)+'>\n' \
                         + str("Date report "+str(datetime_obj))
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

        # Close connection to SMTP server
        if self.smtp_server:
            self.smtp_server.quit()


    def test_mail(self, time_start):

        # Get date from datetime string
        time_end = datetime.datetime.now()
        time_end = time_end.strftime('%d/%m/%y %H:%M:%S')
        datetime_obj = datetime.datetime.strptime(time_start, '%d/%m/%y %H:%M:%S').date()
              
        result = "MAIL TEST"
            
        for x in range(0, self.mails_to.__len__()):
        
            msg_header = 'From: Daily update <' + self.sender_mail + '>\n' \
                         'To: Receiver Name <' + self.mails_to[x] + '>\n' \
                         'MIME-Version: 1.0\n' \
                         'Content-type: text/html\n' \
                         'Subject: PostgreSQL daily update report. Result: <'+str(result)+'>\n' \
                         + str("Date report "+str(datetime_obj))
            body = ' Hora inicio: ' + str(time_start) + '<br>Hora final: ' + str(time_end) + '<br>'

            msg_content = '<h5>{body}<font color="green">Proceso realizado correctamente</font></h2>\n'.format(body=body)
            msg_full = (''.join([msg_header, msg_content])).encode()
            
            print(self.mails_to[x])
            print(msg_full)
            status = self.send_mail(self.mails_to[x], msg_full)
            
        # Close connection to SMTP server
        if self.smtp_server:
            self.smtp_server.quit()


    def connect_smtp_server(self):
        """ Connect to SMTP server """
        
        print("connect_smtp_server")
        status = True
        try:
            self.smtp_server = smtplib.SMTP()
            self.smtp_server.connect(self.domain_host, int(self.domain_port))
            self.smtp_server.starttls()
            self.smtp_server.login(self.sender_mail, self.sender_pwd)
        except Exception as e:
            status = False
            print(e)

        return status

        
    def send_mail(self, mail_address, msg_content):
        """ Send mail to @mail_address """
        
        print("send mail")
        status = True
        try:
            self.smtp_server.sendmail(self.sender_mail, mail_address, msg_content)
        except Exception as e:
            status = False
            print(e)

        return status
        

    def get_mails_from_db(self):
        """ Return list of mails from configuration table """
        
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("begin")
        sql = ("SELECT value FROM " + self.schema + ".config_param_system "
            " WHERE parameter = 'daily_update_mails'")
        cursor.execute(sql)
        mails = cursor.fetchone()
        cursor.execute("commit")
        if mails is None:
            print("Any mail found. Check parameter 'daily_update_mails'")
            return None

        mails_to = []
        result = ast.literal_eval(mails)
        for mail in result['mails']:
            mails_to.append(mail['mail'])

        return mails_to
    

    def get_mails_from_file(self):
        """ Return single mail from config file """
        
        mails_to = []
        if self.mail_to:
            mails_to.append(self.mail_to)

        return mails_to



if __name__ == '__main__':
    script = DailyUpdate()
    script.test()
    

