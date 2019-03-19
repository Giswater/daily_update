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
        time_start = datetime.datetime.now()
        time_start = time_start.strftime('%d/%m/%y %H:%M:%S')
        self.cursor = self.set_db_conection()

        result = self.call_function()
        self.mails_to = self.getMailsTo()

        if self.mails_to:
            self.create_body_mail(result, time_start)


    def set_db_conection(self):
        # Read the connection data to our server and return cursor
        # Read the config file

        config = configparser.ConfigParser()
        ruta = os.getcwd()
        ruta = ruta + "/config.conf"
        config.read(ruta)
        # Set the variables
        self.dbHostName = config.get("postgresConfig", "hostname")
        self.dbName = config.get("postgresConfig", "db")
        self.schema_name = config.get("postgresConfig", "schema_name")
        self.dbUsername = config.get("postgresConfig", "username")
        self.dbPassword = config.get("postgresConfig", "password")

        self.domainport = config.get("Remitente", "puertodominio")
        self.domain = config.get("Remitente", "dominio")
        self.remitente = config.get("Remitente", "sendFrom")
        self.passremitente = config.get("Remitente", "pass_sender")
        conn = psycopg2.connect(database=self.dbName, user=self.dbUsername, password=self.dbPassword,
                                host=self.dbHostName)
        cursor = conn.cursor()
        return cursor


    def call_function(self):
        try:
            query = "SELECT " + self.schema_name + ".gw_fct_utils_daily_update()"
            self.cursor.execute("begin")
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            self.cursor.execute("commit")
            return result
        except ProgrammingError as e:
            return ["An exception has occurred: {e} \n".format(e=e)]
        except Exception as e:
            print(type(e).__name__)
            return ["An exception has occurred: {e}".format(e=e)]


    def create_body_mail(self, result, time_start):
        time_end = datetime.datetime.now()
        time_end = time_end.strftime('%d/%m/%y %H:%M:%S')
        # Get date from datetime string
        datetime_obj = datetime.datetime.strptime(time_start, '%d/%m/%y %H:%M:%S').date()
        if result[0] == 0:
            res = "Proceso realizado correctamente"
        else:
            res = "El proceso no se ha realizado correctamente, consulta log."
        for x in range(0, self.mails_to.__len__()):
            msg_header = 'From: Daily update <' + self.remitente + '>\n' \
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
        server = smtplib.SMTP(self.domain, self.domainport)
        server.starttls()
        server.login(self.remitente, self.passremitente)

        server.sendmail(self.remitente, mail_address, msg_content)
        server.quit()


    def getMailsTo(self):
        """ Return the list of mails in the table llicamunt.config_param_system """
        # Get the datetime from Gui
        conn = psycopg2.connect(database=self.dbName, user=self.dbUsername, password=self.dbPassword,
                                host=self.dbHostName)

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("begin")
        cur.execute("SELECT value FROM " + self.schema_name + ".config_param_system "
                    " WHERE parameter = 'daily_update_mails'")
        # Format of value:
        # {'mails': [{'mail':'info@bgeo.es'},{'mail':' nestor@bgeo.es'}]}

        mails = cur.fetchone()
        if mails is None:
            print("No mails config_param_system where parameter = daily_update_mails")
            return False

        # Convert str to dict
        result = ast.literal_eval(mails[0])
        mails_to = []
        for mail in result['mails']:
            mails_to.append(mail['mail'])
        cur.execute("commit")

        return mails_to








if __name__ == '__main__':
    DailyUpdate()
