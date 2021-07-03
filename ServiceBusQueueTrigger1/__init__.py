import logging
import azure.functions as func
import psycopg2
import os
import settings
import json
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    # TODO: Get connection to database
    conn = psycopg2.connect("dbname='techconfdb' user='serverAdmin@hsproj3dbserver' host='hsproj3dbserver.postgres.database.azure.com' password='P@ssword' port='5432' sslmode='true'")
    cursor = conn.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        # Fetch all rows from table
        cursor.execute("SELECT * FROM notification where id={};".format(notification_id))
        rows = cursor.fetchall()

        # Print all rows
        for row in rows:
            print("Data row = (%s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2])))
            # TODO: Get attendees email and name
            # TODO: Loop through each attendee and send an email with a personalized subject
            attendees = cursor.execute("SELECT * FROM attendee")
            for attendee in attendees:
                subject = '{}: {}'.format(attendee.first_name, row.subject)
                message = Mail(
                    from_email=settings.adminEmail,
                    to_emails=attendee.email,
                    subject=subject,
                    plain_text_content=row.message)
                #sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
                #sg.send(message)
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        date = datetime.utcnow()
        status = 'Notified {} attendees'.format(len(attendees.details))
        cursor.execute("UPDATE notification SET completed_date=%s, status=%s WHERE id=%s;", (date, status, notification_id))
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        conn.close
