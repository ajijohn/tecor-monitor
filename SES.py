__author__ = 'Aji John'
# ==============================================================================
# Adapted from: stack overflow
# Revised: 2016-09-10
__version__ = '0.0.1'
# ==============================================================================

import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_ses(awsregion,fromaddr,
             subject,
             body,
             recipient,
             attachment=None,
             filename=''):
    """Send an email via the Amazon SES service.

    Example:
      send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)

    Return:
      If 'ErrorResponse' appears in the return message from SES,
      return the message, otherwise return an empty '' string.
    """
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient
    msg.attach(MIMEText(body))
    if attachment:
        part = MIMEApplication(attachment)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    conn = boto3.ses.connect_to_region(awsregion)
    result = conn.send_raw_email(msg.as_string())
    return result if 'ErrorResponse' in result else ''