import random, string
import emails
import os

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_USER_PASSWORD = os.getenv("EMAIL_USER_PASSWORD")

def generate_process_code():
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    return x

def generate_mail(user_name="user", process_code=""):

    # process_code = generate_process_code()
    SUBJECT = "[ALERT] DeepCon Processing Complete."
    TEXT = f"Hello {user_name}, We're glad that you've chosen DeepCon. the processing of request number {process_code} is completed and you can download files from our website"

    return SUBJECT, TEXT

def send_email(receivers_name, process_code, receiver_email, sender):
    
    print(receivers_name)
    print(receiver_email)
    print(sender)
    subject, text = generate_mail(receivers_name, process_code)
    
    message = emails.html(
            text=text,
            subject=subject,
            mail_from=sender,
        )
    
    r = message.send(
            to=receiver_email,
            smtp={
                "host": "email-smtp.ap-south-1.amazonaws.com",
                "port": 587,
                "timeout": 5,
                "user": EMAIL_USER,
                "password": EMAIL_USER_PASSWORD,
                "tls": True,
            }
        )
    
    return r
    

    

