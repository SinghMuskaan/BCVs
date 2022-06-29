import random, string
import emails
import os

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_USER_PASSWORD = os.getenv("EMAIL_USER_PASSWORD")

def generate_process_code():
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(4))
    return x

def generate_mail(transcript_link="", translated_link="", minutes_link="",user_name="user", process_code=""):

    # process_code = generate_process_code()
    SUBJECT = "[ALERT] DeepCon Processing Complete."
    TEXT = f"""Hello {user_name}, We're glad that you've chosen DeepCon. your processing of request is completed and you can download files from our website.
                Your Request Number is {process_code} \n
                You can download the results from http://localhost:3000/get-transcript"""

    return SUBJECT, TEXT

def send_email(receivers_name, process_code, receiver_email, sender, minutes_link, translated_link, transcript_link):
    
    print(receivers_name)
    print(receiver_email)
    print(sender)
    subject, text = generate_mail(
        transcript_link=transcript_link,
        translated_link= translated_link,
        minutes_link= minutes_link,
        user_name = receivers_name, 
        process_code= process_code)
    
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
    

    

