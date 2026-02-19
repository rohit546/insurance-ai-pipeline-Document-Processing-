"""
Email service for sending feedback emails
"""
import os
import smtplib
import json
import urllib.request
import urllib.error
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
from dotenv import load_dotenv

load_dotenv()

# Email configuration from environment variables
# IMPORTANT: Never hardcode credentials! Always use environment variables in Railway.
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME', 'mudassir.nedian@gmail.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'gwykmimmztnzawtq')
FROM_EMAIL = os.getenv('FROM_EMAIL', SMTP_USERNAME)

# Resend configuration (recommended for Railway)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
# Default sender that works for Resend onboarding/testing; for production, set this to a verified domain sender.
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")

# Recipient emails
# Note: Resend free tier only allows sending to verified email addresses
# For now, sending to mudassir only. After domain verification, add rohit back.
FEEDBACK_RECIPIENTS = [
    'mudassir@mckinneyandco.com',
    # 'rohit@mckinneyandco.com'  # Uncomment after verifying domain in Resend
]

def _send_via_resend(subject: str, message: str, sender_username: str = "Anonymous") -> dict:
    """
    Send feedback using Resend HTTP API (works on Railway; uses HTTPS, not SMTP).
    """
    if not RESEND_API_KEY:
        return {"success": False, "message": "RESEND_API_KEY is not set"}

    email_subject = f"Feedback: {subject}"
    email_text = (
        f"Feedback submitted from: {sender_username}\n\n"
        f"Subject: {subject}\n\n"
        f"Message:\n{message}\n\n"
        f"---\nThis is an automated email from the McKinney & Co. Insurance Dashboard.\n"
    )

    payload = {
        "from": RESEND_FROM_EMAIL,
        "to": FEEDBACK_RECIPIENTS,
        "subject": email_subject,
        "text": email_text,
    }

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "McKinney-Insurance-Dashboard/1.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if 200 <= resp.status < 300:
                print(f"✅ Feedback email sent via Resend (status={resp.status})")
                return {"success": True, "message": "Feedback email sent via Resend", "provider": "resend"}
            return {"success": False, "message": f"Resend returned status={resp.status}: {body}"}
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        except:
            error_body = str(e)
        
        error_code = getattr(e, 'code', 'unknown')
        error_msg = f"Resend HTTPError status={error_code}"
        
        # Parse Cloudflare errors (error code 1010 = Access Denied)
        if error_code == 403 and "1010" in error_body:
            error_msg += ": Cloudflare blocked request (error 1010). This may indicate an issue with the API key or request format."
        elif error_body:
            error_msg += f": {error_body}"
        
        print(f"❌ Resend API error: {error_msg}")
        return {"success": False, "message": error_msg}
    except Exception as e:
        error_msg = f"Resend error: {str(e)}"
        print(f"❌ {error_msg}")
        return {"success": False, "message": error_msg}


def send_feedback_email(subject: str, message: str, sender_username: str = "Anonymous") -> dict:
    """
    Send feedback email to recipients
    
    Args:
        subject: Email subject
        message: Email message/body
        sender_username: Username of person submitting feedback
    
    Returns:
        dict: {"success": bool, "message": str}
    """
    try:
        # Preferred path on Railway: Resend (HTTPS)
        if RESEND_API_KEY:
            result = _send_via_resend(subject=subject, message=message, sender_username=sender_username)
            if result.get("success"):
                return result
            # If Resend is configured but failing, surface the error (don't silently fall back)
            print(f"❌ Resend failed: {result.get('message')}")
            return result

        # Validate email configuration
        if not SMTP_USERNAME or not SMTP_PASSWORD:
            print("⚠️  SMTP credentials not configured. Email will not be sent.")
            return {
                "success": False,
                "message": "Email service not configured. Please set SMTP_USERNAME and SMTP_PASSWORD environment variables."
            }
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = ', '.join(FEEDBACK_RECIPIENTS)
        msg['Subject'] = f"Feedback: {subject}"
        
        # Email body
        body = f"""
Feedback submitted from: {sender_username}

Subject: {subject}

Message:
{message}

---
This is an automated email from the McKinney & Co. Insurance Dashboard.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Try to send email - Railway may block SMTP ports, so we'll try both methods
        text = msg.as_string()
        email_sent = False
        last_error = None
        
        # Method 1: Try port 587 with STARTTLS (default)
        try:
            print(f"📧 Attempting to send email via {SMTP_SERVER}:{SMTP_PORT}...")
            server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
            server.set_debuglevel(0)
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(FROM_EMAIL, FEEDBACK_RECIPIENTS, text)
            server.quit()
            email_sent = True
            print(f"✅ Email sent successfully via port {SMTP_PORT}")
        except Exception as e1:
            last_error = e1
            print(f"⚠️  Port {SMTP_PORT} failed: {e1}")
            
            # Method 2: Try port 465 with SSL (if 587 failed)
            if SMTP_PORT == 587:
                try:
                    print(f"📧 Trying alternative method: {SMTP_SERVER}:465 (SSL)...")
                    import ssl
                    context = ssl.create_default_context()
                    server = smtplib.SMTP_SSL(SMTP_SERVER, 465, timeout=30, context=context)
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                    server.sendmail(FROM_EMAIL, FEEDBACK_RECIPIENTS, text)
                    server.quit()
                    email_sent = True
                    print(f"✅ Email sent successfully via port 465 (SSL)")
                except Exception as e2:
                    last_error = e2
                    print(f"⚠️  Port 465 also failed: {e2}")
        
        if not email_sent:
            raise last_error or Exception("Failed to send email via both methods")
        
        print(f"✅ Feedback email sent successfully to {len(FEEDBACK_RECIPIENTS)} recipients")
        return {
            "success": True,
            "message": f"Feedback sent successfully to {len(FEEDBACK_RECIPIENTS)} recipients"
        }
        
    except Exception as e:
        print(f"❌ Error sending feedback email: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Failed to send email: {str(e)}"
        }
