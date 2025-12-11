import smtplib
from email.message import EmailMessage
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

def generate_pdf_report(dummy_input=""):
    """Reads the last audit log and generates a PDF file."""
    try:
        # Read the audit result from temp file
        with open("temp_audit_log.txt", "r", encoding="utf-8") as f:
            content = f.read()
            
        pdf_filename = "Audit_Report.pdf"
        
        # Create PDF
        c = canvas.Canvas(pdf_filename, pagesize=letter)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(72, 750, "Skeptic Analyst - Audit Report")
        
        c.setFont("Helvetica", 12)
        text_object = c.beginText(72, 720)
        
        # Split text by lines to prevent overflow
        for line in content.split("\n"):
            text_object.textLine(line)
            
        c.drawText(text_object)
        c.save()
        
        return f"✅ SUCCESS: Report generated as '{pdf_filename}'. You can open it now."
    
    except FileNotFoundError:
        return "❌ ERROR: No audit has been run yet. Please run an audit first."
    except Exception as e:
        return f"❌ ERROR generating PDF: {str(e)}"

def send_email_report(recipient_email: str):
    """
    Sends the 'Audit_Report.pdf' to the specified email.
    NOTE: This is a MOCK implementation for testing.
    """
    pdf_filename = "Audit_Report.pdf"
    
    # Check if PDF exists
    if not os.path.exists(pdf_filename):
        return "❌ ERROR: PDF report not found. Please generate PDF first."
    
    # MOCK EMAIL (Real SMTP commented out for safety)
    print(f"\n[SYSTEM] 📧 MOCK EMAIL SENT TO: {recipient_email}")
    print(f"[SYSTEM] 📎 ATTACHMENT: {pdf_filename}")
    
    return f"✅ SIMULATION: Email queued for {recipient_email} (Check terminal output)."

# REAL EMAIL IMPLEMENTATION (Uncomment to use):
# 
# import os
# 
# def send_email_report(recipient_email: str):
#     """Sends audit report via email using SMTP."""
#     pdf_filename = "Audit_Report.pdf"
#     
#     # Read PDF
#     try:
#         with open(pdf_filename, "rb") as f:
#             pdf_data = f.read()
#     except FileNotFoundError:
#         return "❌ ERROR: PDF report not found."
#     
#     # Get credentials from environment
#     sender = os.getenv("EMAIL_USER")
#     password = os.getenv("EMAIL_PASS")
#     
#     if not sender or not password:
#         return "❌ ERROR: EMAIL_USER or EMAIL_PASS not set in .env"
#     
#     # Send email
#     try:
#         msg = EmailMessage()
#         msg['Subject'] = 'Skeptic Analyst Audit Report'
#         msg['From'] = sender
#         msg['To'] = recipient_email
#         msg.set_content("Please find the attached audit report from Skeptic Analyst.")
#         msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=pdf_filename)
#         
#         with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
#             smtp.login(sender, password)
#             smtp.send_message(msg)
#         
#         return f"✅ SUCCESS: Email sent to {recipient_email}"
#     except Exception as e:
#         return f"❌ ERROR sending email: {e}"