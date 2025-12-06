import smtplib
from email.message import EmailMessage
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# --- TOOL 1: PDF GENERATOR ---
def generate_pdf_report(dummy_input=""):
    """Reads the last audit log and generates a PDF file."""
    try:
        # 1. Read the audit result from the temp file
        with open("temp_audit_log.txt", "r", encoding="utf-8") as f:
            content = f.read()
            
        pdf_filename = "Audit_Report.pdf"
        
        # 2. Create the PDF
        c = canvas.Canvas(pdf_filename, pagesize=letter)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(72, 750, "Skeptic Analyst - Audit Report")
        
        c.setFont("Helvetica", 12)
        text_object = c.beginText(72, 720)
        
        # Split text by lines so it doesn't run off the page
        for line in content.split("\n"):
            text_object.textLine(line)
            
        c.drawText(text_object)
        c.save()
        
        return f"SUCCESS: Report generated as '{pdf_filename}'. You can open it now."
    
    except FileNotFoundError:
        return "ERROR: No audit has been run yet. Please run an audit first."
    except Exception as e:
        return f"ERROR generating PDF: {str(e)}"

# --- TOOL 2: EMAIL SENDER ---
def send_email_report(recipient_email: str):
    """
    Sends the 'Audit_Report.pdf' to the specified email.
    NOTE: Requires valid SMTP credentials in .env to work for real.
    """
    pdf_filename = "Audit_Report.pdf"
    
    # 1. Check if PDF exists
    try:
        with open(pdf_filename, "rb") as f:
            pdf_data = f.read()
    except FileNotFoundError:
        return "ERROR: PDF report not found. Please select 'Download PDF' first to generate it."

    # 2. REAL EMAIL LOGIC (Commented out for safety/simplicity)
    # To make this work, you need EMAIL_USER and EMAIL_PASS in your .env
    # import os
    # sender = os.getenv("EMAIL_USER")
    # password = os.getenv("EMAIL_PASS")
    # if sender and password:
    #     msg = EmailMessage()
    #     msg['Subject'] = 'Skeptic Analyst Audit Report'
    #     msg['From'] = sender
    #     msg['To'] = recipient_email
    #     msg.set_content("Please find the attached audit report.")
    #     msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=pdf_filename)
    #     
    #     with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    #         smtp.login(sender, password)
    #         smtp.send_message(msg)
    #     return f"SUCCESS: Email sent to {recipient_email}"
    
    # 3. MOCK LOGIC (For immediate testing)
    print(f"\n[SYSTEM] 📧 MOCK EMAIL SENT TO: {recipient_email}")
    print(f"[SYSTEM] 📎 ATTACHMENT: {pdf_filename}")
    return f"SIMULATION: Email successfully queued for {recipient_email} (Check terminal output)."