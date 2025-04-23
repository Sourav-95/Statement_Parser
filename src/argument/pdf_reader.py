import pikepdf

try:
    # Replace with the actual path and password
    pdf = pikepdf.open("/Users/souravm/Documents/Projects/statement_parser/input/statement_20250404.pdf", password="SOUR0503")
    
    # Save decrypted version
    pdf.save("/Users/souravm/Documents/Projects/statement_parser/unlocked.pdf")
    pdf.close()
    print("✅ PDF successfully unlocked and saved.")
except pikepdf.PasswordError:
    print("❌ Incorrect password or unable to decrypt the file.")
