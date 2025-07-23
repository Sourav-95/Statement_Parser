# 💼 Personal Finance Automation System

This Python-based modular system automates the ingestion, transformation, and analysis of financial statements. It supports reading bank PDFs, transforming them into structured data, storing in a local SQLite database, and backing up to Google Drive. A GUI and scheduling capability are also available.

---

## ⚠️ Important Prerequisites Before Running

Please **ensure the following files and configurations are in place** before executing the program to avoid failures:

1. ✅ **Input Arguments File**  
   Check and configure all run-time arguments in the YAML file:  
   `./inputs/saved_args.yaml`

2. ✅ **Banking Keyword Categorization**  
   Provide custom keyword-to-category mappings as per your preferences:  
   `./inputs/banking_keywords.txt`

3. ✅ **PDF Passwords (If Required)**  
   If your source PDF files are password-protected, list their filenames and passwords in:  
   `./inputs/passwords.yaml`

4. ✅ **Google Drive Credentials**  
   Download the `Credentials.json` from your Google Cloud Project and place it at:  
   `./auth_connection/Credentials.json`

5. ✅ **Environment File**  
   Ensure your `.env` file (in the project root) points to correct `auth_connection` paths and Drive API settings.

---

## ⚙️ Argument Parser: Inputs During Trigger

The script accepts the following arguments:

| Argument     | Description                     |
|--------------|---------------------------------|
| `--src_Drive`| Google Drive source folder ID   |
| `--backup`   | Google Drive backup folder ID   |

**Example usage**:

```bash
python -m src.main --src_Drive "SOURCE_FOLDER_ID" --backup "BACKUP_FOLDER_ID"

## 🧠 Architecture Overview

+--------------------------------+
|        Argument Parser         |
|  --src_Drive, --backup         |
+---------------+----------------+
                |
                v
+---------------+-----------------+
|     Input Layer (Ingestion)     |
|  - Upload Statement Manually    |
|  - Read from Google Drive Folder|
+---------------+-----------------+
                |
                v
+---------------+-----------------+
|     Data Extraction Layer       |
|  - Convert PDF to DataFrame     |
+---------------+-----------------+
                |
                v
+---------------+-----------------+            +------------------------------+
|   Data Transformation Layer     |            |   Google Drive Backup        |
|  - Feature Engineering          |______\     | - Store transformed DB       |
|  - Data Modeling                |      /     +------------------------------+
+---------------+-----------------+            
                |
                v
+---------------+------------------+
|      Database Layer (SQLite)     |
|  - Cloud-synced local DB         |            +------------------------------+
|  - Google Drive / OneDrive Sync  |________\   |   Post-Processing Cleanup    |
+---------------+------------------+        /   | - Delete source/temp files  |
                |                               +------------------------------+
     +----------+-----------+
     |                      |
     v                      v
+----------------+   +-------------------------+
| Dashboard Layer|   |     Backup Manager      |
| - Expense Trend|   | - After every update    |
| - Income Trend |   | - Upload DB to GDrive   |
| - MoM Graphs   |   +-------------------------+
+----------------+

# 📦 Installation & Setup
### 1: Create a virtual environment
python -m venv venv
venv\Scripts\activate  # (Windows)

### 2: Install dependencies
pip install -r requirements.txt
