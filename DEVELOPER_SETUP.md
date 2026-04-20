# Developer Setup Guide - VMR Scorecard Workflow

## Quick Start (5 minutes)

```powershell
# 1. Clone the repository
git clone https://github.com/juanmorice/vmr_workflow.git
cd vmr_workflow

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install pandas numpy psycopg2-binary openpyxl xlsxwriter python-pptx python-dotenv requests

# 4. Create .env file
copy .env.example .env

# 5. Edit .env with YOUR credentials (see below)
# notepad .env

# 6. Test it works
python src/vmr_standalone.py --dry-run
```

---

## System Requirements

Before starting, ensure you have:

- ✅ **Python 3.10+** — `python --version`
- ✅ **Git** — `git --version`
- ✅ **Network/VPN access** to Catalina company servers
- ✅ **Docker** (optional, only if running Airflow)

---

## Credentials You Must Update

### 1. **Yellowbrick Database** (REQUIRED)
Edit `.env` and update:
```env
YELLOWBRICK_USER=<your_network_username>
YELLOWBRICK_PASSWORD=<your_password>
```

**Where to get:**
- Username: Your Catalina network login (e.g., `jsmith`)
- Password: Contact your DB admin or Catalina IT

---

### 2. **Office365 Email** (REQUIRED for notifications)
Update `.env`:
```env
AIRFLOW__SMTP__SMTP_USER=<your_email@catalina.com>
AIRFLOW__SMTP__SMTP_PASSWORD=<your_app_password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=<your_email@catalina.com>
```

**How to generate Office365 App Password:**
1. Go to https://account.microsoft.com/account/manage-my-microsoft-account
2. Click "Security" → "Advanced security options"
3. Enable "2-Step Verification" if not already enabled
4. Create an **App Password**
5. Use that password in `.env` (NOT your regular Office365 password)

---

### 3. **File Paths** (REQUIRED)
Update `.env`:
```env
EXCEL_FILE_PATH=/path/to/your/template_form.xlsx
CUSTOM_BRAND_DESC_PATH=/analytical_services/Public/<your_username>/Catalina_PG_Categories.xlsx
```

**What these are:**
- `EXCEL_FILE_PATH`: Where your input Excel file is stored
- `CUSTOM_BRAND_DESC_PATH`: Network path to category mapping file (ask your team for correct path)

**Important:** Replace `<your_username>` with your actual network username!

---

## How to Run

### Option 1: Standalone Script (Recommended for Development)
```powershell
# Dry-run (preview without executing)
python src/vmr_standalone.py --dry-run

# Process all UNDONE rows
python src/vmr_standalone.py

# Process specific request ID only
python src/vmr_standalone.py --id 24

# With custom log directory
python src/vmr_standalone.py --log-dir ./my_logs
```

### Option 2: Airflow + Docker
```powershell
# Start Airflow services
docker-compose up -d

# View logs
docker logs airflow_project-airflow-worker-1 --tail 100

# Access Airflow UI
# Open: http://localhost:8080
# Login: airflow / airflow

# Stop services
docker-compose down
```

---

## Common Issues & Troubleshooting

### Issue: "ModuleNotFoundError: No module named 'psycopg2'"
**Solution:** Install missing dependency
```powershell
pip install psycopg2-binary
```

### Issue: "YELLOWBRICK_PASSWORD not found"
**Solution:** Ensure `.env` file exists with password set
```powershell
# Check .env exists
dir .env

# Verify YELLOWBRICK_PASSWORD is set
findstr "YELLOWBRICK_PASSWORD" .env
```

### Issue: "Connection refused" to database
**Causes:**
1. VPN not connected (if off-network)
2. Wrong username/password
3. Database server down

**Solution:**
```powershell
# Test connection
python -c "from src.gettinglmcdataframe import *; print('✓ Connection OK')"
```

### Issue: Email not sending
**Causes:**
1. Wrong SMTP credentials
2. App password not generated (for Office365)
3. Email not enabled in SMTP settings

**Solution:** Verify in `.env`:
```bash
AIRFLOW__SMTP__SMTP_USER=your_email@catalina.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password  # NOT regular password
```

---

## Project Structure

```
vmr_workflow/
├── .env                           # Your credentials (DO NOT commit)
├── .env.example                   # Template - copy to .env
├── docker-compose.yaml            # Docker configuration
├── pyproject.toml                 # Python dependencies
│
├── src/
│   ├── vmr_standalone.py          # Main standalone script
│   ├── excelfilefetcher.py        # Excel reader
│   ├── gettinglmcdataframe.py     # LMC/Yellowbrick connector
│   ├── runningvmrscorecard_excel.py
│   ├── template_form.xlsx         # Input data
│   │
│   ├── local_modules/
│   │   ├── safe_password.py       # Password handling
│   │   ├── lmc_list_upc_2.py      # LMC API
│   │   ├── yb_load.py             # Yellowbrick functions
│   │   └── yb_unload.py
│   │
│   ├── other_modules/
│   │   └── excelupdater.py        # Excel updater
│   │
│   ├── reports/
│   │   └── omni_vmr_scorecard_2024_new_up.py  # Main scorecard logic
│   │
│   └── templates/
│       └── VMR_Scorecard_Template.pptx
│
├── dags/
│   └── vmr_dag.py                 # Airflow DAG
│
├── config/
│   ├── airflow.cfg                # Airflow settings
│   ├── logging_config.py
│   └── settings.py
│
├── outputs/                       # Generated files (auto-created)
└── logs/                          # Log files (auto-created)
```

---

## Git Workflow

```powershell
# Get latest changes from team
git pull

# Make your changes
git add .
git commit -m "Your changes"
git push

# NEVER commit .env file (it's ignored by .gitignore)
```

---

## Next Steps

1. ✅ Complete `.env` setup
2. ✅ Run `python src/vmr_standalone.py --dry-run` to verify setup
3. ✅ For Airflow: Run `docker-compose up -d`
4. ✅ Read the [README.md](README.md) for pipeline details

---

## Support

- **Database questions:** Contact DB admin (Yellowbrick)
- **File path questions:** Contact your team lead
- **Script issues:** Check logs in `logs/vmr_standalone_YYYYMMDD.log`

---

## Final Checklist

- [ ] Python 3.10+ installed
- [ ] Git clone successful
- [ ] Virtual environment created and activated
- [ ] Dependencies installed
- [ ] `.env` file created with credentials
- [ ] Dry-run test passed: `python src/vmr_standalone.py --dry-run`
