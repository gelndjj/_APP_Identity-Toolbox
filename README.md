# 🛠️ Identity Toolbox

A modern **PyQt6 + PowerShell desktop app** designed to simplify **Microsoft Entra ID (Azure AD)** user lifecycle management.  
Built for IT administrators to streamline onboarding, offboarding, and user attribute management with a clean GUI.

---

## ✨ Features

- **🔑 Create Users**  
  - Full user creation via Microsoft Graph API  
  - Extended attributes supported: Job Title, Department, Office, Hire Date, Usage Location, Age Group, etc.  
  - Automatic MailNickname generation  

- **📑 Bulk CSV Integration**  
  - Import/export user data through CSV  
  - Supports all extended attributes  
  - Custom CSV templates  

- **🧩 User Templates**  
  - Save and reapply frequently used field sets  
  - Update existing templates  

- **🎲 Random User Generator**  
  - Create test users with fake data (via Faker)  
  - Useful for labs and demos  

- **📋 Extended Attribute Support**  
  - Job Title, Department, Company  
  - Business / Mobile / Fax phone numbers  
  - Proxy Addresses, IM Addresses, Other Emails  
  - Employee ID, Hire Date, Org Data  
  - Parental Controls: Age Group, Consent Provided for Minor, Legal Age Classification  
  - AccountEnabled, Usage Location, Preferred Data Location  

- **⚡ Responsive UI**  
  - PowerShell commands executed via background threads (QThread)  
  - Live log output

- **📦 Deployment Ready**  
  - Can be packaged and deployed via **Microsoft Intune Company Portal**  
  - Secure integration with **App Registration & delegated permissions**  

---

## 🖥️ Tech Stack

- **Frontend:** Python 3.13, PyQt6  
- **Backend:** PowerShell 7.5.2, Microsoft Graph SDK  
- **APIs:** Microsoft Graph (`User.ReadWrite.All`, `Directory.ReadWrite.All`, `EntitlementManagement.ReadWrite.All`)  
- **Other:** CSV handling, QThread-based workers, Faker (for random users)  

---

## 🚀 Setup

### 1. Prerequisites
- Python 3.13+  
- PowerShell 7.5.2+  
- Microsoft Graph PowerShell SDK (`Install-Module Microsoft.Graph -Scope AllUsers`)  
- Microsoft 365 tenant with admin rights  

### 2. Clone repo
```bash
git clone https://github.com/gelndjj/_APP_Identity-Toolbox.git
cd identity-toolbox
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the app
```bash
python id-toolbox.py
```

### 🔑 Microsoft Graph Setup
	1.	Create an App Registration in Entra ID
	2.	Assign delegated permissions:
	•	User.ReadWrite.All
	•	Directory.ReadWrite.All
	•	EntitlementManagement.ReadWrite.All
	3.	Deploy app via Intune Company Portal if required

### 🧪 Example CSV

| Display Name | First name | Last name | User Principal Name     | Password  | Job title  | Company name | Department | Usage location |
|--------------|------------|-----------|-------------------------|-----------|------------|--------------|------------|----------------|
| John Doe     | John       | Doe       | john.doe@contoso.com   | Pass@123  | Engineer   | Contoso      | IT         | FR             |

### 🗺️ Roadmap
	•	Add group assignments
	•	Add Access Package integration
	•	Add manager/sponsor assignment during creation
	•	Multi-language support

### ⚖️ License
MIT License.
This project is provided as-is for educational and IT administration use.

### 🤝 Contributing
Pull requests are welcome!
For major changes, please open an issue first to discuss what you’d like to change.

### 📸 Screenshots